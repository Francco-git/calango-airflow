"""
dags/calango_marketplace_pipeline.py
======================================
Pipeline de ingestão de dados do Marketplace — Calango Investimentos
---------------------------------------------------------------------

Captura em paralelo:
  • Usuários  (GET /users)
  • Produtos  (GET /products)
  • Carrinhos (GET /carts)

Cada entidade passa pela esteira medalhão:
  Bronze (raw) → Silver (limpo/normalizado)

Topologia:
  inicio
    ├── capturar_users   ──► bronze_users   ──► silver_users
    ├── capturar_products──► bronze_products──► silver_products
    └── capturar_carts   ──► bronze_carts   ──► silver_carts
                                                      │
                                               finalizar (fan-in)
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

FAKESTORE_BASE = "https://fakestoreapi.com"
POSTGRES_CONN_ID = "postgres_calango"


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
@dag(
    dag_id="calango_marketplace_pipeline",
    description="Ingestão paralela de Users, Products e Carts — Calango Investimentos",
    schedule="0 6 * * *",
    start_date=pendulum.now("America/Sao_Paulo").subtract(days=1),
    catchup=False,
    default_args={
        "owner": "time-dados-calango",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["calango", "marketplace", "medaliao"],
)
def calango_marketplace_pipeline():

    # ── Helpers ────────────────────────────────────────────────
    def _get(endpoint: str) -> list[dict]:
        url = f"{FAKESTORE_BASE}{endpoint}"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            log.info("✅ %s → %d registros", endpoint, len(data))
            return data
        except requests.RequestException as exc:
            log.error("Falha em %s: %s", endpoint, exc)
            raise

    # ══════════════════════════════════════════
    # CAPTURA PARALELA (fan-out linear por entidade)
    # ══════════════════════════════════════════

    @task(task_id="capturar_users")
    def capturar_users() -> list[dict]:
        """Captura todos os usuários da API."""
        return _get("/users")

    @task(task_id="capturar_products")
    def capturar_products() -> list[dict]:
        """Captura todos os produtos da API."""
        return _get("/products")

    @task(task_id="capturar_carts")
    def capturar_carts() -> list[dict]:
        """Captura todos os carrinhos (vendas) da API."""
        return _get("/carts")

    # ══════════════════════════════════════════
    # BRONZE — persistência dos dados brutos
    # ══════════════════════════════════════════

    @task(task_id="bronze_users")
    def bronze_users(users: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            INSERT INTO bronze_users
                (id, email, username, firstname, lastname, phone, city, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                email      = EXCLUDED.email,
                raw_json   = EXCLUDED.raw_json,
                ingested_at = NOW();
        """
        import json
        conn = hook.get_conn(); cur = conn.cursor()
        try:
            for u in users:
                addr = u.get("address", {})
                cur.execute(sql, (
                    u["id"], u.get("email"), u.get("username"),
                    u.get("name", {}).get("firstname"),
                    u.get("name", {}).get("lastname"),
                    u.get("phone"),
                    addr.get("city"),
                    json.dumps(u),
                ))
            conn.commit()
            log.info("Bronze users: %d registros.", len(users))
            return len(users)
        except Exception as e:
            conn.rollback(); raise
        finally:
            cur.close(); conn.close()

    @task(task_id="bronze_products")
    def bronze_products(products: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            INSERT INTO bronze_products
                (id, title, price, category, description, image, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                price      = EXCLUDED.price,
                raw_json   = EXCLUDED.raw_json,
                ingested_at = NOW();
        """
        import json
        conn = hook.get_conn(); cur = conn.cursor()
        try:
            for p in products:
                cur.execute(sql, (
                    p["id"], p.get("title"), p.get("price"),
                    p.get("category"), p.get("description"),
                    p.get("image"), json.dumps(p),
                ))
            conn.commit()
            log.info("Bronze products: %d registros.", len(products))
            return len(products)
        except Exception as e:
            conn.rollback(); raise
        finally:
            cur.close(); conn.close()

    @task(task_id="bronze_carts")
    def bronze_carts(carts: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            INSERT INTO bronze_carts (id, user_id, date, raw_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                raw_json   = EXCLUDED.raw_json,
                ingested_at = NOW();
        """
        import json
        from datetime import datetime
        conn = hook.get_conn(); cur = conn.cursor()
        try:
            for c in carts:
                date_val = None
                try:
                    date_val = datetime.fromisoformat(c.get("date", "")).date()
                except Exception:
                    pass
                cur.execute(sql, (
                    c["id"], c.get("userId"), date_val, json.dumps(c),
                ))
            conn.commit()
            log.info("Bronze carts: %d registros.", len(carts))
            return len(carts)
        except Exception as e:
            conn.rollback(); raise
        finally:
            cur.close(); conn.close()

    # ══════════════════════════════════════════
    # SILVER — dados limpos e normalizados
    # ══════════════════════════════════════════

    @task(task_id="silver_users")
    def silver_users(users: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            INSERT INTO silver_users
                (id, email, username, full_name, phone, city)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                email        = EXCLUDED.email,
                full_name    = EXCLUDED.full_name,
                processed_at = NOW();
        """
        conn = hook.get_conn(); cur = conn.cursor()
        try:
            for u in users:
                name = u.get("name", {})
                full = f"{name.get('firstname','')} {name.get('lastname','')}".strip()
                cur.execute(sql, (
                    u["id"], u.get("email"), u.get("username"),
                    full, u.get("phone"),
                    u.get("address", {}).get("city"),
                ))
            conn.commit()
            log.info("Silver users: %d registros.", len(users))
            return len(users)
        except Exception as e:
            conn.rollback(); raise
        finally:
            cur.close(); conn.close()

    @task(task_id="silver_products")
    def silver_products(products: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            INSERT INTO silver_products (id, title, price, category)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                price        = EXCLUDED.price,
                processed_at = NOW();
        """
        conn = hook.get_conn(); cur = conn.cursor()
        try:
            for p in products:
                cur.execute(sql, (
                    p["id"], p.get("title"), p.get("price"), p.get("category"),
                ))
            conn.commit()
            log.info("Silver products: %d registros.", len(products))
            return len(products)
        except Exception as e:
            conn.rollback(); raise
        finally:
            cur.close(); conn.close()

    @task(task_id="silver_carts")
    def silver_carts(carts: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            INSERT INTO silver_carts (id, user_id, date, total_items)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                total_items  = EXCLUDED.total_items,
                processed_at = NOW();
        """
        from datetime import datetime
        conn = hook.get_conn(); cur = conn.cursor()
        try:
            for c in carts:
                date_val = None
                try:
                    date_val = datetime.fromisoformat(c.get("date", "")).date()
                except Exception:
                    pass
                total = sum(item.get("quantity", 0) for item in c.get("products", []))
                cur.execute(sql, (c["id"], c.get("userId"), date_val, total))
            conn.commit()
            log.info("Silver carts: %d registros.", len(carts))
            return len(carts)
        except Exception as e:
            conn.rollback(); raise
        finally:
            cur.close(); conn.close()

    # ══════════════════════════════════════════
    # FAN-IN — consolidação final
    # ══════════════════════════════════════════

    @task(task_id="finalizar")
    def finalizar(n_users: int, n_products: int, n_carts: int) -> None:
        log.info(
            "✅ Pipeline concluído!\n"
            "   Usuários  : %d\n"
            "   Produtos  : %d\n"
            "   Carrinhos : %d",
            n_users, n_products, n_carts,
        )

    # ── Orquestração ────────────────────────────────────────────
    # Captura paralela
    raw_users    = capturar_users()
    raw_products = capturar_products()
    raw_carts    = capturar_carts()

    # Bronze (paralelo)
    b_users    = bronze_users(raw_users)
    b_products = bronze_products(raw_products)
    b_carts    = bronze_carts(raw_carts)

    # Silver (paralelo, depende do bronze correspondente)
    s_users    = silver_users(raw_users)
    s_products = silver_products(raw_products)
    s_carts    = silver_carts(raw_carts)

    b_users    >> s_users
    b_products >> s_products
    b_carts    >> s_carts

    # Fan-in
    finalizar(s_users, s_products, s_carts)


calango_marketplace_pipeline()
