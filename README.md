# Calango Investimentos — Airflow Distribuído 🐊

Pipeline de ingestão paralela com **Apache Airflow + Celery Executor**, atendendo aos objetivos de escalabilidade do marketplace adquirido pela Calango Investimentos.

---

## 🏗️ Arquitetura do Pipeline

```
[FakeStore API]
      │
      ├── GET /users    ──► capturar_users
      ├── GET /products ──► capturar_products   (paralelo)
      └── GET /carts    ──► capturar_carts
              │
              ▼
      bronze_users / bronze_products / bronze_carts   (raw)
              │
              ▼
      silver_users / silver_products / silver_carts   (limpo)
              │
              ▼
           finalizar  (fan-in)
```

### Infraestrutura distribuída

```
[Redis] ◄── broker ──► [Celery Workers x N]
   │                          │
[PostgreSQL] ◄── metadata ────┘
   │
[Webserver] ── UI: localhost:8080
[Flower]    ── UI: localhost:5555
```

---

## 🚀 Passo a passo da atividade

### ETAPA 1 — Subir com 3 Workers fixos

```bash
docker compose up -d --scale airflow-worker=3
```

Verifique os containers:
```bash
docker compose ps
```

Acesse:
- **Airflow UI**: http://localhost:8080 (admin / admin)
- **Flower UI**: http://localhost:5555

> 📸 **Print necessário**: `docker compose ps` + Flower mostrando 3 workers ativos

---

### ETAPA 2 — Executar o DAG e evidenciar

No Airflow UI, ative o DAG `calango_marketplace_pipeline` e clique em **Trigger DAG**.

> 📸 **Print necessário**: Graph view do DAG com execução bem-sucedida

---

### ETAPA 3 — Derrubar os workers e destruir os containers

```bash
docker compose stop airflow-worker
docker compose rm -f airflow-worker
```

Verifique que os workers sumiram:
```bash
docker compose ps
```

> 📸 **Print necessário**: terminal mostrando workers removidos + Flower sem workers

---

### ETAPA 4 — Subir com workers dinâmicos (5 workers)

```bash
docker compose up -d --scale airflow-worker=5
```

```bash
docker compose ps
```

> 📸 **Print necessário**: Flower mostrando 5 workers ativos

---

### ETAPA 5 — Escalar para 2 workers SEM derrubar os serviços

```bash
docker compose up -d --scale airflow-worker=2 --no-recreate
```

```bash
docker compose ps
```

> 📸 **Print necessário**: Flower mostrando apenas 2 workers ativos

---

## 📁 Estrutura do projeto

```
calango-airflow/
├── dags/
│   └── calango_marketplace_pipeline.py   # DAG principal
├── sql/
│   └── init.sql                          # DDL das tabelas bronze e silver
├── logs/                                 # Logs do Airflow (runtime)
├── plugins/                              # Plugins customizados (se houver)
├── config/                               # Configs extras
├── docker-compose.yml                    # Orquestração com Celery
└── README.md
```

---

## 🔧 Serviços Docker

| Serviço | Porta | Descrição |
|---------|-------|-----------|
| `airflow-webserver` | 8080 | Interface web do Airflow |
| `airflow-scheduler` | — | Agendador das DAGs |
| `airflow-worker` | — | Worker Celery (escalável) |
| `airflow-flower` | 5555 | Monitor dos workers Celery |
| `postgres` | 5432 | Banco de dados |
| `redis` | 6379 | Broker de mensagens do Celery |

---

## 📊 Tabelas

| Tabela | Camada | Descrição |
|--------|--------|-----------|
| `bronze_users` | Bronze | Dados brutos de usuários |
| `bronze_products` | Bronze | Dados brutos de produtos |
| `bronze_carts` | Bronze | Dados brutos de carrinhos |
| `silver_users` | Silver | Usuários limpos e normalizados |
| `silver_products` | Silver | Produtos limpos |
| `silver_carts` | Silver | Carrinhos com total de itens |

---

## ✅ Requisitos cobertos

- [x] Captura paralela de Users, Products e Carts
- [x] TaskFlow API (`@dag`, `@task`)
- [x] XComs automáticos via `return`
- [x] Timezone `America/Sao_Paulo` com `pendulum`
- [x] `catchup=False`
- [x] CeleryExecutor com Redis como broker
- [x] Workers fixos em 3: `--scale airflow-worker=3`
- [x] Workers dinâmicos: `--scale airflow-worker=N`
- [x] Escalar sem derrubar: `--no-recreate`
- [x] Flower UI para monitoramento dos workers
- [x] Esteira medalhão Bronze → Silver

---

## 👤 Autor

**Francco La Femina** — Arquiteto de Soluções de Engenharia de Dados  
GitHub: [@Francco-git](https://github.com/Francco-git)
