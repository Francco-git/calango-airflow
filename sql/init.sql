-- ============================================================
-- Calango Investimentos — Camada analítica (estilo medalhão)
-- ============================================================

-- ── BRONZE: dados brutos da API ─────────────────────────────

CREATE TABLE IF NOT EXISTS bronze_users (
    id            INT PRIMARY KEY,
    email         VARCHAR(200),
    username      VARCHAR(100),
    firstname     VARCHAR(100),
    lastname      VARCHAR(100),
    phone         VARCHAR(50),
    city          VARCHAR(100),
    raw_json      JSONB,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze_products (
    id            INT PRIMARY KEY,
    title         VARCHAR(300),
    price         NUMERIC(10,2),
    category      VARCHAR(120),
    description   TEXT,
    image         TEXT,
    raw_json      JSONB,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze_carts (
    id            INT PRIMARY KEY,
    user_id       INT,
    date          DATE,
    raw_json      JSONB,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

-- ── SILVER: dados limpos e normalizados ─────────────────────

CREATE TABLE IF NOT EXISTS silver_users (
    id            INT PRIMARY KEY,
    email         VARCHAR(200),
    username      VARCHAR(100),
    full_name     VARCHAR(200),
    phone         VARCHAR(50),
    city          VARCHAR(100),
    processed_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_products (
    id            INT PRIMARY KEY,
    title         VARCHAR(300),
    price         NUMERIC(10,2),
    category      VARCHAR(120),
    processed_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_carts (
    id            INT PRIMARY KEY,
    user_id       INT,
    date          DATE,
    total_items   INT,
    processed_at  TIMESTAMP DEFAULT NOW()
);

-- Índices úteis
CREATE INDEX IF NOT EXISTS idx_silver_products_category ON silver_products (category);
CREATE INDEX IF NOT EXISTS idx_silver_carts_user ON silver_carts (user_id);
