# DDL для Postgres + начальные данные


-- таблица транзакций
CREATE TABLE IF NOT EXIST public.transactions (
    id            BIGSERIAL PRIMARY KEY,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    account_id    BIGINT NOT NULL,
    amount        NUMERIC(12,2),
    currency      VARCHAR(3),
    merchant_mcc  VARCHAR(4),
    country_iso   CHAR(2),
    is_fraud      BOOLEAN DEFAULT FALSE
);

-- несколько тестовых строк
INSERT INTO public.transactions (account_id, amount, currency, merchant_mcc, country_iso)
SELECT 1000 + i, (random()*500)::numeric(12,2), 'EUR', '5411', 'NL'
FROM generate_series(1,50) AS t(i);

-- публикация WAL
CREATE PUBLICATION pub_tx FOR TABLE public.transactions;
