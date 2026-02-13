-- =================================================================
-- SCRIPT PARA REALIZAÇÃO DO ETL E CRIAR O VIEWDATAMART
-- =================================================================

\connect P4_olist

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS datamart;

-- --- (SCD Tipo 2) ---
CREATE TABLE IF NOT EXISTS datamart.dimCliente (
    sk_cliente BIGSERIAL PRIMARY KEY,
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix VARCHAR(10),
    customer_city TEXT,
    customer_state CHAR(2),

    data_inicio_validade TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    data_fim_validade TIMESTAMP WITHOUT TIME ZONE,
    current_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dimcliente_nk_current ON datamart.dimCliente(customer_id, current_flag);

CREATE TABLE IF NOT EXISTS datamart.dimProduto (
    sk_produto BIGSERIAL PRIMARY KEY,
    product_id TEXT,
    product_category_name TEXT,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,

    data_inicio_validade TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    data_fim_validade TIMESTAMP WITHOUT TIME ZONE,
    current_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dimproduto_nk_current ON datamart.dimProduto(product_id, current_flag);

CREATE TABLE IF NOT EXISTS datamart.dimVendedor (
    sk_vendedor BIGSERIAL PRIMARY KEY,
    seller_id TEXT,
    seller_zip_code_prefix VARCHAR(10),
    seller_city TEXT,
    seller_state CHAR(2),

    data_inicio_validade TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    data_fim_validade TIMESTAMP WITHOUT TIME ZONE,
    current_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dimvendedor_nk_current ON datamart.dimVendedor(seller_id, current_flag);

CREATE TABLE IF NOT EXISTS datamart.dimTempo (
    sk_tempo BIGSERIAL PRIMARY KEY,
    data DATE,
    ano INTEGER,
    mes INTEGER,
    dia INTEGER,
    nome_dia TEXT,
    nome_mes TEXT,
    trimestre INTEGER,

    data_inicio_validade TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    data_fim_validade TIMESTAMP WITHOUT TIME ZONE,
    current_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dimtempo_nk_current ON datamart.dimTempo(data, current_flag);

CREATE TABLE IF NOT EXISTS datamart.dimLocalidade (
    sk_localidade BIGSERIAL PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_city TEXT,
    geolocation_state CHAR(2),
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,

    data_inicio_validade TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    data_fim_validade TIMESTAMP WITHOUT TIME ZONE,
    current_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dimlocalidade_nk_current ON datamart.dimLocalidade(geolocation_zip_code_prefix, current_flag);

CREATE TABLE IF NOT EXISTS datamart.dimPagamento (
    sk_pagamento BIGSERIAL PRIMARY KEY,
    payment_type TEXT,
    max_parcelas INTEGER,

    data_inicio_validade TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    data_fim_validade TIMESTAMP WITHOUT TIME ZONE,
    current_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dimpagamento_nk_current ON datamart.dimPagamento(payment_type, current_flag);

-- --- TABELA FATO ---
CREATE TABLE IF NOT EXISTS datamart.fatoVendas (
    sk_fato BIGSERIAL PRIMARY KEY,
    sk_cliente BIGINT REFERENCES datamart.dimCliente(sk_cliente),
    sk_produto BIGINT REFERENCES datamart.dimProduto(sk_produto),
    sk_vendedor BIGINT REFERENCES datamart.dimVendedor(sk_vendedor),
    sk_tempo BIGINT REFERENCES datamart.dimTempo(sk_tempo),
    sk_localidade BIGINT REFERENCES datamart.dimLocalidade(sk_localidade),
    sk_pagamento BIGINT REFERENCES datamart.dimPagamento(sk_pagamento),

    order_id TEXT,
    order_item_id INTEGER,
    qtd_itens INTEGER,
    valor_venda NUMERIC(12,2),
    valor_frete NUMERIC(12,2),
    media_avaliacao NUMERIC(5,2),

    loaded_at TIMESTAMP DEFAULT current_timestamp,

    CONSTRAINT uq_fato_businesskey UNIQUE (order_id, order_item_id)
);


DO $$
BEGIN
    IF to_regclass('staging.stg_customers') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_customers RESTART IDENTITY';
    END IF;

    IF to_regclass('staging.stg_geolocation') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_geolocation RESTART IDENTITY';
    END IF;

    IF to_regclass('staging.stg_order_items') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_order_items RESTART IDENTITY';
    END IF;

    IF to_regclass('staging.stg_order_payments') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_order_payments RESTART IDENTITY';
    END IF;

    IF to_regclass('staging.stg_order_reviews') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_order_reviews RESTART IDENTITY';
    END IF;

    IF to_regclass('staging.stg_orders') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_orders RESTART IDENTITY';
    END IF;

    IF to_regclass('staging.stg_products') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_products RESTART IDENTITY';
    END IF;

    IF to_regclass('staging.stg_sellers') IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE staging.stg_sellers RESTART IDENTITY';
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS staging.stg_geolocation AS TABLE myolist.geolocation WITH NO DATA;
INSERT INTO staging.stg_geolocation SELECT * FROM myolist.geolocation;

CREATE TABLE IF NOT EXISTS staging.stg_sellers AS TABLE myolist.sellers WITH NO DATA;
INSERT INTO staging.stg_sellers SELECT * FROM myolist.sellers;

CREATE TABLE IF NOT EXISTS staging.stg_customers AS TABLE myolist.customers WITH NO DATA;
INSERT INTO staging.stg_customers SELECT * FROM myolist.customers;

CREATE TABLE IF NOT EXISTS staging.stg_orders AS TABLE myolist.orders WITH NO DATA;
INSERT INTO staging.stg_orders SELECT * FROM myolist.orders;

CREATE TABLE IF NOT EXISTS staging.stg_order_payments AS TABLE myolist.order_payments WITH NO DATA;
INSERT INTO staging.stg_order_payments SELECT * FROM myolist.order_payments;

CREATE TABLE IF NOT EXISTS staging.stg_products AS TABLE myolist.products WITH NO DATA;
INSERT INTO staging.stg_products SELECT * FROM myolist.products;

CREATE TABLE IF NOT EXISTS staging.stg_order_items AS TABLE myolist.order_items WITH NO DATA;
INSERT INTO staging.stg_order_items SELECT * FROM myolist.order_items;

CREATE TABLE IF NOT EXISTS staging.stg_order_reviews AS TABLE myolist.order_reviews WITH NO DATA;
INSERT INTO staging.stg_order_reviews SELECT * FROM myolist.order_reviews;

-- ---------------------------
-- DIM CLIENTE (SCD Tipo 2)
-- ---------------------------
WITH S AS (
    SELECT DISTINCT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM staging.stg_customers
),
UPD AS (
    UPDATE datamart.dimCliente d
    SET
        data_fim_validade = current_timestamp,
        current_flag = false,
        updated_at = current_timestamp
    FROM S s
    WHERE d.customer_id = s.customer_id
      AND d.current_flag = true
      AND (
           COALESCE(d.customer_unique_id,'') IS DISTINCT FROM COALESCE(s.customer_unique_id,'')
        OR COALESCE(d.customer_zip_code_prefix,'') IS DISTINCT FROM COALESCE(s.customer_zip_code_prefix,'')
        OR COALESCE(d.customer_city,'') IS DISTINCT FROM COALESCE(s.customer_city,'')
        OR COALESCE(d.customer_state,'') IS DISTINCT FROM COALESCE(s.customer_state,'')
      )
    RETURNING d.customer_id
)
INSERT INTO datamart.dimCliente
    (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state,
     data_inicio_validade, data_fim_validade, current_flag, created_at, updated_at)
SELECT
    s.customer_id,
    s.customer_unique_id,
    s.customer_zip_code_prefix,
    s.customer_city,
    s.customer_state,
    current_timestamp AS data_inicio_validade,
    NULL AS data_fim_validade,
    true AS current_flag,
    current_timestamp AS created_at,
    current_timestamp AS updated_at
FROM S s
WHERE s.customer_id IN (SELECT customer_id FROM UPD)
   OR NOT EXISTS (SELECT 1 FROM datamart.dimCliente d WHERE d.customer_id = s.customer_id AND d.current_flag = true);

-- ---------------------------
-- DIM PRODUTO (SCD Tipo 2)
-- ---------------------------
WITH S AS (
    SELECT DISTINCT
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM staging.stg_products
),
UPD AS (
    UPDATE datamart.dimProduto d
    SET
        data_fim_validade = current_timestamp,
        current_flag = false,
        updated_at = current_timestamp
    FROM S s
    WHERE d.product_id = s.product_id
      AND d.current_flag = true
      AND (
           COALESCE(d.product_category_name,'') IS DISTINCT FROM COALESCE(s.product_category_name,'')
        OR COALESCE(d.product_name_length,0) IS DISTINCT FROM COALESCE(s.product_name_length,0)
        OR COALESCE(d.product_description_length,0) IS DISTINCT FROM COALESCE(s.product_description_length,0)
        OR COALESCE(d.product_photos_qty,0) IS DISTINCT FROM COALESCE(s.product_photos_qty,0)
        OR COALESCE(d.product_weight_g,0) IS DISTINCT FROM COALESCE(s.product_weight_g,0)
        OR COALESCE(d.product_length_cm,0) IS DISTINCT FROM COALESCE(s.product_length_cm,0)
        OR COALESCE(d.product_height_cm,0) IS DISTINCT FROM COALESCE(s.product_height_cm,0)
        OR COALESCE(d.product_width_cm,0) IS DISTINCT FROM COALESCE(s.product_width_cm,0)
      )
    RETURNING d.product_id
)
INSERT INTO datamart.dimProduto
    (product_id, product_category_name, product_name_length, product_description_length,
     product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm,
     data_inicio_validade, data_fim_validade, current_flag, created_at, updated_at)
SELECT
    s.product_id,
    s.product_category_name,
    s.product_name_length,
    s.product_description_length,
    s.product_photos_qty,
    s.product_weight_g,
    s.product_length_cm,
    s.product_height_cm,
    s.product_width_cm,
    current_timestamp AS data_inicio_validade,
    NULL AS data_fim_validade,
    true AS current_flag,
    current_timestamp AS created_at,
    current_timestamp AS updated_at
FROM S s
WHERE s.product_id IN (SELECT product_id FROM UPD)
   OR NOT EXISTS (SELECT 1 FROM datamart.dimProduto d WHERE d.product_id = s.product_id AND d.current_flag = true);

-- ---------------------------
-- DIM VENDEDOR (SCD Tipo 2)
-- ---------------------------
WITH S AS (
    SELECT DISTINCT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM staging.stg_sellers
),
UPD AS (
    UPDATE datamart.dimVendedor d
    SET
        data_fim_validade = current_timestamp,
        current_flag = false,
        updated_at = current_timestamp
    FROM S s
    WHERE d.seller_id = s.seller_id
      AND d.current_flag = true
      AND (
           COALESCE(d.seller_zip_code_prefix,'') IS DISTINCT FROM COALESCE(s.seller_zip_code_prefix,'')
        OR COALESCE(d.seller_city,'') IS DISTINCT FROM COALESCE(s.seller_city,'')
        OR COALESCE(d.seller_state,'') IS DISTINCT FROM COALESCE(s.seller_state,'')
      )
    RETURNING d.seller_id
)
INSERT INTO datamart.dimVendedor
    (seller_id, seller_zip_code_prefix, seller_city, seller_state,
     data_inicio_validade, data_fim_validade, current_flag, created_at, updated_at)
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    current_timestamp AS data_inicio_validade,
    NULL AS data_fim_validade,
    true AS current_flag,
    current_timestamp AS created_at,
    current_timestamp AS updated_at
FROM S s
WHERE s.seller_id IN (SELECT seller_id FROM UPD)
   OR NOT EXISTS (SELECT 1 FROM datamart.dimVendedor d WHERE d.seller_id = s.seller_id AND d.current_flag = true);

-- ---------------------------
-- DIM TEMPO (SCD Tipo 2)
-- ---------------------------
WITH S AS (
    SELECT DISTINCT
        DATE(order_purchase_timestamp) AS data,
        EXTRACT(YEAR FROM order_purchase_timestamp)::INT AS ano,
        EXTRACT(MONTH FROM order_purchase_timestamp)::INT AS mes,
        EXTRACT(DAY FROM order_purchase_timestamp)::INT AS dia,
        TO_CHAR(order_purchase_timestamp, 'Day')::TEXT AS nome_dia,
        TO_CHAR(order_purchase_timestamp, 'Month')::TEXT AS nome_mes,
        EXTRACT(quarter FROM order_purchase_timestamp) AS trimestre
        FROM staging.stg_orders o
        JOIN (
            SELECT '2010-01-01'::DATE + GENERATE_SERIES(0, 11000) AS data_calendario
        ) cal ON DATE(o.order_purchase_timestamp) = cal.data_calendario
        WHERE o.order_purchase_timestamp IS NOT NULL
),
UPD AS (
    UPDATE datamart.dimTempo d
    SET
        data_fim_validade = current_timestamp,
        current_flag = false,
        updated_at = current_timestamp
    FROM S s
    WHERE d.data = s.data
      AND d.current_flag = true
      AND (
           COALESCE(d.ano,0) IS DISTINCT FROM COALESCE(s.ano,0)
        OR COALESCE(d.mes,0) IS DISTINCT FROM COALESCE(s.mes,0)
        OR COALESCE(d.dia,0) IS DISTINCT FROM COALESCE(s.dia,0)
        OR COALESCE(d.nome_dia,'') IS DISTINCT FROM COALESCE(s.nome_dia,'')
        OR COALESCE(d.nome_mes,'') IS DISTINCT FROM COALESCE(s.nome_mes,'')
      )
    RETURNING d.data
)
INSERT INTO datamart.dimTempo
    (data, ano, mes, dia, nome_dia, nome_mes,
     data_inicio_validade, data_fim_validade, current_flag, created_at, updated_at)
SELECT
    s.data,
    s.ano,
    s.mes,
    s.dia,
    s.nome_dia,
    s.nome_mes,
    current_timestamp AS data_inicio_validade,
    NULL AS data_fim_validade,
    true AS current_flag,
    current_timestamp AS created_at,
    current_timestamp AS updated_at
FROM S s
WHERE s.data IN (SELECT data FROM UPD)
   OR NOT EXISTS (SELECT 1 FROM datamart.dimTempo d WHERE d.data = s.data AND d.current_flag = true);

-- ---------------------------
-- DIM LOCALIDADE (SCD Tipo 2)
-- ---------------------------
WITH S AS (
    SELECT
        geolocation_zip_code_prefix,
        MIN(geolocation_city) AS geolocation_city,
        MIN(geolocation_state) AS geolocation_state,
        AVG(geolocation_lat) AS geolocation_lat,
        AVG(geolocation_lng) AS geolocation_lng
    FROM staging.stg_geolocation
    WHERE geolocation_zip_code_prefix IS NOT NULL AND geolocation_zip_code_prefix <> ''
    GROUP BY geolocation_zip_code_prefix
),
UPD AS (
    UPDATE datamart.dimLocalidade d
    SET
        data_fim_validade = current_timestamp,
        current_flag = false,
        updated_at = current_timestamp
    FROM S s
    WHERE d.geolocation_zip_code_prefix = s.geolocation_zip_code_prefix
      AND d.current_flag = true
      AND (
           COALESCE(d.geolocation_city,'') IS DISTINCT FROM COALESCE(s.geolocation_city,'')
        OR COALESCE(d.geolocation_state,'') IS DISTINCT FROM COALESCE(s.geolocation_state,'')
        OR COALESCE(d.geolocation_lat,0) IS DISTINCT FROM COALESCE(s.geolocation_lat,0)
        OR COALESCE(d.geolocation_lng,0) IS DISTINCT FROM COALESCE(s.geolocation_lng,0)
      )
    RETURNING d.geolocation_zip_code_prefix
)
INSERT INTO datamart.dimLocalidade
    (geolocation_zip_code_prefix, geolocation_city, geolocation_state, geolocation_lat, geolocation_lng,
     data_inicio_validade, data_fim_validade, current_flag, created_at, updated_at)
SELECT
    s.geolocation_zip_code_prefix,
    s.geolocation_city,
    s.geolocation_state,
    s.geolocation_lat,
    s.geolocation_lng,
    current_timestamp AS data_inicio_validade,
    NULL AS data_fim_validade,
    true AS current_flag,
    current_timestamp AS created_at,
    current_timestamp AS updated_at
FROM S s
WHERE s.geolocation_zip_code_prefix IN (SELECT geolocation_zip_code_prefix FROM UPD)
   OR NOT EXISTS (SELECT 1 FROM datamart.dimLocalidade d WHERE d.geolocation_zip_code_prefix = s.geolocation_zip_code_prefix AND d.current_flag = true);

-- ---------------------------
-- DIM PAGAMENTO (SCD Tipo 2)
-- ---------------------------
WITH S AS (
    SELECT payment_type, MAX(payment_installments) AS max_parcelas
    FROM staging.stg_order_payments
    GROUP BY payment_type
),
UPD AS (
    UPDATE datamart.dimPagamento d
    SET
        data_fim_validade = current_timestamp,
        current_flag = false,
        updated_at = current_timestamp
    FROM S s
    WHERE d.payment_type = s.payment_type
      AND d.current_flag = true
      AND COALESCE(d.max_parcelas,0) IS DISTINCT FROM COALESCE(s.max_parcelas,0)
    RETURNING d.payment_type
)
INSERT INTO datamart.dimPagamento
    (payment_type, max_parcelas, data_inicio_validade, data_fim_validade, current_flag, created_at, updated_at)
SELECT
    s.payment_type,
    s.max_parcelas,
    current_timestamp AS data_inicio_validade,
    NULL AS data_fim_validade,
    true AS current_flag,
    current_timestamp AS created_at,
    current_timestamp AS updated_at
FROM S s
WHERE s.payment_type IN (SELECT payment_type FROM UPD)
   OR NOT EXISTS (SELECT 1 FROM datamart.dimPagamento d WHERE d.payment_type = s.payment_type AND d.current_flag = true);

-- LOAD TABELA FATO
CREATE TABLE IF NOT EXISTS staging.order_payment_by_order AS
SELECT op.order_id,
       (ARRAY_AGG(op.payment_type ORDER BY op.payment_value DESC))[1] AS payment_type,
       MAX(op.payment_installments) AS max_installments
FROM staging.stg_order_payments op
GROUP BY op.order_id;

CREATE TABLE IF NOT EXISTS staging.review_by_order AS
SELECT orv.order_id,
       AVG(orv.review_score)::NUMERIC(5,2) AS avg_review_score
FROM staging.stg_order_reviews orv
GROUP BY orv.order_id;

-- Inserir novo fatos
WITH base AS (
    SELECT
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.price,
        oi.freight_value,
        o.customer_id,
        DATE(o.order_purchase_timestamp) AS purchase_date,
        c.customer_zip_code_prefix,
        pb.payment_type,
        rb.avg_review_score
    FROM staging.stg_order_items oi
    JOIN staging.stg_orders o ON oi.order_id = o.order_id
    LEFT JOIN staging.stg_customers c ON o.customer_id = c.customer_id
    LEFT JOIN staging.order_payment_by_order pb ON oi.order_id = pb.order_id
    LEFT JOIN staging.review_by_order rb ON oi.order_id = rb.order_id
)
INSERT INTO datamart.fatoVendas
    (sk_cliente, sk_produto, sk_vendedor, sk_tempo, sk_localidade, sk_pagamento,
     order_id, order_item_id, qtd_itens, valor_venda, valor_frete, media_avaliacao, loaded_at)
SELECT
    dc.sk_cliente,
    dp.sk_produto,
    dv.sk_vendedor,
    dt.sk_tempo,
    dl.sk_localidade,
    dpg.sk_pagamento,
    b.order_id,
    b.order_item_id,
    1 AS qtd_itens,
    b.price::NUMERIC(12,2) AS valor_venda,
    b.freight_value::NUMERIC(12,2) AS valor_frete,
    COALESCE(b.avg_review_score, NULL)::NUMERIC(5,2) AS media_avaliacao,
    current_timestamp AS loaded_at
FROM base b
LEFT JOIN datamart.dimCliente dc ON b.customer_id = dc.customer_id AND dc.current_flag = true
LEFT JOIN datamart.dimProduto dp ON b.product_id = dp.product_id AND dp.current_flag = true
LEFT JOIN datamart.dimVendedor dv ON b.seller_id = dv.seller_id AND dv.current_flag = true
LEFT JOIN datamart.dimTempo dt ON b.purchase_date = dt.data AND dt.current_flag = true
LEFT JOIN datamart.dimLocalidade dl ON b.customer_zip_code_prefix = dl.geolocation_zip_code_prefix AND dl.current_flag = true
LEFT JOIN datamart.dimPagamento dpg ON b.payment_type = dpg.payment_type AND dpg.current_flag = true
ON CONFLICT ON CONSTRAINT uq_fato_businesskey DO NOTHING;


DROP TABLE IF EXISTS staging.order_payment_by_order;
DROP TABLE IF EXISTS staging.review_by_order;


-- ---------------------------
-- Criar View
-- ---------------------------

CREATE OR REPLACE VIEW datamart.vw_fatoVendas_expandido AS
SELECT
  f.sk_fato,
  f.order_id,
  f.order_item_id,
  f.qtd_itens,
  f.valor_venda,
  f.valor_frete,
  f.media_avaliacao,
  f.loaded_at,

  dt.data       AS purchase_date,
  dt.ano,
  dt.mes,
  dt.dia,

  dcli.customer_id,
  dcli.customer_unique_id,
  dcli.customer_city AS customer_city,
  dcli.customer_state AS customer_state,
  dcli.customer_zip_code_prefix,

  dprod.product_id,
  dprod.product_category_name,
  dprod.product_name_length,

  dv.seller_id,
  dv.seller_city,
  dv.seller_state,

  dl.geolocation_city,
  dl.geolocation_state,
  dl.geolocation_zip_code_prefix,
  dl.geolocation_lat,
  dl.geolocation_lng,

  dpg.payment_type,
  dpg.max_parcelas

FROM datamart.fatoVendas f
LEFT JOIN datamart.dimTempo dt ON f.sk_tempo = dt.sk_tempo
LEFT JOIN datamart.dimCliente dcli ON f.sk_cliente = dcli.sk_cliente
LEFT JOIN datamart.dimProduto dprod ON f.sk_produto = dprod.sk_produto
LEFT JOIN datamart.dimVendedor dv ON f.sk_vendedor = dv.sk_vendedor
LEFT JOIN datamart.dimLocalidade dl ON f.sk_localidade = dl.sk_localidade
LEFT JOIN datamart.dimPagamento dpg ON f.sk_pagamento = dpg.sk_pagamento;

DO $$
BEGIN
    RAISE NOTICE 'View criada.';
END;
$$;

COMMIT;
