-- =================================================================
-- SCRIPT PARA CRIAR E POPULAR AS TABELAS
-- =================================================================

CREATE DATABASE "P4_olist"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

\connect P4_olist

CREATE SCHEMA IF NOT EXISTS myolist;

DROP TABLE IF EXISTS myolist.geolocation;
CREATE TABLE myolist.geolocation (
    geolocation_zip_code_prefix VARCHAR(20),
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
    geolocation_city TEXT,
    geolocation_state CHAR(2)
);

DROP TABLE IF EXISTS myolist.sellers;
CREATE TABLE myolist.sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(20),
    seller_city TEXT,
    seller_state CHAR(2)
);

DROP TABLE IF EXISTS myolist.customers;
CREATE TABLE myolist.customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix VARCHAR(20),
    customer_city TEXT,
    customer_state CHAR(2)
);

DROP TABLE IF EXISTS myolist.orders;
CREATE TABLE myolist.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES myolist.customers (customer_id) 
        ON DELETE RESTRICT ON UPDATE CASCADE
);

DROP TABLE IF EXISTS myolist.order_payments;
CREATE TABLE myolist.order_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES myolist.orders (order_id) 
        ON DELETE RESTRICT ON UPDATE CASCADE
);

DROP TABLE IF EXISTS myolist.products;
CREATE TABLE myolist.products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

DROP TABLE IF EXISTS myolist.order_items;
CREATE TABLE myolist.order_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES myolist.orders (order_id) 
        ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (product_id) REFERENCES myolist.products (product_id) 
        ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (seller_id) REFERENCES myolist.sellers (seller_id) 
        ON DELETE RESTRICT ON UPDATE CASCADE
);

DROP TABLE IF EXISTS myolist.order_reviews;
CREATE TABLE myolist.order_reviews (
    review_id TEXT PRIMARY KEY,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES myolist.orders (order_id) 
        ON DELETE RESTRICT ON UPDATE CASCADE
);

DROP TABLE IF EXISTS myolist.staging_order_reviews, myolist.staging_orders, myolist.staging_products;

CREATE TABLE myolist.staging_order_reviews (review_id TEXT, order_id TEXT, review_score TEXT, review_comment_title TEXT, review_comment_message TEXT, review_creation_date TEXT, review_answer_timestamp TEXT);
CREATE TABLE myolist.staging_orders (order_id TEXT, customer_id TEXT, order_status TEXT, order_purchase_timestamp TEXT, order_approved_at TEXT, order_delivered_carrier_date TEXT, order_delivered_customer_date TEXT, order_estimated_delivery_date TEXT);
CREATE TABLE myolist.staging_products (product_id TEXT, product_category_name TEXT, product_name_length TEXT, product_description_length TEXT, product_photos_qty TEXT, product_weight_g TEXT, product_length_cm TEXT, product_height_cm TEXT, product_width_cm TEXT);

DO $$
BEGIN
    RAISE NOTICE 'Criacao das tabelas concluida.';
END;
$$;

\copy myolist.geolocation FROM 'C:/Users/USER/Desktop/tables/olist_geolocation_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252', NULL '')
\copy myolist.customers FROM 'C:/Users/USER/Desktop/tables/olist_customers_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252', NULL '')
\copy myolist.sellers FROM 'C:/Users/USER/Desktop/tables/olist_sellers_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252', NULL '')
\copy myolist.staging_products FROM 'C:/Users/USER/Desktop/tables/olist_products_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252')
\copy myolist.staging_orders FROM 'C:/Users/USER/Desktop/tables/olist_orders_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252')

INSERT INTO myolist.orders SELECT order_id, customer_id, order_status, NULLIF(order_purchase_timestamp, '')::TIMESTAMP, NULLIF(order_approved_at, '')::TIMESTAMP, NULLIF(order_delivered_carrier_date, '')::TIMESTAMP, NULLIF(order_delivered_customer_date, '')::TIMESTAMP, NULLIF(order_estimated_delivery_date, '')::TIMESTAMP FROM myolist.staging_orders;
INSERT INTO myolist.products SELECT product_id, product_category_name, NULLIF(product_name_length, '')::INTEGER, NULLIF(product_description_length, '')::INTEGER, NULLIF(product_photos_qty, '')::INTEGER, NULLIF(product_weight_g, '')::INTEGER, NULLIF(product_length_cm, '')::INTEGER, NULLIF(product_height_cm, '')::INTEGER, NULLIF(product_width_cm, '')::INTEGER FROM myolist.staging_products;

\copy myolist.staging_order_reviews FROM 'C:/Users/USER/Desktop/tables/olist_order_reviews_clean.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252', NULL '\N')
\copy myolist.order_payments FROM 'C:/Users/USER/Desktop/tables/olist_order_payments_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252', NULL '')
\copy myolist.order_items FROM 'C:/Users/USER/Desktop/tables/olist_order_items_dataset.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'WIN1252', NULL '')

TRUNCATE TABLE myolist.order_reviews;
INSERT INTO myolist.order_reviews
SELECT DISTINCT ON (review_id) review_id, order_id, NULLIF(review_score, '')::INTEGER, review_comment_title, review_comment_message, NULLIF(review_creation_date, '')::TIMESTAMP, NULLIF(review_answer_timestamp, '')::TIMESTAMP FROM myolist.staging_order_reviews ON CONFLICT (review_id) DO NOTHING;

DO $$
BEGIN
    RAISE NOTICE 'Importacao dos dados concluida.';
END;
$$;
