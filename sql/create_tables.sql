-- These sql statements are responsible for creating the needed tables.

-- create a customer id table
CREATE TABLE IF NOT EXISTS CUSTOMER_IDS AS (
    customer_id_hash varchar,
    customer_id varchar
)