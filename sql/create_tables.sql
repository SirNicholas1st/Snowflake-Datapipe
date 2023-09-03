-- These sql statements are responsible for creating the needed tables.

USE DATABASE "OPEN-METEO";
USE SCHEMA "PUBLIC";

-- create a customer id table

CREATE TABLE IF NOT EXISTS CUSTOMER_IDS (
    customer_id_hash varchar,
    customer_id varchar
)