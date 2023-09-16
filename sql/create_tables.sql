-- These sql statements are responsible for creating the needed tables.

USE DATABASE "OPEN-METEO";
USE SCHEMA "PUBLIC";

-- create a customer id table

CREATE TABLE IF NOT EXISTS CUSTOMER_IDS (
    customer_id_hash varchar,
    customer_id varchar
);

CREATE TABLE IF NOT EXISTS LOCATIONS (
    location_name_hash varchar,
    location_name varchar,
    latitude float,
    lontitude float);

CREATE TABLE IF NOT EXISTS current_weather (
    customer_id_hash varchar,
    location_name_hash varchar,
    weather_time timestamp,
    is_day boolean,
    temperature float,
    windspeed float);