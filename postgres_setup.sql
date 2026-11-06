-- ============================================================
-- PostgreSQL setup for the e-commerce streaming pipeline
-- Run as the superuser:  psql -U postgres -f postgres_setup.sql
-- Safe to re-run: drops and recreates the database and user.
-- ============================================================

-- Start clean
DROP DATABASE IF EXISTS ecommerce WITH (FORCE);
DROP ROLE IF EXISTS ecommerce_user;

-- Dedicated, local-only user for this project
CREATE ROLE ecommerce_user WITH LOGIN PASSWORD 'ecommerce_local_pw';

-- Database owned by that user
CREATE DATABASE ecommerce OWNER ecommerce_user;

-- Switch into the new database, acting as the project user
\c ecommerce
SET ROLE ecommerce_user;

-- Events table
CREATE TABLE events (
    event_id         VARCHAR(36)    PRIMARY KEY,
    event_type       VARCHAR(20)    NOT NULL
                     CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    user_id          INTEGER        NOT NULL,
    product_id       INTEGER        NOT NULL,
    product_name     VARCHAR(100)   NOT NULL,
    category         VARCHAR(50)    NOT NULL,
    price            NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    quantity         INTEGER        NOT NULL CHECK (quantity > 0),
    total_amount     NUMERIC(12, 2) NOT NULL,
    event_timestamp  TIMESTAMP      NOT NULL,
    source_file      VARCHAR(255),
    processed_at     TIMESTAMP      NOT NULL,
    inserted_at      TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- Indexes for common queries (by time, by event type)
CREATE INDEX idx_events_timestamp ON events (event_timestamp);
CREATE INDEX idx_events_type      ON events (event_type);

-- Show the result
\d events