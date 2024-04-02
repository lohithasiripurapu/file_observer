-- Source Tables

CREATE TABLE sales_data (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    quantity INTEGER,
    price NUMERIC(10,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging Tables

CREATE TABLE sales_data_staging (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    quantity INTEGER,
    price NUMERIC(10,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Incremental Load

-- Incremental load: Identify new/updated records
INSERT INTO sales_data_staging (product_name, quantity, price, last_updated)
SELECT product_name, quantity, price, last_updated
FROM sales_data
WHERE last_updated > last_load_timestamp;

-- Incremental load: Identify deleted records
DELETE FROM sales_data_staging
WHERE id NOT IN (SELECT id FROM sales_data);


-- Delta Processing

-- Apply new/updated records
INSERT INTO sales_data (product_name, quantity, price, last_updated)
SELECT product_name, quantity, price, last_updated
FROM sales_data_staging
ON CONFLICT (id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    quantity = EXCLUDED.quantity,
    price = EXCLUDED.price,
    last_updated = EXCLUDED.last_updated;

-- Apply deletes
DELETE FROM sales_data
WHERE id IN (SELECT id FROM sales_data_staging);


