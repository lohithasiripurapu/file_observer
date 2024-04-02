CREATE TABLE sales_data (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    quantity INTEGER,
    price NUMERIC(10,2)
);


CREATE TABLE insert_log (
    operation_type CHAR(1),
    operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name TEXT,
    primary_key INT
);

CREATE TABLE update_log (
    operation_type CHAR(1),
    operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name TEXT,
    primary_key INT
);

CREATE TABLE delete_log (
    operation_type CHAR(1),
    operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name TEXT,
    primary_key INT
);
=============

CREATE OR REPLACE FUNCTION log_insert_trigger() RETURNS trigger AS $emp_stamp$
    BEGIN
       INSERT INTO insert_log (operation_type, table_name, primary_key)
       VALUES ('I', TG_TABLE_NAME, NEW.id);
       RETURN NEW;
    END;
$emp_stamp$ LANGUAGE plpgsql;



CREATE TRIGGER sales_data_insert_trigger
AFTER INSERT ON sales_data
FOR EACH ROW
EXECUTE FUNCTION log_insert_trigger();

==============


CREATE OR REPLACE FUNCTION log_update_trigger() RETURNS trigger AS $emp_stamp$
    BEGIN
       INSERT INTO update_log (operation_type, table_name, primary_key)
       VALUES ('U', TG_TABLE_NAME, NEW.id);
       RETURN NEW;
    END;
$emp_stamp$ LANGUAGE plpgsql;



CREATE TRIGGER sales_data_update_trigger
AFTER UPDATE ON sales_data
FOR EACH ROW
EXECUTE FUNCTION log_update_trigger();

================


CREATE OR REPLACE FUNCTION log_delete_trigger() RETURNS trigger AS $emp_stamp$
    BEGIN
       INSERT INTO delete_log (operation_type, table_name, primary_key)
       VALUES ('D', TG_TABLE_NAME, NEW.id);
       RETURN NEW;
    END;
$emp_stamp$ LANGUAGE plpgsql;



CREATE TRIGGER sales_data_delete_trigger
AFTER DELETE ON sales_data
FOR EACH ROW
EXECUTE FUNCTION log_delete_trigger();







-- Insert data
INSERT INTO sales_data (product_name, quantity, price) VALUES ('Product A', 100, 10.99);
INSERT INTO sales_data (product_name, quantity, price) VALUES ('Product B', 200, 20.49);

-- Update data
UPDATE sales_data SET quantity = 150 WHERE product_name = 'Product A';

-- Delete data
DELETE FROM sales_data WHERE product_name = 'Product B';


-- Query insert_log
SELECT * FROM insert_log;

-- Query update_log
SELECT * FROM update_log;

-- Query delete_log
SELECT * FROM delete_log;
