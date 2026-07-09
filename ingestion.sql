use role accountadmin;

use warehouse transformer;

use database raw;

-- create or replace schema the_look_ecommerce;

use schema the_look_ecommerce;

-- -- Workflow
-- 1. Add data to buckets on GCP 
-- 2. Create tables in Snowflake - 
--     a. Create an integration with GCP Bucket 
--     b. Create External Stage
--     c. Copy from External Stage 
-- 3. Create a new DBT Project 
--     a. Link to snowflake tables and make initial layout - stage files, source files 
--     b. Write tests for sources 
--     c. Create Kimball style data warehouse having facts and dimension tables 

-- create a GCS integration 
CREATE OR REPLACE STORAGE INTEGRATION THE_LOOK_STORAGE
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://the-look-ecommerce-data/');

DESC INTEGRATION THE_LOOK_STORAGE;

-- create the stage 
CREATE OR REPLACE STAGE GCS_THE_LOOK_STAGE
    URL = 'gcs://the-look-ecommerce-data/'
    STORAGE_INTEGRATION = THE_LOOK_STORAGE;

LIST @GCS_THE_LOOK_STAGE;

-- create a file format 
CREATE OR REPLACE FILE FORMAT THE_LOOK_CSV_FORMAT
    TYPE = 'CSV'
    COMPRESSION = 'GZIP'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS UTC';



-- create individual tables and copy data into them
-- distribution_centers
CREATE OR REPLACE TABLE distribution_centers (
    id INTEGER,
    name VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    distribution_center_geom GEOGRAPHY,
    _batched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_source VARCHAR(100)
);

-- normal COPY command 
-- COPY INTO distribution_centers
--     FROM @GCS_THE_LOOK_STAGE/distribution_centers/
--     FILE_FORMAT = (FORMAT_NAME = THE_LOOK_CSV_FORMAT)
--     ON_ERROR = 'ABORT_STATEMENT';

-- Transformation on LOAD 
COPY INTO distribution_centers FROM (
    SELECT 
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        SYSDATE(),
        METADATA$FILENAME
    FROM @GCS_THE_LOOK_STAGE/distribution_centers/ (file_format => 'THE_LOOK_CSV_FORMAT') t
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM distribution_centers LIMIT 10;
    
SELECT count(*) FROM distribution_centers;


-- events
CREATE OR REPLACE TABLE events (
    id INTEGER,
    user_id INTEGER,
    sequence_number INTEGER,
    session_id VARCHAR(50),
    created_at VARCHAR(50),
    ip_address VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(50),
    browser VARCHAR(50),
    traffic_source VARCHAR(50),
    uri VARCHAR(100),
    event_type VARCHAR(50),
    _batched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_source VARCHAR(100)
);

COPY INTO events FROM (
    SELECT 
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        t.$9,
        t.$10,
        t.$11,
        t.$12,
        t.$13,
        SYSDATE(),
        METADATA$FILENAME
    FROM @GCS_THE_LOOK_STAGE/events/ (file_format => 'THE_LOOK_CSV_FORMAT') t
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM events LIMIT 10;
    
SELECT count(*) FROM events;



-- inventory_items
CREATE OR REPLACE TABLE inventory_items (
    id	INTEGER, 
    product_id	INTEGER, 
    created_at	VARCHAR(50),
    sold_at	VARCHAR(50),
    cost	FLOAT, 
    product_category	VARCHAR(50), 
    product_name	VARCHAR(500), 
    product_brand	VARCHAR(500), 
    product_retail_price	FLOAT, 
    product_department	VARCHAR(500), 
    product_sku	VARCHAR(500), 
    product_distribution_center_id	INTEGER,
    _batched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_source VARCHAR(100)
);

COPY INTO inventory_items FROM (
    SELECT 
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        t.$9,
        t.$10,
        t.$11,
        t.$12,
        SYSDATE(),
        METADATA$FILENAME
    FROM @GCS_THE_LOOK_STAGE/inventory_items/ (file_format => 'THE_LOOK_CSV_FORMAT') t
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM inventory_items LIMIT 10;
    
SELECT count(*) FROM inventory_items;



-- order_items
CREATE OR REPLACE TABLE order_items (
    id	INTEGER, 
    order_id	INTEGER, 
    user_id	INTEGER, 
    product_id	INTEGER, 
    inventory_item_id	INTEGER, 
    status	VARCHAR(50), 
    created_at	VARCHAR(50),
    shipped_at	VARCHAR(50),
    delivered_at	VARCHAR(50),
    returned_at	VARCHAR(50),
    sale_price	FLOAT,
    _batched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_source VARCHAR(100)
);

COPY INTO order_items FROM (
    SELECT 
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        t.$9,
        t.$10,
        t.$11,
        SYSDATE(),
        METADATA$FILENAME
    FROM @GCS_THE_LOOK_STAGE/order_items/ (file_format => 'THE_LOOK_CSV_FORMAT') t
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM order_items LIMIT 10;
    
SELECT count(*) FROM order_items;



-- orders
CREATE OR REPLACE TABLE orders (
    order_id	INTEGER, 
    user_id	INTEGER, 
    status	VARCHAR(500), 
    gender	VARCHAR(500), 
    created_at	VARCHAR(50), 
    returned_at	VARCHAR(50), 
    shipped_at	VARCHAR(50), 
    delivered_at	VARCHAR(50), 
    num_of_item	INTEGER,
    _batched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_source VARCHAR(100)
);

COPY INTO orders FROM (
    SELECT 
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        t.$9,
        SYSDATE(),
        METADATA$FILENAME
    FROM @GCS_THE_LOOK_STAGE/orders/ (file_format => 'THE_LOOK_CSV_FORMAT') t
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM orders LIMIT 10;
    
SELECT count(*) FROM orders;



-- products
CREATE OR REPLACE TABLE products (
    id	INTEGER,
    cost	FLOAT,
    category	VARCHAR(50), 
    name	VARCHAR(500), 
    brand	VARCHAR(500), 
    retail_price	FLOAT,
    department	VARCHAR(500),
    sku	VARCHAR(500),
    distribution_center_id	INTEGER,
    _batched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_source VARCHAR(100)
);

COPY INTO products FROM (
    SELECT 
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        t.$9,
        SYSDATE(),
        METADATA$FILENAME
    FROM @GCS_THE_LOOK_STAGE/products/ (file_format => 'THE_LOOK_CSV_FORMAT') t
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM products LIMIT 10;
    
SELECT count(*) FROM products;



-- users
CREATE OR REPLACE TABLE users (
    id	INTEGER,
    first_name	VARCHAR(500),
    last_name	VARCHAR(500),
    email	VARCHAR(500),
    age	INTEGER,
    gender	VARCHAR(500),
    state	VARCHAR(500),
    street_address	VARCHAR(500),
    postal_code	VARCHAR(500),
    city	VARCHAR(500),
    country	VARCHAR(500),
    latitude	FLOAT,
    longitude	FLOAT,
    traffic_source	VARCHAR(500),
    created_at	VARCHAR(50),
    user_geom	GEOGRAPHY,
    _batched_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_source VARCHAR(100)
);

COPY INTO users FROM (
    SELECT 
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        t.$9,
        t.$10,
        t.$11,
        t.$12,
        t.$13,
        t.$14,
        t.$15,
        t.$16,
        SYSDATE(),
        METADATA$FILENAME
    FROM @GCS_THE_LOOK_STAGE/users/ (file_format => 'THE_LOOK_CSV_FORMAT') t
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT * FROM users LIMIT 10;
    
SELECT count(*) FROM users;


SELECT 
    lower(table_name)
    -- lower(column_name), 
    -- lower(data_type), 
    -- ordinal_position
FROM RAW.information_schema.columns 
WHERE table_schema = 'THE_LOOK_ECOMMERCE'
GROUP BY 1
ORDER BY 1;


