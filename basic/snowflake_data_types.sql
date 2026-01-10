DROP TABLE DE_DATABASE.DE_PROJECT.snowflake_data_types_demo

USE ROLE accountadmin;

USE WAREHOUSE COMPUTE_WH;

USE DATABASE DE_DATABASE;

USE SCHEMA DE_PROJECT;

-- ============================================
-- CREATE TABLE WITH ALL SNOWFLAKE DATA TYPES
-- ============================================
CREATE OR REPLACE TABLE DE_DATABASE.DE_PROJECT.snowflake_data_types_demo
(
    /* =========================
       Numeric Data Types
    ========================= */
    col_number            NUMBER(38, 2),
    col_int               INT,
    col_integer           INTEGER,
    col_bigint            BIGINT,
    col_smallint          SMALLINT,
    col_byteint           BYTEINT,
    col_float             FLOAT,
    col_double            DOUBLE,
    col_real              REAL,
    col_decimal           DECIMAL(10, 3),
    col_numeric           NUMERIC(15, 5),

    /* =========================
       String & Binary
    ========================= */
    col_varchar            VARCHAR(16777216),
    col_char               CHAR(10),
    col_string             STRING,
    col_text               TEXT,
    col_binary             BINARY,

    /* =========================
       Boolean
    ========================= */
    col_boolean            BOOLEAN,

    /* =========================
       Date & Time
    ========================= */
    col_date               DATE,
    col_time               TIME,
    col_timestamp          TIMESTAMP,
    col_timestamp_ltz      TIMESTAMP_LTZ,
    col_timestamp_ntz      TIMESTAMP_NTZ,
    col_timestamp_tz       TIMESTAMP_TZ,

    /* =========================
       Semi-structured
    ========================= */
    col_variant            VARIANT,
    col_object             OBJECT,
    col_array              ARRAY,

    /* =========================
       Geospatial
    ========================= */
    col_geography          GEOGRAPHY,

    /* =========================
       Vector
    ========================= */
    col_vector             VECTOR(FLOAT, 3)
);


INSERT INTO DE_DATABASE.DE_PROJECT.snowflake_data_types_demo
SELECT
    12345.67, 100, 200, 300, 400, 600,
    123.456, 901.234, 432.109, 1234.567, 12345.67890,

    'Sample VARCHAR text', 'CHAR10', 'STRING value', 'This is TEXT data',
    TO_BINARY('48656C6C6F', 'HEX'),

    TRUE,

    '2025-12-28', '17:30:00',
    '2025-12-28 17:30:00',
    '2025-12-28 17:30:00 +05:30',
    '2025-12-28 17:30:00',
    '2025-12-28 17:30:00 +05:30',

    PARSE_JSON('{"name":"John","age":30}'),
    OBJECT_CONSTRUCT('key1','value1'),
    ARRAY_CONSTRUCT('a','b','c'),

    TO_GEOGRAPHY('POINT(77.2090 28.6139)'),

    ARRAY_CONSTRUCT(1.5, 2.7, 3.2)::VECTOR(FLOAT, 3);


INSERT INTO DE_DATABASE.DE_PROJECT.snowflake_data_types_demo
SELECT
    -- Numeric
    55555.55, 111, 222, 333, 444, 666,
    11.11, 44.44, 66.66, 777.888, 99999.99999,

    -- String & Binary
    'Third row VARCHAR', 'THIRDCHAR', 'Third STRING value', 'Third TEXT data',
    TO_BINARY('5468697264526F77', 'HEX'),

    -- Boolean
    TRUE,

    -- Date & Time
    '2024-06-20', '14:25:30',
    '2024-06-20 14:25:30',
    '2024-06-20 14:25:30 +00:00',
    '2024-06-20 14:25:30',
    '2024-06-20 14:25:30 +00:00',

    -- Semi-structured
    PARSE_JSON('{"product":"Snowflake","version":"2025"}'),
    OBJECT_CONSTRUCT('status','active','priority','high'),
    ARRAY_CONSTRUCT('red','green','blue'),

    -- Geography
    TO_GEOGRAPHY('POINT(12.4964 41.9028)'),  -- Rome

    -- Vector (must be SELECT, not VALUES)
    ARRAY_CONSTRUCT(7.5, 8.6, 9.7)::VECTOR(FLOAT, 3);


-- ============================================
-- VERIFY THE DATA
-- ============================================
SELECT * FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;

-- Count rows
SELECT COUNT(*) as total_rows FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;


-- =====================================================
-- 1. INSPECT TABLE STRUCTURE
-- =====================================================
DESCRIBE TABLE DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;

-- =====================================================
-- 2. RUNTIME TYPE CHECKS (TYPEOF)
-- =====================================================
SELECT
    TYPEOF(col_number)   AS number_type,
    TYPEOF(col_variant)  AS variant_type,
    TYPEOF(col_object)   AS object_type,
    TYPEOF(col_array)    AS array_type
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;

-- =====================================================
-- 3. VIEW RAW SEMI-STRUCTURED DATA
-- =====================================================
SELECT
    col_variant,
    col_object,
    col_array
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;

-- =====================================================
-- 4. ARITHMETIC ON STANDARD NUMERIC COLUMN
-- =====================================================
SELECT
    col_number,
    col_number * 2 AS doubled_number
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;

-- =====================================================
-- 5. ARITHMETIC ON VARIANT (SAFE & CORRECT)
-- =====================================================
SELECT
    col_variant,
    TRY_TO_NUMBER(col_variant:'age') * 2 AS variant_doubled_safe
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;

-- =====================================================
-- 6. OBJECT FIELD ACCESS (JSON NAVIGATION)
-- =====================================================

SELECT
    col_object,
    col_object:status::STRING,
    col_object:priority::STRING
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo
WHERE IS_OBJECT(col_object);

-- =====================================================
-- 7. FLATTEN ARRAY DATA
-- =====================================================
SELECT
    t.col_string,
    f.index AS array_index,
    f.value AS array_value
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo t,
     LATERAL FLATTEN(input => t.col_array) f;

-- =====================================================
-- 8. CREATE PARSED / RELATIONAL TABLE (CTAS)
-- =====================================================
CREATE OR REPLACE TABLE DE_DATABASE.DE_PROJECT.parsed_snowflake_data_types AS
SELECT
    col_int,
    col_varchar,
    col_number,
    TRY_TO_NUMBER(col_variant)            AS variant_number,
    col_object:'key1'::STRING             AS object_key1,
    col_object:'key2'::NUMBER             AS object_key2,
    ARRAY_SIZE(col_array)                 AS array_size,
    col_geography,
    col_vector
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo;

CREATE OR REPLACE TABLE DE_DATABASE.DE_PROJECT.parsed_snowflake_data_types AS
SELECT
    col_int,
    col_varchar,
    col_number,

    /* VARIANT → STRING → NUMBER (NO TRY_CAST EVER) */
    --TO_NUMBER(TRY_TO_VARCHAR(col_variant:age)) AS variant_age,
    --col_variant:age::NUMBER AS variant_age,
    col_variant['age'] variant_age,

    /* OBJECT fields */
    col_object:status::STRING   AS status,
    col_object:priority::STRING AS priority,

    /* ARRAY */
    ARRAY_SIZE(col_array)       AS array_size,

    /* Pass-through */
    col_geography,
    col_vector
FROM DE_DATABASE.DE_PROJECT.snowflake_data_types_demo
WHERE IS_OBJECT(col_variant)
  AND IS_OBJECT(col_object);



-- =====================================================
-- 9. VALIDATE NEW TABLE
-- =====================================================
DESCRIBE TABLE DE_DATABASE.DE_PROJECT.parsed_snowflake_data_types;

SELECT *
FROM DE_DATABASE.DE_PROJECT.parsed_snowflake_data_types;

SELECT COUNT(*) AS total_rows
FROM DE_DATABASE.DE_PROJECT.parsed_snowflake_data_types;

-- =====================================================
-- 10. OPTIONAL CLEANUP
-- =====================================================
-- DROP TABLE IF EXISTS DE_DATABASE.DE_PROJECT.parsed_snowflake_data_types;

--===========================================================================================


-- =====================================================
-- DEMO: LATERAL FLATTEN WITH NESTED JSON (FROSTBYTE STYLE)
-- =====================================================

-- -----------------------------------------------------
-- 1. CREATE DEMO TABLE
-- -----------------------------------------------------
CREATE OR REPLACE TABLE DE_DATABASE.DE_PROJECT.flatten_demo (
    menu_id        NUMBER,
    menu_name      STRING,
    health_metrics VARIANT
);

-- -----------------------------------------------------
-- 2. INSERT SAMPLE DATA (OBJECT WITH NESTED ARRAY)
-- -----------------------------------------------------
INSERT INTO DE_DATABASE.DE_PROJECT.flatten_demo
SELECT
    1,
    'Veggie Burger',
    PARSE_JSON('{
        "menu_item_health_metrics": [
            { "metric": "calories", "value": 350 },
            { "metric": "fat", "value": 12 },
            { "metric": "protein", "value": 18 }
        ]
    }')
UNION ALL
SELECT
    2,
    'Chicken Wrap',
    PARSE_JSON('{
        "menu_item_health_metrics": [
            { "metric": "calories", "value": 450 },
            { "metric": "fat", "value": 20 },
            { "metric": "protein", "value": 30 }
        ]
    }');

-- -----------------------------------------------------
-- 3. VERIFY RAW DATA
-- -----------------------------------------------------
SELECT * 
FROM DE_DATABASE.DE_PROJECT.flatten_demo;

-- -----------------------------------------------------
-- 4. CHECK DATA TYPE
-- -----------------------------------------------------
SELECT
    TYPEOF(health_metrics) AS health_metrics_type
FROM DE_DATABASE.DE_PROJECT.flatten_demo;

-- -----------------------------------------------------
-- 5. LATERAL FLATTEN (FROSTBYTE-EQUIVALENT QUERY)
-- -----------------------------------------------------
SELECT
    d.menu_id,
    d.menu_name,
    f.index                                AS metric_index,
    f.value:metric::STRING                AS metric_name,
    f.value:value::NUMBER                 AS metric_value
FROM DE_DATABASE.DE_PROJECT.flatten_demo d,
     LATERAL FLATTEN(
         input => d.health_metrics:menu_item_health_metrics
     ) f;

-- -----------------------------------------------------
-- 6. OPTIONAL: AGGREGATE AFTER FLATTEN
-- -----------------------------------------------------
SELECT
    d.menu_name,
    SUM(f.value:value::NUMBER) AS total_metric_value
FROM DE_DATABASE.DE_PROJECT.flatten_demo d,
     LATERAL FLATTEN(
         input => d.health_metrics:menu_item_health_metrics
     ) f
GROUP BY d.menu_name;


SELECT
    *,
    value:metric::string as metric
FROM DE_DATABASE.DE_PROJECT.flatten_demo d,
     LATERAL FLATTEN(
         input => d.health_metrics:menu_item_health_metrics
     ) ;

SELECT
    *,
    health_metrics['menu_item_health_metrics'] AS menu_item_health_metrics,
    health_metrics['menu_item_health_metrics'][0] AS metric
FROM DE_DATABASE.DE_PROJECT.flatten_demo;



-- -----------------------------------------------------
-- 7. OPTIONAL CLEANUP
-- -----------------------------------------------------
-- DROP TABLE IF EXISTS DE_DATABASE.DE_PROJECT.flatten_demo;





