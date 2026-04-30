
-- PALLADIUM BANK STAR SCHEMA
-- Designed by: Awene Dickson Busayo
-- Date: April 2026
-- Description: Dimensional model for retail banking analytics



-- 1. DATE DIMENSION

CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    day_of_week     VARCHAR(10),
    day_number      INT,
    month_number    INT,
    month_name      VARCHAR(10),
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN
);


-- 2. CUSTOMER DIMENSION

CREATE TABLE dim_customer (
    customer_key    INT PRIMARY KEY,
    customer_id     VARCHAR(10) NOT NULL,
    customer_name   VARCHAR(100),
    tier            VARCHAR(20),
    effective_date  DATE,
    expiry_date     DATE
    is_current      BOOLEAN
);



-- 3. BRANCH DIMENSION

CREATE TABLE dim_branch (
    branch_key      INT PRIMARY KEY,
    branch_id       VARCHAR(10) NOT NULL,
    branch_name     VARCHAR(100),
    state           VARCHAR(50),
    city            VARCHAR(50),
    effective_date  DATE,
    expiry_date     DATE,
    is_current      BOOLEAN
);



-- 4. PRODUCT DIMENSION

CREATE TABLE dim_product (
    product_key     INT PRIMARY KEY,
    product_id      VARCHAR(10) NOT NULL,
    product_name    VARCHAR(100),
    product_type    VARCHAR(50),
    is_active       BOOLEAN
);



-- 5. CHANNEL DIMENSION

CREATE TABLE dim_channel (
    channel_key     INT PRIMARY KEY,
    channel_name    VARCHAR(50) NOT NULL,
    channel_type    VARCHAR(50),
    txn_type        VARCHAR(50)
);


-- 6. FACT TABLE

CREATE TABLE fact_transactions (
    txn_key         INT PRIMARY KEY,
    txn_id          VARCHAR(20) NOT NULL,
    date_key        INT REFERENCES dim_date(date_key),
    customer_key    INT REFERENCES dim_customer(customer_key),
    branch_key      INT REFERENCES dim_branch(branch_key),
    product_key     INT REFERENCES dim_product(product_key),
    channel_key     INT REFERENCES dim_channel(channel_key),
    amount          DECIMAL(15,2),
    balance_after   DECIMAL(15,2)
);



-- 7. AGGREGATION TABLE

CREATE TABLE agg_monthly_branch_revenue (
    year            INT,
    month_number    INT,
    branch_key      INT,
    branch_name     VARCHAR(100),
    state           VARCHAR(50),
    total_amount    DECIMAL(15,2),
    txn_count       INT,
    avg_amount      DECIMAL(15,2)
);



-- 8. INDEXES

CREATE INDEX idx_fact_customer ON fact_transactions(customer_key);
CREATE INDEX idx_fact_branch   ON fact_transactions(branch_key);
CREATE INDEX idx_fact_date     ON fact_transactions(date_key);
CREATE INDEX idx_fact_product  ON fact_transactions(product_key);


-- 9. PARTITIONING STRATEGY
-- fact_transactions is partitioned by txn_date
-- Each partition covers one calendar month
-- Reduces query time for time-based reports


CREATE TABLE fact_transactions_2024_01
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE fact_transactions_2024_02
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE fact_transactions_2024_03
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

CREATE TABLE fact_transactions_2024_04
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

CREATE TABLE fact_transactions_2024_05
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');

CREATE TABLE fact_transactions_2024_06
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');

CREATE TABLE fact_transactions_2024_07
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');

CREATE TABLE fact_transactions_2024_08
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

CREATE TABLE fact_transactions_2024_09
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE fact_transactions_2024_10
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE fact_transactions_2024_11
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE fact_transactions_2024_12
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE fact_transactions_2025_01
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE fact_transactions_2025_02
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE fact_transactions_2025_03
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE fact_transactions_2025_04
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

CREATE TABLE fact_transactions_2025_05
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

CREATE TABLE fact_transactions_2025_06
    PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');


   -- 10. INCREMENTAL LOAD STRATEGY
-- Run daily after initial historical load

-- Step 1: Insert new customers (if not already in dim_customer)
INSERT INTO dim_customer (
    customer_id, customer_name, tier, 
    effective_date, expiry_date, is_current
)
SELECT DISTINCT
    t.customer_id,
    t.customer_name,
    t.tier,
    CURRENT_DATE,
    NULL,
    TRUE
FROM staging_transactions t
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer c
    WHERE c.customer_id = t.customer_id
    AND c.is_current = TRUE
);

-- Step 2: Handle SCD Type 2 for customer tier changes
-- Close old record
UPDATE dim_customer
SET is_current = FALSE,
    expiry_date = CURRENT_DATE
WHERE customer_id IN (
    SELECT t.customer_id
    FROM staging_transactions t
    JOIN dim_customer c ON t.customer_id = c.customer_id
    WHERE c.is_current = TRUE
    AND c.tier != t.tier
);

-- Insert new record with updated tier
INSERT INTO dim_customer (
    customer_id, customer_name, tier,
    effective_date, expiry_date, is_current
)
SELECT DISTINCT
    t.customer_id,
    t.customer_name,
    t.tier,
    CURRENT_DATE,
    NULL,
    TRUE
FROM staging_transactions t
JOIN dim_customer c ON t.customer_id = c.customer_id
WHERE c.is_current = FALSE
AND c.expiry_date = CURRENT_DATE;

-- Step 3: Insert new transactions into fact table
-- Prevents duplicates using NOT EXISTS check
INSERT INTO fact_transactions (
    txn_id, date_key, customer_key,
    branch_key, product_key, channel_key,
    amount, balance_after
)
SELECT
    t.txn_id,
    d.date_key,
    c.customer_key,
    b.branch_key,
    p.product_key,
    ch.channel_key,
    t.amount,
    t.balance_after
FROM staging_transactions t
JOIN dim_date d       ON d.full_date = CAST(t.txn_date AS DATE)
JOIN dim_customer c   ON c.customer_id = t.customer_id AND c.is_current = TRUE
JOIN dim_branch b     ON b.branch_id = t.branch_id AND b.is_current = TRUE
JOIN dim_product p    ON p.product_id = t.product_id
JOIN dim_channel ch   ON ch.channel_name = t.channel
WHERE NOT EXISTS (
    SELECT 1 FROM fact_transactions f
    WHERE f.txn_id = t.txn_id
);


-- 11. DATA QUALITY CHECKS
-- Run before every load cycle

-- Check 1: Detect null amounts
-- Action: Reject and log
SELECT txn_id, 'NULL AMOUNT' AS issue
FROM staging_transactions
WHERE amount IS NULL;

-- Check 2: Detect invalid future dates
-- Action: Flag and quarantine
SELECT txn_id, txn_date, 'FUTURE DATE' AS issue
FROM staging_transactions
WHERE CAST(txn_date AS DATE) > CURRENT_DATE;

-- Check 3: Detect orphan transactions
-- Customer_ID not found in dim_customer
-- Action: Reject to maintain referential integrity
SELECT txn_id, customer_id, 'ORPHAN CUSTOMER' AS issue
FROM staging_transactions
WHERE customer_id NOT IN (
    SELECT customer_id FROM dim_customer
    WHERE is_current = TRUE
);

-- Check 4: Detect duplicate Txn_IDs
-- Action: Keep one, discard the rest
SELECT txn_id, COUNT(*) AS duplicate_count
FROM staging_transactions
GROUP BY txn_id
HAVING COUNT(*) > 1;


-- 12. CHURN SIGNAL TRACKING
-- Tracks recency and frequency per customer
-- Helps identify high-value customers reducing activity


CREATE TABLE agg_customer_activity (
    customer_key        INT REFERENCES dim_customer(customer_key),
    last_txn_date       DATE,          -- recency: when did they last transact?
    txn_count_30days    INT,           -- frequency: how many times in last 30 days?
    txn_count_90days    INT,           -- frequency: how many times in last 90 days?
    total_amount_30days DECIMAL(15,2), -- value in last 30 days
    is_churned          BOOLEAN        -- TRUE if no transaction in last 90 days
);



-- 13. REQUIRED TRANSFORMATIONS (INITIAL LOAD)
-- Applied before loading raw data into dimension/fact tables

-- Transformation 1: Standardize date format
-- Convert raw txn_date from text to proper DATE format
SELECT 
    txn_id,
    CAST(txn_date AS DATE) AS txn_date_clean
FROM staging_transactions;

-- Transformation 2: Generate surrogate keys
-- Assign integer surrogate keys to each dimension record
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    customer_name,
    tier
FROM staging_transactions;

-- Transformation 3: Standardize tier values
-- Ensure consistent casing e.g. 'platinum' → 'Platinum'
UPDATE staging_transactions
SET tier = INITCAP(LOWER(tier));

-- Transformation 4: Standardize channel values
-- Ensure consistent casing e.g. 'pos' → 'POS'
UPDATE staging_transactions
SET channel = UPPER(channel)
WHERE channel IN ('pos', 'atm', 'ussd');

-- Transformation 5: Derive date dimension attributes
-- Break txn_date into day, month, quarter, year
SELECT
    CAST(txn_date AS DATE)                    AS full_date,
    EXTRACT(DOW FROM txn_date)                AS day_of_week,
    EXTRACT(DAY FROM txn_date)                AS day_number,
    EXTRACT(MONTH FROM txn_date)              AS month_number,
    TO_CHAR(txn_date, 'Month')                AS month_name,
    EXTRACT(QUARTER FROM txn_date)            AS quarter,
    EXTRACT(YEAR FROM txn_date)               AS year,
    CASE WHEN EXTRACT(DOW FROM txn_date) 
         IN (0,6) THEN TRUE ELSE FALSE END    AS is_weekend
FROM staging_transactions;

-- Transformation 6: Derive channel_type from channel name
-- Classify each channel as Physical or Digital
SELECT
    channel,
    CASE 
        WHEN channel IN ('POS', 'ATM', 'Branch') 
             THEN 'Physical'
        WHEN channel IN ('Mobile App', 'Internet Banking', 'USSD') 
             THEN 'Digital'
        ELSE 'Unknown'
    END AS channel_type
FROM staging_transactions;