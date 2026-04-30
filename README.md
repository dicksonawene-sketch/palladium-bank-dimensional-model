# Palladium Bank — Dimensional Data Model

## Project Overview
This project designs a star schema dimensional model for Palladium Bank,
which operates across Lagos, Abuja, Kano, Port Harcourt, and Ibadan.
The bank has 18 months of raw transaction data with no dimensional model.
Reports currently run directly on transaction logs making them slow and inconsistent.

## Business Objectives
- Customer Behaviour Analysis (by tier, branch, tenure, time)
- Product & Channel Performance (volume, value, trends)
- Branch-Level Reporting (monitor performance & engagement)
- Churn Signals (recency & frequency tracking)
- Time-Based Analysis (MoM, QoQ, YoY comparisons)

## Star Schema Design
[palladium_bank_star_schema](

### Fact Table
- **fact_transactions** — one row per transaction (grain)
  - txn_key, txn_id (degenerate dimension), amount, balance_after
  - Foreign keys: date_key, customer_key, branch_key, product_key, channel_key

### Dimension Tables
| Table | Description | SCD Type |
|---|---|---|
| dim_date | Time dimension — year, quarter, month, day | N/A |
| dim_customer | Customer details — tier, name | Type 2 |
| dim_branch | Branch details — name, state, city | Type 2 |
| dim_product | Product details — name, type | Type 1 |
| dim_channel | Channel details — name, type, txn_type | Type 1 |

### Aggregation Tables
| Table | Purpose |
|---|---|
| agg_monthly_branch_revenue | Pre-calculated monthly totals by branch |
| agg_customer_activity | Recency & frequency tracking for churn detection |

## Slowly Changing Dimensions
- **dim_customer — SCD Type 2:** Customer tier can change over time
  (e.g. Standard → Gold). Full history preserved using effective_date,
  expiry_date, and is_current columns.
- **dim_branch — SCD Type 2:** Branch name or state can change over time.
  Transactions always linked to the branch as it existed at time of transaction.
- **dim_product — SCD Type 1:** Products rarely change. Old value overwritten.
- **dim_channel — SCD Type 1:** Channels rarely change. Old value overwritten.

## ETL Strategy

### Initial Load Order
1. dim_date
2. dim_customer
3. dim_branch
4. dim_product
5. dim_channel
6. fact_transactions (loaded last — depends on all dimensions)

### Required Transformations
- Date format standardisation (text → DATE)
- Surrogate key generation (ROW_NUMBER)
- Tier value casing (INITCAP)
- Channel value casing (UPPER)
- Date attribute derivation (year, quarter, month, day, is_weekend)
- Channel type derivation (Physical vs Digital)

### Incremental Loads
- New records detected by checking Txn_ID against fact_transactions
- SCD Type 2 updates handled by closing old record and inserting new one
- Duplicate facts prevented using NOT EXISTS check

### Data Quality Checks
| # | Check | Action |
|---|---|---|
| 1 | Null amounts | Reject and log |
| 2 | Invalid future dates | Flag and quarantine |
| 3 | Orphan transactions | Reject |
| 4 | Duplicate Txn_IDs | Keep one, discard rest |

## Hierarchies
- **Time:** Year → Quarter → Month → Day
- **Location:** State → Branch
- **Product:** Product_Type → Product_Name

## Performance & Scalability
- **Partitioning:** fact_transactions partitioned by txn_date (range, monthly)
- **Indexing:** Indexes on customer_key, branch_key, date_key, product_key
- **Aggregation:** agg_monthly_branch_revenue for fast dashboard loading

## Files
| File | Description |
|---|---|
| palladium_bank_schema.sql | Full star schema SQL file |
| README.md | Project documentation |

## Tools Used
- SQL (PostgreSQL syntax)
- VS Code

## Author
Awene Dickson Busayo — April 2026
