# 04 - SQL Mastery

## Table of Contents
1. [Core SQL](#1-core-sql)
2. [Window Functions](#2-window-functions)
3. [CTEs and Subqueries](#3-ctes-and-subqueries)
4. [Advanced SQL Techniques](#4-advanced-sql-techniques)
5. [Real-World Scenarios](#5-real-world-scenarios)
6. [Banking Examples](#6-banking-examples)
7. [E-Commerce Examples](#7-e-commerce-examples)
8. [Hands-On Exercises](#8-hands-on-exercises)
9. [Interview Questions](#9-interview-questions)

---

## 1. Core SQL

### DDL (Data Definition Language)

```sql
-- Create table
CREATE TABLE dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL UNIQUE,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    email           VARCHAR(100),
    segment         VARCHAR(20) DEFAULT 'Standard',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alter table
ALTER TABLE dim_customer ADD COLUMN phone VARCHAR(20);
ALTER TABLE dim_customer ALTER COLUMN email SET NOT NULL;
ALTER TABLE dim_customer DROP COLUMN legacy_id;

-- Create index
CREATE INDEX idx_customer_email ON dim_customer(email);
CREATE INDEX idx_customer_segment ON dim_customer(segment, customer_id);
`

### DML (Data Manipulation Language)

`sql
-- Insert
INSERT INTO dim_customer (customer_id, first_name, last_name, email, segment)
VALUES 
    ('C001', 'John', 'Smith', 'john@email.com', 'Premium'),
    ('C002', 'Jane', 'Doe', 'jane@email.com', 'Standard'),
    ('C003', 'Bob', 'Wilson', 'bob@email.com', 'Premium');

-- Insert from another table
INSERT INTO dim_customer_archive
SELECT * FROM dim_customer WHERE created_at < '2023-01-01';

-- Update
UPDATE dim_customer 
SET segment = 'Premium', updated_at = CURRENT_TIMESTAMP
WHERE customer_id = 'C002';

-- Delete
DELETE FROM dim_customer WHERE customer_id = 'C003';

-- Merge (UPSERT)
MERGE INTO dim_customer tgt
USING staging_customer src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND tgt.hash_diff <> src.hash_diff THEN
    UPDATE SET 
        first_name = src.first_name,
        last_name = src.last_name,
        email = src.email,
        segment = src.segment,
        updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (customer_id, first_name, last_name, email, segment)
    VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.segment);
`

### Joins

`sql
-- INNER JOIN: Only matching rows
SELECT o.order_id, c.customer_name, p.product_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id;

-- LEFT JOIN: All from left, matching from right
SELECT c.customer_name, COALESCE(SUM(o.amount), 0) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name;

-- SELF JOIN: Table joined with itself
SELECT 
    e.employee_name as employee,
    m.employee_name as manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;

-- CROSS JOIN: Cartesian product
SELECT d.day_name, h.hour_value
FROM days d
CROSS JOIN hours h;

-- NATURAL JOIN: Automatic join on common columns
SELECT * FROM orders NATURAL JOIN customers;

-- LATERAL JOIN: Correlated subquery in FROM
SELECT c.customer_name, recent_orders.*
FROM customers c
JOIN LATERAL (
    SELECT order_id, amount
    FROM orders
    WHERE customer_id = c.customer_id
    ORDER BY order_date DESC
    LIMIT 3
) recent_orders ON TRUE;
`

### Aggregate Functions

`sql
SELECT 
    p.category,
    d.quarter,
    -- Basic aggregates
    COUNT(*) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(oi.quantity * oi.unit_price) as total_revenue,
    AVG(oi.quantity * oi.unit_price) as avg_order_value,
    MIN(oi.unit_price) as min_price,
    MAX(oi.unit_price) as max_price,
    -- Statistical aggregates
    STDDEV(oi.unit_price) as price_stddev,
    VARIANCE(oi.quantity) as quantity_variance
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN dim_date d ON o.order_date = d.full_date
GROUP BY p.category, d.quarter
HAVING SUM(oi.quantity * oi.unit_price) > 10000
ORDER BY total_revenue DESC;
`

---

## 2. Window Functions

Window functions perform calculations across a set of rows related to the current row, without collapsing them.

### Syntax

`sql
function_name() OVER (
    [PARTITION BY partition_column]
    [ORDER BY sort_column [ASC|DESC]]
    [ROWS|RANGE BETWEEN frame_start AND frame_end]
)
`

### Ranking Functions

`sql
-- ROW_NUMBER: Unique sequential number
SELECT 
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (ORDER BY amount DESC) as rank
FROM orders;

-- RANK: Same rank for ties, gaps in sequence
SELECT 
    customer_id,
    total_spent,
    RANK() OVER (ORDER BY total_spent DESC) as spending_rank
FROM (
    SELECT customer_id, SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
) customer_totals;

-- DENSE_RANK: Same rank for ties, no gaps
SELECT 
    customer_id,
    total_spent,
    DENSE_RANK() OVER (ORDER BY total_spent DESC) as spending_dense_rank
FROM (
    SELECT customer_id, SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
) customer_totals;

-- NTILE: Divide into N equal groups
SELECT 
    customer_id,
    total_spent,
    NTILE(4) OVER (ORDER BY total_spent DESC) as quartile
FROM (
    SELECT customer_id, SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
) customer_totals;

-- PERCENT_RANK: Relative rank as percentage
SELECT 
    customer_id,
    total_spent,
    PERCENT_RANK() OVER (ORDER BY total_spent DESC) as percentile
FROM customer_totals;

-- CUME_DIST: Cumulative distribution
SELECT 
    customer_id,
    total_spent,
    CUME_DIST() OVER (ORDER BY total_spent DESC) as cumulative_dist
FROM customer_totals;
`

### Navigation Functions

`sql
-- LAG: Access previous row
SELECT 
    month_date,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY month_date) as prev_month_revenue,
    revenue - LAG(revenue, 1) OVER (ORDER BY month_date) as mom_change,
    ROUND(
        (revenue - LAG(revenue, 1) OVER (ORDER BY month_date)) * 100.0 / 
        LAG(revenue, 1) OVER (ORDER BY month_date), 2
    ) as mom_pct_change
FROM monthly_revenue;

-- LEAD: Access next row
SELECT 
    month_date,
    revenue,
    LEAD(revenue, 1) OVER (ORDER BY month_date) as next_month_revenue
FROM monthly_revenue;

-- FIRST_VALUE: First row in window
SELECT 
    customer_id,
    order_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as first_order_amount
FROM orders;

-- LAST_VALUE: Last row in window
SELECT 
    customer_id,
    order_date,
    amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as last_order_amount
FROM orders;

-- NTH_VALUE: Nth row in window
SELECT 
    customer_id,
    order_date,
    amount,
    NTH_VALUE(amount, 2) OVER (
        PARTITION BY customer_id 
        ORDER BY amount DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as second_highest_order
FROM orders;
`

### Aggregate Window Functions

`sql
-- Running total
SELECT 
    order_date,
    amount,
    SUM(amount) OVER (ORDER BY order_date) as running_total
FROM orders;

-- Moving average (3-day)
SELECT 
    order_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3day
FROM daily_revenue;

-- Cumulative distribution
SELECT 
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as cumulative_spend,
    amount * 100.0 / SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as pct_of_cumulative
FROM orders;
`

### Frame Clauses

`sql
-- ROWS: Physical rows
SELECT 
    order_date,
    amount,
    AVG(amount) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) as five_day_moving_avg
FROM orders;

-- RANGE: Logical range (based on ORDER BY value)
SELECT 
    order_date,
    amount,
    AVG(amount) OVER (
        ORDER BY order_date 
        RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
    ) as seven_day_rolling_avg
FROM orders;

-- UNBOUNDED: From beginning/end
SELECT 
    order_date,
    amount,
    SUM(amount) OVER (
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total,
    AVG(amount) OVER (
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as overall_average
FROM orders;
`

### Real-World Window Function Examples

`sql
-- Customer cohort analysis
SELECT 
    customer_id,
    first_order_date,
    order_date,
    DATE_TRUNC('month', order_date) - DATE_TRUNC('month', first_order_date) as months_since_first,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
    ) as lifetime_value
FROM (
    SELECT 
        *,
        MIN(order_date) OVER (PARTITION BY customer_id) as first_order_date
    FROM orders
) customer_orders;

-- Identify top customers per category
SELECT *
FROM (
    SELECT 
        c.customer_name,
        p.category,
        SUM(oi.amount) as total_spent,
        ROW_NUMBER() OVER (
            PARTITION BY p.category 
            ORDER BY SUM(oi.amount) DESC
        ) as rank_in_category
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY c.customer_name, p.category
) ranked
WHERE rank_in_category <= 3;

-- Calculate gaps between consecutive events
SELECT 
    customer_id,
    visit_date,
    visit_date - LAG(visit_date) OVER (
        PARTITION BY customer_id 
        ORDER BY visit_date
    ) as days_between_visits
FROM customer_visits;
`

---

## 3. CTEs and Subqueries

### Common Table Expressions (CTEs)

`sql
-- Basic CTE
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        SUM(amount) as total_sales
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
),
monthly_growth AS (
    SELECT 
        month,
        total_sales,
        LAG(total_sales) OVER (ORDER BY month) as prev_month_sales,
        ROUND(
            (total_sales - LAG(total_sales) OVER (ORDER BY month)) * 100.0 / 
            LAG(total_sales) OVER (ORDER BY month), 2
        ) as growth_pct
    FROM monthly_sales
)
SELECT * FROM monthly_growth
WHERE growth_pct > 10
ORDER BY growth_pct DESC;

-- Multiple CTEs
WITH customer_orders AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
),
customer_segments AS (
    SELECT 
        *,
        CASE 
            WHEN total_spent > 10000 THEN 'Premium'
            WHEN total_spent > 5000 THEN 'Gold'
            WHEN total_spent > 1000 THEN 'Silver'
            ELSE 'Bronze'
        END as segment
    FROM customer_orders
)
SELECT 
    segment,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_spend
FROM customer_segments
GROUP BY segment;
`

### Recursive CTEs

`sql
-- Employee hierarchy traversal
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: top-level managers
    SELECT 
        employee_id,
        employee_name,
        manager_id,
        1 as level,
        employee_name as path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: subordinates
    SELECT 
        e.employee_id,
        e.employee_name,
        e.manager_id,
        h.level + 1,
        h.path || ' -> ' || e.employee_name
    FROM employees e
    JOIN employee_hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM employee_hierarchy ORDER BY path;

-- Date dimension generation
WITH RECURSIVE date_series AS (
    SELECT DATE '2024-01-01' as date_value
    
    UNION ALL
    
    SELECT date_value + INTERVAL '1 day'
    FROM date_series
    WHERE date_value < DATE '2024-12-31'
)
SELECT 
    date_value,
    EXTRACT(DOW FROM date_value) as day_of_week,
    EXTRACT(MONTH FROM date_value) as month,
    EXTRACT(QUARTER FROM date_value) as quarter,
    EXTRACT(YEAR FROM date_value) as year
FROM date_series;

-- Bill of Materials explosion
WITH RECURSIVE bom AS (
    SELECT 
        parent_item,
        child_item,
        quantity,
        1 as level
    FROM bill_of_materials
    WHERE parent_item = 'FINISHED_PRODUCT'
    
    UNION ALL
    
    SELECT 
        b.parent_item,
        b.child_item,
        b.quantity,
        bom.level + 1
    FROM bill_of_materials b
    JOIN bom ON b.parent_item = bom.child_item
)
SELECT * FROM bom;
`

### Subqueries

`sql
-- Correlated subquery
SELECT 
    c.customer_name,
    o.order_date,
    o.amount,
    (SELECT AVG(amount) FROM orders WHERE customer_id = c.customer_id) as avg_customer_order,
    o.amount - (SELECT AVG(amount) FROM orders WHERE customer_id = c.customer_id) as diff_from_avg
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- EXISTS subquery
SELECT c.customer_name
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.customer_id 
    AND o.order_date >= '2024-01-01'
);

-- IN subquery
SELECT *
FROM products
WHERE product_id IN (
    SELECT DISTINCT product_id
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_date >= '2024-01-01'
);

-- Derived table (inline subquery)
SELECT 
    category_stats.category,
    category_stats.total_revenue,
    category_stats.total_revenue / grand_total.total * 100 as pct_of_total
FROM (
    SELECT p.category, SUM(oi.amount) as total_revenue
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.category
) category_stats
CROSS JOIN (
    SELECT SUM(amount) as total FROM order_items
) grand_total;
`

---

## 4. Advanced SQL Techniques

### Pivot and Unpivot

`sql
-- Pivot: Rows to columns
SELECT 
    customer_id,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 1 THEN amount ELSE 0 END) as jan,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 2 THEN amount ELSE 0 END) as feb,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 3 THEN amount ELSE 0 END) as mar
FROM orders
WHERE EXTRACT(YEAR FROM order_date) = 2024
GROUP BY customer_id;

-- Using PIVOT (if supported)
SELECT * FROM (
    SELECT customer_id, EXTRACT(MONTH FROM order_date) as month, amount
    FROM orders
    WHERE EXTRACT(YEAR FROM order_date) = 2024
)
PIVOT (
    SUM(amount)
    FOR month IN (1 as jan, 2 as feb, 3 as mar)
);

-- Unpivot: Columns to rows
SELECT customer_id, month_name, amount
FROM customer_monthly_sales
UNPIVOT (
    amount FOR month_name IN (jan, feb, mar, apr, may, jun)
);
`

### JSON Operations

`sql
-- JSON extraction
SELECT 
    order_id,
    JSON_EXTRACT_PATH_TEXT(metadata, 'payment_method') as payment_method,
    JSON_EXTRACT_PATH_TEXT(metadata, 'shipping', 'carrier') as carrier
FROM orders
WHERE JSON_EXTRACT_PATH_TEXT(metadata, 'payment_method') = 'credit_card';

-- PostgreSQL JSONB operators
SELECT 
    order_id,
    metadata->>'payment_method' as payment_method,
    metadata->'shipping'->>'carrier' as carrier
FROM orders
WHERE metadata @> '{"payment_method": "credit_card"}';

-- Array operations
SELECT 
    customer_id,
    ARRAY_LENGTH(interests, 1) as interest_count,
    interests @> ARRAY['electronics'] as interested_in_electronics
FROM customers;
`

### String and Date Functions

`sql
-- String manipulation
SELECT 
    UPPER(first_name) || ' ' || UPPER(last_name) as full_name_upper,
    INITCAP(first_name) || ' ' || INITCAP(last_name) as full_name_proper,
    SUBSTRING(email, 1, POSITION('@' IN email) - 1) as username,
    LENGTH(phone) as phone_length,
    REPLACE(phone, '-', '') as phone_clean,
    CONCAT_WS(', ', address, city, state, zip) as full_address
FROM customers;

-- Date manipulation
SELECT 
    order_date,
    DATE_TRUNC('month', order_date) as month_start,
    DATE_TRUNC('quarter', order_date) as quarter_start,
    order_date + INTERVAL '30 days' as due_date,
    AGE(CURRENT_DATE, order_date) as age,
    EXTRACT(DOW FROM order_date) as day_of_week,
    TO_CHAR(order_date, 'YYYY-MM-DD') as formatted_date,
    TO_CHAR(order_date, 'Day') as day_name,
    DATE_PART('epoch', order_date) as epoch_seconds
FROM orders;

-- Conditional logic
SELECT 
    customer_id,
    CASE 
        WHEN total_spent > 10000 THEN 'Platinum'
        WHEN total_spent > 5000 THEN 'Gold'
        WHEN total_spent > 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as tier,
    COALESCE(nickname, first_name, 'Customer') as display_name,
    NULLIF(phone, '') as phone_null_if_empty
FROM customer_stats;
```

### Modern SQL Tools

| Tool | Type | Best For | Pricing |
|------|------|----------|--------|
| **DBeaver** | Desktop IDE | Database management, visual queries | Free / Community |
| **DataGrip** | Desktop IDE | Professional SQL development | Paid |
| **SQLPad** | Web-based | Team SQL editor | Open source |
| **dbt** | CLI/Framework | SQL transformations, testing | Free / Cloud |
| **Metabase** | BI Tool | Ad-hoc queries, dashboards | Free / Paid |
| **Explain Visually** | Web tool | Query plan visualization | Free |

---

## 5. Real-World Scenarios

### Scenario 1: E-Commerce Analytics Query

```sql
-- Monthly revenue with MoM growth and moving average
WITH monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', o.order_date) as month,
        COUNT(DISTINCT o.order_id) as orders,
        COUNT(DISTINCT o.customer_id) as customers,
        SUM(oi.quantity * oi.unit_price) as revenue,
        SUM(oi.quantity * oi.unit_price - oi.cost) as profit
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'completed'
    GROUP BY DATE_TRUNC('month', o.order_date)
),
with_calculations AS (
    SELECT 
        *,
        LAG(revenue) OVER (ORDER BY month) as prev_month_revenue,
        AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3m,
        revenue * 100.0 / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) - 100 as mom_growth_pct,
        profit * 100.0 / revenue as profit_margin_pct
    FROM monthly_metrics
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') as month,
    orders,
    customers,
    ROUND(revenue, 2) as revenue,
    ROUND(prev_month_revenue, 2) as prev_month,
    ROUND(mom_growth_pct, 2) as mom_growth_pct,
    ROUND(moving_avg_3m, 2) as moving_avg_3m,
    ROUND(profit_margin_pct, 2) as margin_pct
FROM with_calculations
ORDER BY month;
```

### Scenario 2: Banking Transaction Analysis

```sql
-- Customer transaction patterns with risk scoring
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as txn_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        STDDEV(amount) as stddev_amount,
        MAX(amount) as max_amount,
        COUNT(DISTINCT DATE_TRUNC('day', txn_date)) as active_days
    FROM transactions
    WHERE txn_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY customer_id
),
risk_scored AS (
    SELECT 
        *,
        CASE 
            WHEN avg_amount > 10000 THEN 3
            WHEN avg_amount > 5000 THEN 2
            ELSE 1
        END as amount_risk,
        CASE 
            WHEN txn_count > 100 THEN 3
            WHEN txn_count > 50 THEN 2
            ELSE 1
        END as frequency_risk,
        CASE 
            WHEN stddev_amount > avg_amount * 2 THEN 3
            WHEN stddev_amount > avg_amount THEN 2
            ELSE 1
        END as consistency_risk
    FROM customer_stats
)
SELECT 
    customer_id,
    txn_count,
    total_amount,
    ROUND(avg_amount, 2) as avg_amount,
    amount_risk + frequency_risk + consistency_risk as total_risk_score,
    CASE 
        WHEN amount_risk + frequency_risk + consistency_risk >= 7 THEN 'HIGH'
        WHEN amount_risk + frequency_risk + consistency_risk >= 4 THEN 'MEDIUM'
        ELSE 'LOW'
    END as risk_category
FROM risk_scored
ORDER BY total_risk_score DESC;
```

---

## 6. Banking Examples

### Example 1: Daily Balance Reconciliation

```sql
-- Compare expected vs actual balances
WITH expected_balances AS (
    SELECT 
        account_id,
        opening_balance + SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE -amount END) as expected_balance
    FROM daily_transactions
    GROUP BY account_id, opening_balance
),
actual_balances AS (
    SELECT account_id, balance as actual_balance
    FROM account_balances
    WHERE balance_date = CURRENT_DATE
)
SELECT 
    e.account_id,
    e.expected_balance,
    a.actual_balance,
    e.expected_balance - a.actual_balance as variance,
    CASE 
        WHEN ABS(e.expected_balance - a.actual_balance) > 0.01 THEN 'RECONCILIATION FAILED'
        ELSE 'RECONCILED'
    END as status
FROM expected_balances e
JOIN actual_balances a ON e.account_id = a.account_id
WHERE ABS(e.expected_balance - a.actual_balance) > 0.01;
```

### Example 2: NPA (Non-Performing Asset) Classification

```sql
-- Identify loans that have become NPA (90+ DPD)
WITH loan_dpd AS (
    SELECT 
        l.loan_id,
        l.customer_id,
        l.outstanding_amount,
        l.last_payment_date,
        CURRENT_DATE - l.last_payment_date as days_past_due,
        CASE 
            WHEN CURRENT_DATE - l.last_payment_date > 180 THEN 'Loss'
            WHEN CURRENT_DATE - l.last_payment_date > 90 THEN 'Doubtful'
            WHEN CURRENT_DATE - l.last_payment_date > 60 THEN 'Substandard'
            WHEN CURRENT_DATE - l.last_payment_date > 30 THEN 'Special Mention'
            ELSE 'Standard'
        END as classification
    FROM loans l
    WHERE l.status = 'Active'
)
SELECT 
    classification,
    COUNT(*) as loan_count,
    SUM(outstanding_amount) as total_outstanding
FROM loan_dpd
GROUP BY classification
ORDER BY total_outstanding DESC;
```

---

## 7. E-Commerce Examples

### Example 1: Customer Lifetime Value (CLV)

```sql
-- Calculate CLV using RFM analysis
WITH rfm AS (
    SELECT 
        customer_id,
        DATEDIFF(day, MAX(order_date), CURRENT_DATE) as recency,
        COUNT(DISTINCT order_id) as frequency,
        SUM(amount) as monetary
    FROM orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency) as f_score,
        NTILE(5) OVER (ORDER BY monetary) as m_score
    FROM rfm
)
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score as rfm_total,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
        ELSE 'Potential'
    END as customer_segment
FROM rfm_scored
ORDER BY rfm_total DESC;
```

### Example 2: Product Affinity Analysis

```sql
-- Find products frequently bought together
WITH order_products AS (
    SELECT 
        order_id,
        ARRAY_AGG(product_id ORDER BY product_id) as products
    FROM order_items
    GROUP BY order_id
    HAVING COUNT(*) > 1
),
product_pairs AS (
    SELECT 
        p1.product_id as product_a,
        p2.product_id as product_b,
        COUNT(*) as pair_count
    FROM order_products op
    JOIN UNNEST(op.products) WITH ORDINALITY AS p1(product_id, idx1)
    JOIN UNNEST(op.products) WITH ORDINALITY AS p2(product_id, idx2)
    WHERE idx1 < idx2
    GROUP BY p1.product_id, p2.product_id
)
SELECT 
    pp.product_a,
    pa.product_name as product_a_name,
    pp.product_b,
    pb.product_name as product_b_name,
    pp.pair_count,
    pp.pair_count * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM order_items) as support_pct
FROM product_pairs pp
JOIN products pa ON pp.product_a = pa.product_id
JOIN products pb ON pp.product_b = pb.product_id
WHERE pp.pair_count >= 10
ORDER BY pp.pair_count DESC
LIMIT 20;
```

---

## 8. Hands-On Exercises

### Exercise 1: Window Functions Practice
```sql
-- Task: Rank customers by total spending and find top 3 per segment

-- Create sample data
CREATE TEMPORARY TABLE orders_sample (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2)
);

INSERT INTO orders_sample VALUES
(1, 101, '2024-01-15', 150.00),
(2, 102, '2024-01-16', 250.00),
(3, 101, '2024-02-01', 100.00),
(4, 103, '2024-02-05', 300.00),
(5, 102, '2024-02-10', 200.00),
(6, 104, '2024-02-15', 500.00),
(7, 103, '2024-03-01', 150.00),
(8, 104, '2024-03-05', 400.00);

-- Solution: Rank by total spending
WITH customer_totals AS (
    SELECT 
        customer_id,
        SUM(amount) as total_spent,
        COUNT(*) as order_count
    FROM orders_sample
    GROUP BY customer_id
),
ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY total_spent DESC) as overall_rank,
        DENSE_RANK() OVER (ORDER BY total_spent DESC) as dense_rank,
        NTILE(4) OVER (ORDER BY total_spent DESC) as spending_quartile
    FROM customer_totals
)
SELECT * FROM ranked_customers ORDER BY overall_rank;
```

### Exercise 2: Recursive CTE Challenge
```sql
-- Task: Generate a calendar table for 2024 with all date attributes

WITH RECURSIVE calendar AS (
    SELECT 
        DATE '2024-01-01' as date_value,
        1 as day_number
    
    UNION ALL
    
    SELECT 
        date_value + INTERVAL '1 day',
        day_number + 1
    FROM calendar
    WHERE date_value < DATE '2024-12-31'
)
SELECT 
    date_value,
    day_number,
    EXTRACT(DOW FROM date_value) as day_of_week,
    CASE EXTRACT(DOW FROM date_value)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    EXTRACT(MONTH FROM date_value) as month_number,
    TO_CHAR(date_value, 'Month') as month_name,
    EXTRACT(QUARTER FROM date_value) as quarter,
    EXTRACT(YEAR FROM date_value) as year,
    EXTRACT(DAY FROM date_value) as day_of_month,
    EXTRACT(DOY FROM date_value) as day_of_year,
    EXTRACT(WEEK FROM date_value) as week_number,
    CASE 
        WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN TRUE
        ELSE FALSE
    END as is_weekend
FROM calendar
ORDER BY date_value;
```

### Exercise 3: Complex Aggregation
```sql
-- Task: Calculate running totals and moving averages

-- Create sample daily sales data
CREATE TEMPORARY TABLE daily_sales (
    sale_date DATE,
    revenue DECIMAL(12,2)
);

INSERT INTO daily_sales VALUES
('2024-01-01', 1000), ('2024-01-02', 1200), ('2024-01-03', 900),
('2024-01-04', 1100), ('2024-01-05', 1300), ('2024-01-06', 800),
('2024-01-07', 1400), ('2024-01-08', 1250), ('2024-01-09', 1100),
('2024-01-10', 1350);

-- Solution: Running totals, moving averages, and growth
SELECT 
    sale_date,
    revenue,
    SUM(revenue) OVER (ORDER BY sale_date) as running_total,
    AVG(revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3day,
    LAG(revenue) OVER (ORDER BY sale_date) as prev_day_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY sale_date)) * 100.0 / 
        NULLIF(LAG(revenue) OVER (ORDER BY sale_date), 0), 2
    ) as daily_growth_pct,
    ROUND(
        revenue * 100.0 / SUM(revenue) OVER (ORDER BY sale_date), 2
    ) as pct_of_running_total
FROM daily_sales
ORDER BY sale_date;
```

### Exercise 4: Pivot Table Challenge
```sql
-- Task: Create a monthly sales pivot by product category

-- Create sample data
CREATE TEMPORARY TABLE sales_data (
    order_date DATE,
    category VARCHAR(50),
    amount DECIMAL(10,2)
);

INSERT INTO sales_data VALUES
('2024-01-15', 'Electronics', 500),
('2024-01-20', 'Clothing', 200),
('2024-02-10', 'Electronics', 750),
('2024-02-15', 'Food', 150),
('2024-03-05', 'Electronics', 600),
('2024-03-10', 'Clothing', 300),
('2024-03-15', 'Food', 180);

-- Solution: Manual pivot with CASE
SELECT 
    category,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 1 THEN amount ELSE 0 END) as jan,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 2 THEN amount ELSE 0 END) as feb,
    SUM(CASE WHEN EXTRACT(MONTH FROM order_date) = 3 THEN amount ELSE 0 END) as mar,
    SUM(amount) as total,
    COUNT(*) as transaction_count
FROM sales_data
GROUP BY category
ORDER BY total DESC;
```

### Exercise 5: Advanced Analytics Query
```sql
-- Task: Customer cohort analysis with retention calculation

-- Create sample cohort data
CREATE TEMPORARY TABLE customer_orders (
    customer_id INT,
    first_order_date DATE,
    order_date DATE,
    amount DECIMAL(10,2)
);

INSERT INTO customer_orders VALUES
(1, '2024-01-01', '2024-01-01', 100),
(1, '2024-01-01', '2024-02-15', 150),
(1, '2024-01-01', '2024-03-20', 200),
(2, '2024-01-15', '2024-01-15', 80),
(2, '2024-01-15', '2024-03-10', 120),
(3, '2024-02-01', '2024-02-01', 90),
(4, '2024-02-15', '2024-02-15', 110),
(4, '2024-02-15', '2024-03-05', 130);

-- Solution: Cohort retention analysis
WITH cohort AS (
    SELECT 
        customer_id,
        first_order_date,
        DATE_TRUNC('month', first_order_date) as cohort_month,
        DATE_TRUNC('month', order_date) as order_month,
        AGE(DATE_TRUNC('month', order_date), DATE_TRUNC('month', first_order_date)) as months_since_first
    FROM customer_orders
),
cohort_size AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_id) as customers
    FROM cohort
    GROUP BY cohort_month
),
cohort_activity AS (
    SELECT 
        c.cohort_month,
        c.months_since_first,
        COUNT(DISTINCT c.customer_id) as active_customers
    FROM cohort c
    GROUP BY c.cohort_month, c.months_since_first
)
SELECT 
    ca.cohort_month,
    cs.customers as cohort_size,
    ca.months_since_first,
    ca.active_customers,
    ROUND(ca.active_customers * 100.0 / cs.customers, 2) as retention_pct
FROM cohort_activity ca
JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
ORDER BY ca.cohort_month, ca.months_since_first;
```

### Exercise 6: String Manipulation Challenge
```sql
-- Task: Parse and clean messy data

-- Create sample messy data
CREATE TEMPORARY TABLE messy_customers (
    id INT,
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(50),
    address VARCHAR(200)
);

INSERT INTO messy_customers VALUES
(1, '  john smith  ', 'JOHN@EMAIL.COM', '123-456-7890', '123 Main St, New York, NY 10001'),
(2, 'jane doe', 'jane.doe@gmail.com', '(555) 123-4567', '456 Oak Ave\nSuite 100\nLos Angeles, CA 90001'),
(3, 'BOB WILSON', 'bob_wilson@company.org', '555.123.4567', '789 Pine Rd; Chicago; IL; 60601');

-- Solution: Clean and parse the data
SELECT 
    id,
    -- Clean name
    INITCAP(TRIM(full_name)) as clean_name,
    -- Extract username from email
    LOWER(SUBSTRING(email FROM 1 FOR POSITION('@' IN email) - 1)) as username,
    -- Extract domain
    LOWER(SUBSTRING(email FROM POSITION('@' IN email) + 1)) as domain,
    -- Clean phone (remove all non-digits)
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as clean_phone,
    -- Parse address components
    SPLIT_PART(REPLACE(address, ';', ','), ',', 1) as street,
    SPLIT_PART(REPLACE(address, ';', ','), ',', 2) as city,
    TRIM(SPLIT_PART(REPLACE(address, ';', ','), ',', 3)) as state,
    TRIM(SPLIT_PART(REPLACE(address, ';', ','), ',', 4)) as zip
FROM messy_customers;
```

---

## 9. Interview Questions

### Q1: What is the difference between WHERE and HAVING?

**Answer:** WHERE filters rows before grouping/aggregation. HAVING filters groups after aggregation. WHERE cannot use aggregate functions (SUM, COUNT, etc.), HAVING can. Use WHERE to filter individual records (e.g., WHERE amount > 100), use HAVING to filter aggregated results (e.g., HAVING SUM(amount) > 1000).

### Q2: Explain window functions vs aggregate functions.

**Answer:** Aggregate functions (GROUP BY) collapse rows - you lose individual row details. Window functions calculate across related rows while preserving all original rows. For example, SUM(amount) GROUP BY customer gives one row per customer. SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) gives each order row with a running total. Window functions are essential for ranking, running totals, moving averages, and period-over-period comparisons.

### Q3: When would you use a CTE vs a subquery vs a temp table?

**Answer:** 

**CTE**: Best for readability, recursive queries, and when used once or twice. Better than subqueries for complex logic. 

**Subquery**: Best for simple one-off filters or when CTEs add unnecessary complexity. 

**Temp table**: Best when the result is reused multiple times in the same query, needs indexes, or the dataset is very large (materializes the result). Temp tables persist for the session; CTEs/subqueries are inline.

### Q4: Write a query to find the second highest salary.

**Answer:**
```sql
-- Method 1: Subquery
SELECT MAX(salary) FROM employees WHERE salary < (SELECT MAX(salary) FROM employees);

-- Method 2: LIMIT/OFFSET
SELECT DISTINCT salary FROM employees ORDER BY salary DESC LIMIT 1 OFFSET 1;

-- Method 3: Window function (handles ties)
SELECT salary FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
) ranked WHERE rank = 2;

-- Method 4: CTE
WITH ranked AS (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rank
    FROM employees
)
SELECT salary FROM ranked WHERE rank = 2;
```

### Q5: How do you optimize a slow SQL query?

**Answer:** 

1) Check the execution plan (EXPLAIN/EXPLAIN ANALYZE) to find bottlenecks. 

2) Add indexes on columns used in WHERE, JOIN, ORDER BY. 

3) Avoid SELECT * - only retrieve needed columns. 

4) Filter early with WHERE before JOINs. 

5) Replace correlated subqueries with JOINs or CTEs. 

6) Partition large tables by date. 

7) Use materialized views for complex aggregations. 

8) Consider denormalizing for frequently joined tables. 

9) Update table statistics. 

10) Check for lock contention and connection pooling.

### Q6: What is the difference between UNION and UNION ALL?

**Answer:** 

**UNION** combines results from two queries and removes duplicate rows (performs distinct sort). 

**UNION ALL** combines results without removing duplicates (faster, no overhead). Use UNION when you need unique results. 

Use UNION ALL when duplicates are acceptable or you know results don't overlap. UNION ALL is generally faster and preferred when possible.

### Q7: How do you handle NULL values in SQL?

**Answer:**
- **COALESCE(value1, value2)**: Returns first non-NULL value
- **NULLIF(value1, value2)**: Returns NULL if values are equal
- **IS NULL / IS NOT NULL**: Test for NULL
- **CASE WHEN x IS NULL**: Conditional logic for NULLs
- **Aggregate functions ignore NULLs** (except COUNT(*) which counts all rows)
- **NULL comparisons return UNKNOWN** (not TRUE or FALSE), so use IS NULL

---

## Summary Checklist

### Core SQL
- [ ] Master DDL (CREATE, ALTER, DROP, INDEX)
- [ ] Know DML (INSERT, UPDATE, DELETE, MERGE)
- [ ] Understand all JOIN types (INNER, LEFT, RIGHT, FULL, CROSS, LATERAL)
- [ ] Use aggregate functions with GROUP BY and HAVING

### Window Functions
- [ ] Use ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
- [ ] Apply navigation functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE)
- [ ] Calculate running totals and moving averages
- [ ] Understand frame clauses (ROWS vs RANGE, UNBOUNDED)

### CTEs and Subqueries
- [ ] Write basic and multiple CTEs
- [ ] Implement recursive CTEs for hierarchical data
- [ ] Know when to use CTE vs subquery vs temp table

### Advanced Techniques
- [ ] Pivot and unpivot data
- [ ] Work with JSON and array data
- [ ] Master string and date functions
- [ ] Use conditional logic (CASE, COALESCE, NULLIF)

### Performance
- [ ] Read and interpret EXPLAIN plans
- [ ] Write efficient queries with proper indexing
- [ ] Avoid common anti-patterns (SELECT *, correlated subqueries)

---

*Next Section: [05 - ETL/ELT](../05-ETL-ELT/README.md)*
