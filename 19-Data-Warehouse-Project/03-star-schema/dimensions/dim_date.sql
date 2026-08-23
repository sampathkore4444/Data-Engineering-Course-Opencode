-- =============================================================================
-- DIMENSION TABLE: dim_date
-- =============================================================================
-- Type: Static Dimension (no SCD)
-- Purpose: Calendar dimension for time-based analysis
-- =============================================================================

CREATE TABLE dw.dim_date (
    date_key             INT PRIMARY KEY,  -- YYYYMMDD format
    full_date            DATE NOT NULL,
    day_of_week          INT,  -- 1=Sunday, 7=Saturday
    day_name             VARCHAR(10),
    day_of_month         INT,
    day_of_year          INT,
    is_weekend           BOOLEAN,
    is_business_day      BOOLEAN,
    week_of_year         INT,
    month_number         INT,
    month_name           VARCHAR(10),
    month_abbrev         VARCHAR(3),
    quarter              INT,
    quarter_name         VARCHAR(6),  -- Q1, Q2, Q3, Q4
    year                 INT,
    year_month           VARCHAR(7),  -- YYYY-MM
    year_quarter         VARCHAR(6),  -- YYYY-Q1
    fiscal_year          INT,
    fiscal_quarter       INT,
    is_month_end         BOOLEAN,
    is_quarter_end       BOOLEAN,
    is_year_end          BOOLEAN
);

-- Indexes
CREATE INDEX idx_dim_date_full ON dw.dim_date(full_date);
CREATE INDEX idx_dim_date_year ON dw.dim_date(year);
CREATE INDEX idx_dim_date_month ON dw.dim_date(year, month_number);

-- =============================================================================
-- GENERATE DATE DATA (5 years: 2020-2025)
-- =============================================================================
INSERT INTO dw.dim_date (
    date_key, full_date, day_of_week, day_name, day_of_month, day_of_year,
    is_weekend, is_business_day, week_of_year, month_number, month_name, month_abbrev,
    quarter, quarter_name, year, year_month, year_quarter,
    fiscal_year, fiscal_quarter, is_month_end, is_quarter_end, is_year_end
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS full_date,
    EXTRACT(DOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DAY FROM d)::INT AS day_of_month,
    EXTRACT(DOY FROM d)::INT AS day_of_year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(DOW FROM d) NOT IN (0, 6) THEN TRUE ELSE FALSE END AS is_business_day,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(MONTH FROM d)::INT AS month_number,
    TO_CHAR(d, 'Month') AS month_name,
    TO_CHAR(d, 'Mon') AS month_abbrev,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT AS quarter_name,
    EXTRACT(YEAR FROM d)::INT AS year,
    TO_CHAR(d, 'YYYY-MM') AS year_month,
    TO_CHAR(d, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM d)::TEXT AS year_quarter,
    EXTRACT(YEAR FROM d)::INT AS fiscal_year,
    EXTRACT(QUARTER FROM d)::INT AS fiscal_quarter,
    CASE WHEN d = (DATE_TRUNC('MONTH', d) + INTERVAL '1 MONTH - 1 DAY')::DATE THEN TRUE ELSE FALSE END AS is_month_end,
    CASE WHEN d = (DATE_TRUNC('QUARTER', d) + INTERVAL '3 MONTHS - 1 DAY')::DATE THEN TRUE ELSE FALSE END AS is_quarter_end,
    CASE WHEN d = (DATE_TRUNC('YEAR', d) + INTERVAL '1 YEAR - 1 DAY')::DATE THEN TRUE ELSE FALSE END AS is_year_end
FROM generate_series('2020-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) d;

COMMENT ON TABLE dw.dim_date IS 'Calendar dimension for time-based analysis (2020-2025)';
