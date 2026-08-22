-- =============================================================================
-- DIMENSION TABLE: dim_customer
-- =============================================================================
-- Type: Slowly Changing Dimension (SCD) Type 2
-- Purpose: Store customer information with history tracking
-- =============================================================================

CREATE TABLE dw.dim_customer (
    -- Surrogate Key (auto-increment, unique identifier in DW)
    customer_sk         SERIAL PRIMARY KEY,

    -- Natural Key (from source system)
    customer_id         VARCHAR(20) NOT NULL,

    -- Descriptive Attributes
    customer_name       VARCHAR(100),
    date_of_birth       DATE,
    gender              VARCHAR(10),
    nationality         VARCHAR(50),
    pan_number          VARCHAR(20),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    city                VARCHAR(50),
    state               VARCHAR(50),
    pin_code            VARCHAR(10),
    customer_type       VARCHAR(20),  -- INDIVIDUAL, CORPORATE

    -- Derived Attributes
    age                 INT,
    age_group           VARCHAR(20),  -- YOUNG, ADULT, MIDDLE_AGE, SENIOR
    customer_segment    VARCHAR(20),  -- RETAIL, CORPORATE, HNWI

    -- SCD Type 2 Attributes
    effective_date      DATE DEFAULT CURRENT_DATE,
    expiry_date         DATE DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,

    -- Audit Columns
    source_system       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_dim_customer_id ON dw.dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON dw.dim_customer(is_current);
CREATE INDEX idx_dim_customer_city ON dw.dim_customer(city);
CREATE INDEX idx_dim_customer_segment ON dw.dim_customer(customer_segment);

-- =============================================================================
-- SCD Type 2 Merge Logic
-- =============================================================================
-- This function handles SCD Type 2 updates
-- When a customer changes, we:
-- 1. Expire the old record (set is_current = FALSE, expiry_date = today)
-- 2. Insert a new record (set is_current = TRUE, effective_date = today)

CREATE OR REPLACE FUNCTION dw.fn_update_customer_scd2()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if any attribute has changed
    IF OLD.customer_name != NEW.customer_name
       OR OLD.email != NEW.email
       OR OLD.phone != NEW.phone
       OR OLD.city != NEW.city
       OR OLD.customer_type != NEW.customer_type THEN

        -- Expire the old record
        UPDATE dw.dim_customer
        SET is_current = FALSE,
            expiry_date = CURRENT_DATE - INTERVAL '1 day',
            updated_at = CURRENT_TIMESTAMP
        WHERE customer_sk = OLD.customer_sk;

        -- Insert new record
        INSERT INTO dw.dim_customer (
            customer_id, customer_name, date_of_birth, gender, nationality,
            pan_number, email, phone, city, state, pin_code, customer_type,
            age, age_group, customer_segment,
            effective_date, expiry_date, is_current, source_system
        ) VALUES (
            NEW.customer_id, NEW.customer_name, NEW.date_of_birth, NEW.gender, NEW.nationality,
            NEW.pan_number, NEW.email, NEW.phone, NEW.city, NEW.state, NEW.pin_code, NEW.customer_type,
            NEW.age, NEW.age_group, NEW.customer_segment,
            CURRENT_DATE, '9999-12-31', TRUE, NEW.source_system
        );

        RETURN NEW;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SAMPLE DATA (Initial Load)
-- =============================================================================
INSERT INTO dw.dim_customer (
    customer_id, customer_name, date_of_birth, gender, nationality,
    pan_number, email, phone, city, state, pin_code, customer_type,
    age, age_group, customer_segment, source_system
)
SELECT
    c.customer_id,
    c.customer_name,
    c.date_of_birth,
    c.gender,
    c.nationality,
    c.pan_number,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.pin_code,
    c.customer_type,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.date_of_birth))::INT AS age,
    CASE
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.date_of_birth)) < 30 THEN 'YOUNG'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.date_of_birth)) < 45 THEN 'ADULT'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.date_of_birth)) < 60 THEN 'MIDDLE_AGE'
        ELSE 'SENIOR'
    END AS age_group,
    CASE
        WHEN c.customer_type = 'CORPORATE' THEN 'CORPORATE'
        ELSE 'RETAIL'
    END AS customer_segment,
    'CORE_BANKING' AS source_system
FROM cbs.customers c;

-- =============================================================================
-- DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE dw.dim_customer IS 'Customer dimension with SCD Type 2 tracking';
COMMENT ON COLUMN dw.dim_customer.customer_sk IS 'Surrogate key - auto-increment';
COMMENT ON COLUMN dw.dim_customer.customer_id IS 'Natural key from source system';
COMMENT ON COLUMN dw.dim_customer.effective_date IS 'Start date of this version';
COMMENT ON COLUMN dw.dim_customer.expiry_date IS 'End date of this version (9999-12-31 = current)';
COMMENT ON COLUMN dw.dim_customer.is_current IS 'TRUE if this is the current version';
