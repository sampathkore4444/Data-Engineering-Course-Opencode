-- =============================================================================
-- COLUMN-LEVEL DATA MASKING FOR BANKING PII
-- =============================================================================
-- Purpose: Protect sensitive customer data based on user roles
-- Tool:    Dremio Column-Level Security (CLS)
-- =============================================================================

-- =============================================================================
-- 1. CREATE MASKING POLICIES
-- =============================================================================

-- Policy: Mask customer name based on role
CREATE MASKING POLICY customer_name_mask
AS (
    CASE 
        -- Full access for data engineers
        WHEN IS_MEMBER_OF_ROLE('data-engineers') THEN customer_name
        
        -- Show last name initial for analysts
        WHEN IS_MEMBER_OF_ROLE('data-analysts') THEN 
            CONCAT(
                LEFT(customer_name, 1),
                '***',
                CASE 
                    WHEN CHARINDEX(' ', customer_name) > 0 
                    THEN CONCAT(' ', SUBSTRING(customer_name, CHARINDEX(' ', customer_name) + 1, 1), '***')
                    ELSE ''
                END
            )
        
        -- Show only "Customer XXXX" for risk team
        WHEN IS_MEMBER_OF_ROLE('risk-team') THEN 
            CONCAT('Customer ', RIGHT(customer_id, 4))
        
        -- Fully mask for others
        ELSE '***MASKED***'
    END
) ON banking-cleansed.core_banking_customers.customer_name;


-- Policy: Mask PAN (tax ID) number
CREATE MASKING POLICY pan_number_mask
AS (
    CASE 
        WHEN IS_MEMBER_OF_ROLE('data-engineers') THEN pan_number
        
        WHEN IS_MEMBER_OF_ROLE('compliance-team') THEN pan_number
        
        -- Show only last 4 digits for analysts
        WHEN IS_MEMBER_OF_ROLE('data-analysts') THEN 
            CONCAT('XXXXX', RIGHT(pan_number, 4))
        
        -- Fully mask for others
        ELSE 'XXXXX*****'
    END
) ON banking-cleansed.core_banking_customers.pan_number;


-- Policy: Mask email address
CREATE MASKING POLICY email_mask
AS (
    CASE 
        WHEN IS_MEMBER_OF_ROLE('data-engineers') THEN email
        
        WHEN IS_MEMBER_OF_ROLE('compliance-team') THEN email
        
        -- Partially mask for analysts
        WHEN IS_MEMBER_OF_ROLE('data-analysts') THEN 
            CONCAT(
                LEFT(SPLIT_PART(email, '@', 1), 2),
                '***@',
                SPLIT_PART(email, '@', 2)
            )
        
        -- Fully mask for others
        ELSE '***@***.***'
    END
) ON banking-cleansed.core_banking_customers.email;


-- Policy: Mask phone number
CREATE MASKING POLICY phone_mask
AS (
    CASE 
        WHEN IS_MEMBER_OF_ROLE('data-engineers') THEN phone
        
        WHEN IS_MEMBER_OF_ROLE('compliance-team') THEN phone
        
        -- Show only last 4 digits
        WHEN IS_MEMBER_OF_ROLE('data-analysts') THEN 
            CONCAT('XXXX', RIGHT(phone, 4))
        
        -- Fully mask for others
        ELSE 'XXXXXXXXXX'
    END
) ON banking-cleansed.core_banking_customers.phone;


-- Policy: Mask credit card number
CREATE MASKING POLICY card_number_mask
AS (
    CASE 
        WHEN IS_MEMBER_OF_ROLE('fraud-team') THEN card_number
        
        WHEN IS_MEMBER_OF_ROLE('data-engineers') THEN card_number
        
        -- Show only last 4 digits
        WHEN IS_MEMBER_OF_ROLE('data-analysts') THEN 
            CONCAT('XXXX-XXXX-XXXX-', RIGHT(card_number, 4))
        
        -- Fully mask for others
        ELSE 'XXXX-XXXX-XXXX-XXXX'
    END
) ON banking-cleansed.credit_cards.card_number;


-- =============================================================================
-- 2. APPLY MASKING POLICIES
-- =============================================================================

-- Apply customer_name_mask
ALTER TABLE banking-cleansed.core_banking_customers
MODIFY COLUMN customer_name
SET MASKING POLICY customer_name_mask;

-- Apply pan_number_mask
ALTER TABLE banking-cleansed.core_banking_customers
MODIFY COLUMN pan_number
SET MASKING POLICY pan_number_mask;

-- Apply email_mask
ALTER TABLE banking-cleansed.core_banking_customers
MODIFY COLUMN email
SET MASKING POLICY email_mask;

-- Apply phone_mask
ALTER TABLE banking-cleansed.core_banking_customers
MODIFY COLUMN phone
SET MASKING POLICY phone_mask;

-- Apply card_number_mask
ALTER TABLE banking-cleansed.credit_cards
MODIFY COLUMN card_number
SET MASKING POLICY card_number_mask;


-- =============================================================================
-- 3. ROW-LEVEL SECURITY (RLS)
-- =============================================================================

-- Policy: Branch-based row access
CREATE ROW ACCESS POLICY branch_access_policy
AS (
    CASE 
        -- Data engineers see all branches
        WHEN IS_MEMBER_OF_ROLE('data-engineers') THEN TRUE
        
        -- Analysts see only their branch
        WHEN IS_MEMBER_OF_ROLE('data-analysts') THEN 
            branch_code = CURRENT_USER_BRANCH()
        
        -- Risk team sees all branches
        WHEN IS_MEMBER_OF_ROLE('risk-team') THEN TRUE
        
        -- Others see nothing
        ELSE FALSE
    END
) ON banking-cleansed.core_banking_accounts;

-- Apply RLS
ALTER TABLE banking-cleansed.core_banking_accounts
SET ROW ACCESS POLICY branch_access_policy;


-- =============================================================================
-- 4. VERIFICATION QUERIES
-- =============================================================================

-- Test: Data Engineer sees full data
SET ROLE data-engineers;
SELECT customer_name, pan_number, email, phone 
FROM banking-cleansed.core_banking_customers 
LIMIT 5;
-- Expected: Full unmasked data

-- Test: Analyst sees masked data
SET ROLE data-analysts;
SELECT customer_name, pan_number, email, phone 
FROM banking-cleansed.core_banking_customers 
LIMIT 5;
-- Expected: Partially masked data

-- Test: Compliance team sees full PII
SET ROLE compliance-team;
SELECT customer_name, pan_number, email, phone 
FROM banking-cleansed.core_banking_customers 
LIMIT 5;
-- Expected: Full unmasked data

-- Reset role
RESET ROLE;
