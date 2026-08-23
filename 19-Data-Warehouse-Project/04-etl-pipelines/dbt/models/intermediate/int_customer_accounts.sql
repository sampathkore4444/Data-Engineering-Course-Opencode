-- Intermediate model: Customer with Accounts
-- Purpose: Combine customers with their accounts for dimension building

with customers as (
    select * from {{ ref('stg_customers') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

customer_accounts as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.phone_number,
        c.customer_type,
        c.kyc_status,
        c.address,
        c.city,
        c.state,
        c.zip_code,
        c.date_of_birth,
        
        -- Account aggregations
        count(distinct a.account_id) as total_accounts,
        sum(a.balance) as total_balance,
        max(a.balance) as max_account_balance,
        min(a.balance) as min_account_balance,
        
        -- Account types
        count(distinct case when a.account_type = 'SAVINGS' then a.account_id end) as savings_accounts,
        count(distinct case when a.account_type = 'CURRENT' then a.account_id end) as current_accounts,
        count(distinct case when a.account_type = 'TERM_DEPOSIT' then a.account_id end) as term_deposit_accounts,
        
        -- Metadata
        c.created_at as customer_created_at,
        c.updated_at as customer_updated_at,
        current_timestamp as dbt_processed_at
        
    from customers c
    left join accounts a on c.customer_id = a.customer_id
    group by 
        c.customer_id,
        c.customer_name,
        c.email,
        c.phone_number,
        c.customer_type,
        c.kyc_status,
        c.address,
        c.city,
        c.state,
        c.zip_code,
        c.date_of_birth,
        c.created_at,
        c.updated_at
)

select * from customer_accounts
