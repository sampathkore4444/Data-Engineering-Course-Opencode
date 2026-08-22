-- Staging model for accounts
-- Source: core_banking.accounts
-- Purpose: Clean and standardize account data

with source as (
    select * from {{ source('staging', 'stg_accounts') }}
),

cleaned as (
    select
        -- Primary key
        account_id::varchar as account_id,
        
        -- Foreign key
        customer_id::varchar as customer_id,
        
        -- Account info
        upper(account_type) as account_type,
        trim(account_number) as account_number,
        
        -- Balance
        balance::decimal(15,2) as balance,
        
        -- Status
        upper(status) as account_status,
        
        -- Dates
        opened_date::date as opened_date,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        
        -- Metadata
        current_timestamp as dbt_loaded_at
        
    from source
    where account_id is not null
)

select * from cleaned
