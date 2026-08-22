-- Staging model for transactions
-- Source: core_banking.transactions
-- Purpose: Clean and standardize transaction data

with source as (
    select * from {{ source('staging', 'stg_transactions') }}
),

cleaned as (
    select
        -- Primary key
        transaction_id::varchar as transaction_id,
        
        -- Foreign keys
        account_id::varchar as account_id,
        
        -- Transaction info
        upper(transaction_type) as transaction_type,
        amount::decimal(15,2) as amount,
        
        -- Description
        trim(description) as transaction_description,
        
        -- Status
        upper(status) as transaction_status,
        
        -- Reference
        reference_number::varchar as reference_number,
        
        -- Dates
        transaction_date::date as transaction_date,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        
        -- Metadata
        current_timestamp as dbt_loaded_at
        
    from source
    where transaction_id is not null
      and amount > 0
)

select * from cleaned
