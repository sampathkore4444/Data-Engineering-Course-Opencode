-- Staging model for customers
-- Source: core_banking.customers
-- Purpose: Clean and standardize customer data

with source as (
    select * from {{ source('staging', 'stg_customers') }}
),

cleaned as (
    select
        -- Primary key
        customer_id::varchar as customer_id,
        
        -- Customer info
        upper(trim(full_name)) as customer_name,
        lower(trim(email)) as email,
        phone as phone_number,
        
        -- Classification
        upper(customer_type) as customer_type,
        upper(kyc_status) as kyc_status,
        
        -- Address
        trim(address) as address,
        trim(city) as city,
        trim(state) as state,
        trim(zip_code) as zip_code,
        
        -- Dates
        date_of_birth as date_of_birth,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        
        -- Metadata
        current_timestamp as dbt_loaded_at
        
    from source
    where customer_id is not null
)

select * from cleaned
