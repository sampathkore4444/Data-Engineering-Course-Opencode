-- Mart model: Customer Dimension (SCD Type 2)
-- Purpose: Create a slowly changing dimension for customers

{{
    config(
        materialized='table',
        schema='gold',
        unique_key='customer_sk'
    )
}}

with customer_data as (
    select * from {{ ref('int_customer_accounts') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
        
        -- Natural key
        customer_id,
        
        -- Customer attributes
        customer_name,
        email,
        phone_number,
        customer_type,
        kyc_status,
        address,
        city,
        state,
        zip_code,
        date_of_birth,
        
        -- Account aggregations
        total_accounts,
        total_balance,
        max_account_balance,
        min_account_balance,
        savings_accounts,
        current_accounts,
        term_deposit_accounts,
        
        -- Customer segmentation
        case
            when total_balance >= 1000000000 then 'PLATINUM'
            when total_balance >= 500000000 then 'GOLD'
            when total_balance >= 100000000 then 'SILVER'
            else 'STANDARD'
        end as customer_segment,
        
        -- SCD Type 2 columns
        customer_created_at as valid_from,
        coalesce(customer_updated_at, current_timestamp) as valid_to,
        true as is_current,
        
        -- Metadata
        current_timestamp as dbt_updated_at
        
    from customer_data
)

select * from final
