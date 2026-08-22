-- Mart model: Transaction Fact Table
-- Purpose: Transaction facts with dimensional references
-- Materialization: Table (incremental)

{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='transaction_id',
        incremental_strategy='merge',
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_fact_txn_account ON {{ this }} (account_sk)",
            "CREATE INDEX IF NOT EXISTS idx_fact_txn_date ON {{ this }} (transaction_date_sk)",
            "CREATE INDEX IF NOT EXISTS idx_fact_txn_type ON {{ this }} (transaction_type)"
        ]
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
    {% endif %}
),

accounts as (
    select * from {{ ref('dim_account') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

final as (
    select
        -- Transaction identifiers
        t.transaction_id,
        
        -- Foreign keys (dimension surrogate keys)
        a.account_sk,
        d.date_key as transaction_date_sk,
        
        -- Transaction attributes
        t.transaction_type,
        t.amount,
        t.transaction_description,
        t.transaction_status,
        t.reference_number,
        
        -- Date components for analysis
        t.transaction_date,
        extract(year from t.transaction_date) as transaction_year,
        extract(month from t.transaction_date) as transaction_month,
        extract(day from t.transaction_date) as transaction_day,
        extract(dow from t.transaction_date) as transaction_day_of_week,
        
        -- Amount categories
        case
            when t.amount < 1000000 then 'SMALL'
            when t.amount < 10000000 then 'MEDIUM'
            when t.amount < 100000000 then 'LARGE'
            else 'VERY_LARGE'
        end as amount_category,
        
        -- Running totals (for analysis)
        sum(t.amount) over (
            partition by t.account_id 
            order by t.transaction_date, t.created_at
            rows between unbounded preceding and current row
        ) as running_balance,
        
        -- Metadata
        t.created_at,
        current_timestamp as dbt_processed_at
        
    from transactions t
    left join accounts a on t.account_id = a.account_id
    left join date_dim d on t.transaction_date = d.date_day
)

select * from final
