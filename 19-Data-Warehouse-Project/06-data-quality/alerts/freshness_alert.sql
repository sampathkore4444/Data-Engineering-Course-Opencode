-- Data Freshness Alert
-- Check if data is stale (not updated in last 24 hours)

-- Check staging table freshness
SELECT 
    'stg_customers' as table_name,
    MAX(updated_at) as last_update,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at))) / 3600 as hours_since_update,
    CASE 
        WHEN MAX(updated_at) < CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'FRESH'
    END as status
FROM staging.stg_customers

UNION ALL

SELECT 
    'stg_accounts' as table_name,
    MAX(updated_at) as last_update,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at))) / 3600 as hours_since_update,
    CASE 
        WHEN MAX(updated_at) < CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'FRESH'
    END as status
FROM staging.stg_accounts

UNION ALL

SELECT 
    'stg_transactions' as table_name,
    MAX(created_at) as last_update,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(created_at))) / 3600 as hours_since_update,
    CASE 
        WHEN MAX(created_at) < CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 'STALE'
        ELSE 'FRESH'
    END as status
FROM staging.stg_transactions;
