-- Encryption Setup for PII Columns
-- Banking Data Warehouse

-- Install pgcrypto extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create encryption key function
CREATE OR REPLACE FUNCTION public.set_encryption_key(key_text text)
RETURNS void AS $$
BEGIN
    PERFORM set_config('app.encryption_key', key_text, false);
END;
$$ LANGUAGE plpgsql;

-- Create encryption function
CREATE OR REPLACE FUNCTION public.encrypt_pii(data text)
RETURNS text AS $$
BEGIN
    RETURN pgp_sym_encrypt(data, current_setting('app.encryption_key'));
END;
$$ LANGUAGE plpgsql;

-- Create decryption function
CREATE OR REPLACE FUNCTION public.decrypt_pii(encrypted_data bytea)
RETURNS text AS $$
BEGIN
    RETURN pgp_sym_decrypt(encrypted_data, current_setting('app.encryption_key'));
EXCEPTION
    WHEN OTHERS THEN
        RETURN '[DECRYPTION_ERROR]';
END;
$$ LANGUAGE plpgsql;

-- Example: Add encrypted column to dim_customer
-- ALTER TABLE gold.dim_customer ADD COLUMN email_encrypted bytea;
-- UPDATE gold.dim_customer SET email_encrypted = encrypt_pii(email);

-- Example: Query with decryption
-- SELECT customer_id, decrypt_pii(email_encrypted) as email FROM gold.dim_customer;
