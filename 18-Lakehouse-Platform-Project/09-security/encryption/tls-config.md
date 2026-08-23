# TLS Configuration for Banking Data Platform

## Overview
This guide covers TLS/SSL configuration for all components in the banking data platform to ensure data encryption in transit.

## Components Requiring TLS

| Component | Port | Protocol | Certificate Required |
|-----------|------|----------|---------------------|
| Dremio | 9047 | HTTPS | Yes |
| Kafka | 9093 | SSL | Yes |
| MinIO | 9000 | HTTPS | Yes |
| PostgreSQL | 5432 | SSL | Yes |
| Airflow | 8080 | HTTPS | Yes |
| Grafana | 3000 | HTTPS | Yes |

## Certificate Generation

### Self-Signed CA (Development Only)

```bash
# Generate CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt \
  -subj "/C=VN/ST=HCM/L=HCM/O=Banking/CN=Banking CA"

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/C=VN/ST=HCM/L=HCM/O=Banking/CN=dremio"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt
```

### Production Certificates

Use a trusted CA (DigiCert, Let's Encrypt, internal PKI):

```bash
# Request certificate from CA
openssl req -new -newkey rsa:2048 -nodes \
  -keyout server.key -out server.csr \
  -subj "/C=VN/ST=HCM/L=HCM/O=Banking/CN=dremio.bank.com"

# Submit CSR to CA and receive signed certificate
# Place certificate and key in /etc/ssl/certs/
```

## Dremio TLS Configuration

### docker-compose.yml additions

```yaml
services:
  dremio-master:
    environment:
      - DREMIO_JAVA_SERVER_EXTRA_OPTS=-Ddremio.ssl.enabled=true
      - DREMIO_SERVER_CERT=/opt/dremio/certs/server.crt
      - DREMIO_SERVER_KEY=/opt/dremio/certs/server.key
      - DREMIO_CA_CERT=/opt/dremio/certs/ca.crt
    volumes:
      - ./certs/dremio:/opt/dremio/certs:ro
```

### dremio.conf

```hocon
# /opt/dremio/conf/dremio.conf
dremio {
  ssl {
    enabled = true
    port = 9047
    keystore {
      path = "/opt/dremio/certs/server.jks"
      password = "${DREMIO_SSL_KEYSTORE_PASSWORD}"
    }
    truststore {
      path = "/opt/dremio/certs/truststore.jks"
      password = "${DREMIO_SSL_TRUSTSTORE_PASSWORD}"
    }
  }
}
```

## Kafka SSL Configuration

### server.properties

```properties
# Listener SSL
listeners=SSL://kafka-1:9093
ssl.keystore.location=/etc/kafka/ssl/kafka.server.keystore.jks
ssl.keystore.password=${KAFKA_SSL_KEYSTORE_PASSWORD}
ssl.key.password=${KAFKA_SSL_KEY_PASSWORD}
ssl.truststore.location=/etc/kafka/ssl/kafka.server.truststore.jks
ssl.truststore.password=${KAFKA_SSL_TRUSTSTORE_PASSWORD}
ssl.client.auth=required
ssl.endpoint.identification.algorithm=https

# Inter-broker SSL
inter.broker.listener.name=SSL
```

### Create Kafka SSL Keystore

```bash
# Create keystore
keytool -keystore kafka.server.keystore.jks \
  -alias localhost -validity 365 -genkey \
  -keyalg RSA -keysize 2048 \
  -dname "CN=kafka-1, OU=Banking, O=Bank, L=HCM, ST=HCM, C=VN"

# Import CA certificate
keytool -keystore kafka.server.truststore.jks \
  -alias CARoot -import -file ca.crt -noprompt

# Import server certificate
keytool -keystore kafka.server.keystore.jks \
  -alias CARoot -import -file ca.crt -noprompt
```

## PostgreSQL SSL Configuration

### postgresql.conf

```conf
# Enable SSL
ssl = on
ssl_cert_file = '/etc/ssl/certs/server.crt'
ssl_key_file = '/etc/ssl/private/server.key'
ssl_ca_file = '/etc/ssl/certs/ca.crt'
ssl_crl_file = ''
ssl_prefer_server_ciphers = on
ssl_ciphers = 'HIGH:!aNULL:!MD5'
ssl_ecdh_curve = 'prime256v1'
```

### pg_hba.conf

```conf
# Require SSL for all connections
hostssl all all 0.0.0.0/0 scram-sha-256
```

## MinIO TLS Configuration

### docker-compose.yml

```yaml
services:
  minio:
    command: server /data --console-address ":9001"
    environment:
      - MINIO_CERT_FILE=/root/.minio/certs/public.crt
      - MINIO_KEY_FILE=/root/.minio/certs/private.key
    volumes:
      - ./certs/minio:/root/.minio/certs:ro
```

## Airflow TLS Configuration

### airflow.cfg

```cfg
[webserver]
web_server_ssl_cert = /opt/airflow/certs/server.crt
web_server_ssl_key = /opt/airflow/certs/server.key
web_server_ssl_ca_cert = /opt/airflow/certs/ca.crt
web_server_ssl_protocol = TLSv1.2
```

## Grafana TLS Configuration

### grafana.ini

```ini
[server]
protocol = https
cert_file = /etc/grafana/certs/server.crt
cert_key = /etc/grafana/certs/server.key
```

## Verification Commands

```bash
# Test TLS connection to Dremio
curl -k https://dremio-master:9047/apiv2/system/info

# Test TLS connection to Kafka
openssl s_client -connect kafka-1:9093

# Test TLS connection to PostgreSQL
psql "host=postgres-server sslmode=require dbname=banking"

# Test TLS connection to MinIO
curl -k https://minio:9000/minio/health/live
```

## Security Best Practices

1. **Use TLS 1.2 or higher** - Disable TLS 1.0 and 1.1
2. **Use strong cipher suites** - Avoid weak ciphers like RC4, DES
3. **Rotate certificates** - Set calendar reminders before expiry
4. **Monitor certificate expiry** - Use Prometheus/Grafana alerts
5. **Use private keys securely** - Store in secure vault, not on disk
6. **Enable mutual TLS (mTLS)** - For inter-service communication
7. **Audit TLS connections** - Log all SSL/TLS handshakes
