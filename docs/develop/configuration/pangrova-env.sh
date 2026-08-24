#!/usr/bin/env bash
# ============================================================
# Pangrova — Environment Variable Configuration (Production)
#
# Sourced by bin/ startup scripts before launching Java
# processes. Uncomment and set variables for your environment.
#
# DO NOT store plaintext secrets in this file if it is
# committed to version control. Use a secrets manager
# (Vault, AWS Secrets Manager, GCP Secret Manager) and
# inject secrets via environment variables at runtime.
#
# Usage (bare-metal / VM):
#   cp docs/develop/configuration/pangrova-env.sh \
#      /etc/pangrova/pangrova-env.sh
#   chmod 640 /etc/pangrova/pangrova-env.sh
#   chown root:pangrova /etc/pangrova/pangrova-env.sh
#   vim /etc/pangrova/pangrova-env.sh   # uncomment & fill in
#
# Usage (container / Kubernetes):
#   Set these as container environment variables or mount
#   a Kubernetes Secret as an env-from source. The startup
#   scripts source this file only if it exists on disk.
# ============================================================


# ── Java ─────────────────────────────────────────────────────
# Path to JDK 17+ installation. Required.
# If unset, scripts use $JAVA_HOME from the system environment.
export JAVA_HOME="/usr/lib/jvm/java-17"

# ── Pangrova Home ──────────────────────────────────────────────
# Installation root. Auto-detected from script location when the
# distribution layout is intact. Override only if scripts are
# symlinked or installed in a non-standard location.
# export PANGROVA_HOME="/opt/pangrova"

# ── Log Directory ────────────────────────────────────────────
# All log files and PID files are written here.
# The directory must be writable by the pangrova user.
export PANGROVA_LOG_DIR="/var/log/pangrova"

# ── Configuration Directory ──────────────────────────────────
# Overrides the default config directory location.
# Default: $PANGROVA_HOME/etc/ (or docs/develop/configuration/ in dev)
# export PANGROVA_CONF_DIR="/etc/pangrova"

# ── JVM Options Override ─────────────────────────────────────
# When set, REPLACES all JVM options from jvm-*.config files.
# Prefer editing jvm-*.config files for per-role tuning;
# use PANGROVA_JAVA_OPTS only for emergency per-node overrides.
# export PANGROVA_JAVA_OPTS="-Xms16g -Xmx16g -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"


# ════════════════════════════════════════════════════════════
# DATABASE — State Store and Metadata (Coordinator only)
#
# The Coordinator uses PostgreSQL for:
#   - HA leader-election state store
#   - Query metadata persistence
#
# Inject the password via a secrets manager at runtime.
# Example (Vault agent sidecar):
#   export PANGROVA_DB_PASSWORD=$(vault kv get -field=password secret/pangrova/db)
# ════════════════════════════════════════════════════════════

# PostgreSQL host and port (required for HA mode)
export PANGROVA_DB_HOST="postgres.internal"
export PANGROVA_DB_PORT="5432"

# Database name for Pangrova metadata
export PANGROVA_DB_NAME="pangrova_meta"

# Database user (must have CREATE TABLE / INSERT / UPDATE / DELETE)
export PANGROVA_DB_USER="pangrova"

# Database password — MUST be injected from a secrets manager
# in production. Never commit a real password here.
# export PANGROVA_DB_PASSWORD="INJECT_AT_RUNTIME"


# ════════════════════════════════════════════════════════════
# SECURITY — OAuth2 / JWT (Coordinator)
#
# Pangrova validates JWT bearer tokens issued by your IdP.
# Supported: Keycloak, Auth0, Okta, AWS Cognito, Azure AD.
# ════════════════════════════════════════════════════════════

# OIDC Issuer URI — must match the "iss" claim in tokens.
# Example: https://keycloak.internal/realms/pangrova
# export PANGROVA_OAUTH2_ISSUER_URI="https://idp.internal/realms/pangrova"

# Expected audience — must match the "aud" claim in tokens.
# export PANGROVA_OAUTH2_AUDIENCE="pangrova-api"

# JWKS endpoint URI (overrides auto-discovery if needed)
# Usually discovered from ${issuer-uri}/.well-known/openid-configuration
# export PANGROVA_OAUTH2_JWKS_URI="https://idp.internal/realms/pangrova/protocol/openid-connect/certs"

# Internal cluster auth — shared HMAC secret for
# Coordinator ↔ Worker RPC. Must be the same on all nodes.
# Generate: openssl rand -base64 48
# export PANGROVA_INTERNAL_AUTH_SECRET="INJECT_AT_RUNTIME"


# ════════════════════════════════════════════════════════════
# TLS (all nodes)
#
# Pangrova supports TLS for:
#   - Client → Coordinator REST API
#   - Coordinator → Worker gRPC / Arrow Flight
#   - Worker → Worker shuffle (Arrow Flight)
# ════════════════════════════════════════════════════════════

# TLS certificate chain (PEM), key (PEM), and CA bundle.
# Use Let's Encrypt, your corporate PKI, or self-signed certs.
# export PANGROVA_TLS_CERT_FILE="/etc/pangrova/tls/server.crt"
# export PANGROVA_TLS_KEY_FILE="/etc/pangrova/tls/server.key"
# export PANGROVA_TLS_CA_FILE="/etc/pangrova/tls/ca-bundle.crt"


# ════════════════════════════════════════════════════════════
# NETWORK — Node Identity
#
# Workers must advertise the hostname/IP that the Coordinator
# and other Workers can connect to. When running behind NAT
# or with multiple network interfaces, auto-detection picks
# the wrong address — set these explicitly.
# ════════════════════════════════════════════════════════════

# Hostname or IP that other cluster members connect to.
# export PANGROVA_ADVERTISED_HOST="$(hostname -f)"

# Fixed port for the Worker's transport listener (gRPC / Flight).
# Default: 9090. Set explicitly in multi-worker deployments.
# export PANGROVA_TRANSPORT_PORT="9090"


# ════════════════════════════════════════════════════════════
# OBSERVABILITY — OpenTelemetry
#
# Pangrova exports traces and metrics via OTLP (gRPC or HTTP).
# Targets: Grafana Tempo, Jaeger, Honeycomb, Datadog Agent.
# ════════════════════════════════════════════════════════════

# OTLP gRPC endpoint (no scheme, no trailing slash)
# export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.internal:4317"

# Service name as it appears in your trace backend
# export OTEL_SERVICE_NAME="pangrova-coordinator"   # or pangrova-worker

# Resource attributes (optional, propagated to all spans)
# export OTEL_RESOURCE_ATTRIBUTES="deployment.environment=production,cluster=us-east-1"

# Disable OTLP export entirely (useful in dev)
# export OTEL_TRACES_EXPORTER="none"
# export OTEL_METRICS_EXPORTER="none"


# ════════════════════════════════════════════════════════════
# AI INFERENCE (Worker — optional)
#
# Workers host the embedding and inference pipeline.
# Configure the inference provider endpoint and credentials.
# ════════════════════════════════════════════════════════════

# Base URL for the OpenAI-compatible inference endpoint.
# Supported: Ollama, vLLM, OpenAI, Bedrock proxy, etc.
# export PANGROVA_INFERENCE_BASE_URL="http://ollama.internal:11434"

# Embedding model name as registered with the inference provider.
# export PANGROVA_EMBEDDING_MODEL="nomic-embed-text"

# Reranker model name (optional, for hybrid search reranking).
# export PANGROVA_RERANKER_MODEL="BAAI/bge-reranker-v2-m3"

# API key for the inference provider (if required).
# Inject from a secrets manager at runtime.
# export PANGROVA_INFERENCE_API_KEY="INJECT_AT_RUNTIME"


# ════════════════════════════════════════════════════════════
# STORAGE — Object Store Credentials (Connector side)
#
# S3-compatible credentials are passed to connectors (Iceberg,
# Paimon) via system properties or environment variables.
# Prefer IAM instance roles / Workload Identity over static keys.
# ════════════════════════════════════════════════════════════

# ── AWS S3 ─────────────────────────────────────────────────
# Preferred: IAM instance profile (EC2) or Pod Identity (EKS).
# Static credentials only when IAM roles are unavailable.
# export AWS_REGION="us-east-1"
# export AWS_ACCESS_KEY_ID="INJECT_AT_RUNTIME"
# export AWS_SECRET_ACCESS_KEY="INJECT_AT_RUNTIME"
# export AWS_ROLE_ARN="arn:aws:iam::123456789012:role/pangrova-s3-role"

# Custom S3 endpoint for VPC endpoint or non-AWS stores
# export AWS_ENDPOINT_URL="https://s3.us-east-1.amazonaws.com"

# ── Alibaba Cloud OSS ──────────────────────────────────────
# OSS is S3-compatible. Use the S3 protocol with a custom endpoint.
# Endpoint format:
#   Public:   https://oss-{region}.aliyuncs.com
#   Internal: https://oss-{region}-internal.aliyuncs.com (within VPC)
#
# export ALIBABA_CLOUD_ACCESS_KEY_ID="INJECT_AT_RUNTIME"
# export ALIBABA_CLOUD_ACCESS_KEY_SECRET="INJECT_AT_RUNTIME"
# export PANGROVA_OSS_ENDPOINT="https://oss-cn-hangzhou-internal.aliyuncs.com"
# export PANGROVA_OSS_REGION="cn-hangzhou"
# export PANGROVA_OSS_BUCKET="my-lakehouse-bucket"
#
# For STS temporary credentials:
# export ALIBABA_CLOUD_SECURITY_TOKEN="INJECT_AT_RUNTIME"

# ── MinIO / S3-Compatible ──────────────────────────────────
# MinIO, Ceph RADOS GW, Cloudflare R2, Wasabi, etc.
# export MINIO_ACCESS_KEY="INJECT_AT_RUNTIME"
# export MINIO_SECRET_KEY="INJECT_AT_RUNTIME"
# export PANGROVA_MINIO_ENDPOINT="http://minio.internal:9000"
# export PANGROVA_MINIO_BUCKET="pangrova-data"

# ── GCS (Google Cloud Storage) ─────────────────────────────
# export GOOGLE_APPLICATION_CREDENTIALS="/etc/pangrova/gcp-sa-key.json"

# ── Azure ADLS Gen2 ────────────────────────────────────────
# export AZURE_STORAGE_ACCOUNT="myaccount"
# export AZURE_STORAGE_ACCESS_KEY="INJECT_AT_RUNTIME"


# ════════════════════════════════════════════════════════════
# OS TUNING
#
# These settings should be applied at the OS level (e.g., via
# /etc/security/limits.conf and /etc/sysctl.conf) rather than
# in this file, but they are documented here for completeness.
#
# Apply as root BEFORE starting Pangrova:
#
#   # Open file descriptors — needed for many concurrent connections
#   ulimit -n 1048576
#
#   # Max user processes — for large thread pools
#   ulimit -u 65536
#
#   # Disable swap — latency-sensitive query execution must not page
#   sysctl -w vm.swappiness=1
#
#   # Large page support for Arrow off-heap allocations (optional)
#   sysctl -w vm.nr_hugepages=2048
#
#   # Network tuning — large send/receive buffers for shuffle
#   sysctl -w net.core.rmem_max=134217728
#   sysctl -w net.core.wmem_max=134217728
#   sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
#   sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
#   sysctl -w net.core.netdev_max_backlog=250000
#
#   # Increase max socket backlog for the coordinator's REST listener
#   sysctl -w net.core.somaxconn=65535
#
#   # Reduce time_wait — allows rapid port reuse after worker restart
#   sysctl -w net.ipv4.tcp_tw_reuse=1
#
# Persist in /etc/sysctl.d/99-pangrova.conf and
# /etc/security/limits.d/99-pangrova.conf.
# ════════════════════════════════════════════════════════════
