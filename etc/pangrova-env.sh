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
#   cp etc/pangrova-env.sh \
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
# Path to JDK 17+ installation. If unset, startup scripts use java from PATH.
# export JAVA_HOME="/usr/lib/jvm/java-17"

# ── Pangrova Home ──────────────────────────────────────────────
# Installation root. Auto-detected from script location when the
# distribution layout is intact. Override only if scripts are
# symlinked or installed in a non-standard location.
# export PANGROVA_HOME="/opt/pangrova"

# ── Log Directory ────────────────────────────────────────────
# All log files and PID files are written here. If unset, startup scripts use
# $PANGROVA_HOME/logs. The directory must be writable by the pangrova user.
# export PANGROVA_LOG_DIR="/var/log/pangrova"

# ── Configuration Directory ──────────────────────────────────
# Overrides the default config directory location.
# Default: $PANGROVA_HOME/etc/ (or etc/ in dev)
# export PANGROVA_CONF_DIR="/etc/pangrova"

# ── JVM Options Override ─────────────────────────────────────
# When set, REPLACES all JVM options from jvm-*.config files.
# Prefer editing jvm-*.config files for per-role tuning;
# use PANGROVA_JAVA_OPTS only for emergency per-node overrides.
# export PANGROVA_JAVA_OPTS="-Xms16g -Xmx16g -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"


# ════════════════════════════════════════════════════════════
# DATABASE — State Store and Metadata (Coordinator only)
#
# coordinator.yml interpolates these placeholders into the HA and
# metadata JDBC URLs. The JDBC connections themselves are opened
# with empty username/password — authenticate via connection-string
# parameters or instance-level trust.
# ════════════════════════════════════════════════════════════

# PostgreSQL host and port (required for HA mode)
# export PANGROVA_DB_HOST="postgres.internal"
# export PANGROVA_DB_PORT="5432"


# ════════════════════════════════════════════════════════════
# SECURITY (Coordinator ↔ Worker)
# ════════════════════════════════════════════════════════════

# Internal cluster auth — shared secret for the X-Internal-Auth
# header between Coordinator and Workers. Must be the same on all
# nodes (consumed via worker.internal-auth-secret).
# Generate: openssl rand -base64 48
# export PANGROVA_INTERNAL_AUTH_SECRET="INJECT_AT_RUNTIME"


# ════════════════════════════════════════════════════════════
# NETWORK — Node Identity
#
# Workers must advertise the hostname/IP that the Coordinator
# and other Workers can connect to. When running behind NAT
# or with multiple network interfaces, auto-detection picks
# the wrong address — set these explicitly.
# ════════════════════════════════════════════════════════════

# Hostname or IP that other cluster members connect to
# (consumed via worker.advertised-host).
# export PANGROVA_ADVERTISED_HOST="$(hostname -f)"


# ════════════════════════════════════════════════════════════
# OBSERVABILITY — OpenTelemetry (Workers)
#
# Consumed via observability.otlp-endpoint in worker.yml.
# ════════════════════════════════════════════════════════════

# OTLP endpoint for the OpenTelemetry Collector
# export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.internal:4317"


# ════════════════════════════════════════════════════════════
# AI INFERENCE (Worker — optional)
#
# AI_* SQL functions call an OpenAI-compatible inference endpoint.
# These are JVM system properties, not environment variables —
# add them to jvm-worker.config or PANGROVA_JAVA_OPTS:
#   -Dpangrova.ai.inference.endpoint=http://ollama.internal:11434/v1
#   -Dpangrova.ai.inference.api-key=<key>
#   -Dpangrova.ai.inference.model=llama3
# ════════════════════════════════════════════════════════════


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
# These variables are referenced by the catalog YAML placeholders.
#
# export ALIBABA_CLOUD_ACCESS_KEY_ID="INJECT_AT_RUNTIME"
# export ALIBABA_CLOUD_ACCESS_KEY_SECRET="INJECT_AT_RUNTIME"

# ── MinIO / S3-Compatible ──────────────────────────────────
# MinIO, Ceph RADOS GW, Cloudflare R2, Wasabi, etc.
# These variables are referenced by the catalog YAML placeholders.
# export MINIO_ACCESS_KEY="INJECT_AT_RUNTIME"
# export MINIO_SECRET_KEY="INJECT_AT_RUNTIME"

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
