---
title: Getting Started
sidebar_position: 1
description: Build and run Pangrova in 5 minutes
---

# Getting Started

This guide walks you through building Pangrova from source, launching a standalone instance, and running your first query in under 5 minutes.

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| JDK | 17 or later |
| Maven | 3.8+ |
| OS | Linux (x86_64/aarch64) or macOS (aarch64) |
| Disk | 2 GB free (build artifacts) |
| Memory | 4 GB minimum |

## Build from Source

```bash
git clone https://github.com/pangrova-project/pangrova.git
cd pangrova
mvn clean package -DskipTests
```

The build produces JAR files in each module's `target/` directory.

Expected build time: ~3-5 minutes on a modern machine. If the build fails, check:

| Symptom | Fix |
|---------|-----|
| `javac: invalid target release: 17` | Install JDK 17+ and set `JAVA_HOME` |
| `Could not resolve dependencies` | Run `mvn dependency:resolve` first, check network/proxy |
| `Native build failure` | Safe to skip: `mvn clean package -DskipTests -pl '!pangrova-worker-native,!pangrova-native-bundle'` |

### Verify the Build

```bash
# Check that key JARs were produced
ls pangrova-coordinator/target/pangrova-coordinator-*.jar
ls pangrova-worker/target/pangrova-worker-*.jar
ls pangrova-client/pangrova-client-cli/target/pangrova-client-cli-*.jar
```

## Start Standalone Mode

Standalone mode runs both Coordinator and Worker in a single JVM — ideal for development and testing.

### Set Up Environment (Optional)

```bash
# If JAVA_HOME is not already set
export JAVA_HOME=/path/to/jdk-17

# Customize JVM memory (default: 2g-8g heap + 4g direct memory)
# export PANGROVA_JAVA_OPTS="-Xms1g -Xmx4g -XX:MaxDirectMemorySize=2g"
```

### Launch

```bash
bin/pangrova-standalone.sh
```

You should see output like:

```
Starting Pangrova Standalone (Coordinator + Worker)...
  PANGROVA_HOME:     /path/to/pangrova
  PANGROVA_CONF_DIR: /path/to/pangrova/docs/develop/configuration
  JAVA:            /path/to/jdk-17/bin/java (version 17)
  JVM_CONFIG:      /path/to/pangrova/docs/develop/configuration/jvm-coordinator.config (loaded)
```

By default, the Coordinator listens on port **8080**. Verify it is running:

```bash
bin/pangrova-health-check.sh localhost:8080
# Output: healthy
```

### Using a Custom Configuration

```bash
bin/pangrova-standalone.sh --config /path/to/standalone.yml
```

The standalone config must contain both `coordinator.*` and `worker.*` sections. See `docs/develop/configuration/standalone.yml` for the template.

### Running as a Daemon

```bash
bin/pangrova-standalone.sh --daemon
# PID written to logs/standalone.pid
```

Check the daemon log:

```bash
tail -f logs/standalone.out
```

### Common Startup Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ERROR: Java 17+ required` | Wrong JDK version | Set `JAVA_HOME` to JDK 17+ |
| `ERROR: No JARs found` | Build not run | Run `mvn clean package -DskipTests` |
| `ERROR: Standalone already running` | Previous instance still alive | Run `bin/pangrova-stop.sh --all` |
| `Port 8080 already in use` | Another process on port 8080 | `lsof -i :8080` to find it, or change port in `docs/develop/configuration/standalone.yml` |

## Run Your First Query

### Using the CLI

```bash
bin/pangrova-cli.sh --host localhost --port 8080
```

At the Pangrova prompt:

```sql
SELECT 1 + 1 AS result;
```

### Using curl (REST API)

```bash
curl -X POST http://localhost:8080/v1/query/submit \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT 1 + 1 AS result"}'
```

The response includes a `query_id` and `state`. See [Clients](clients.md) for the full REST API reference.

## Stop the Server

```bash
bin/pangrova-stop.sh --all
```

## End-to-End Example: Query Iceberg Data

Once Pangrova is running, try a complete workflow with an Iceberg catalog:

### Register a Local Iceberg Catalog

```sql
-- In the CLI
CREATE CATALOG local_iceberg
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = 'file:///tmp/iceberg-warehouse'
);
```

### Create a Table and Insert Data

```sql
CREATE SCHEMA local_iceberg.demo;

CREATE TABLE local_iceberg.demo.events (
  event_id BIGINT,
  event_type VARCHAR,
  user_id VARCHAR,
  event_time TIMESTAMP,
  payload VARCHAR
) PARTITIONED BY (event_type);

INSERT INTO local_iceberg.demo.events VALUES
  (1, 'click', 'user-001', TIMESTAMP '2024-01-15 10:30:00', '{"page": "/home"}'),
  (2, 'purchase', 'user-002', TIMESTAMP '2024-01-15 11:00:00', '{"item": "widget"}'),
  (3, 'click', 'user-001', TIMESTAMP '2024-01-15 11:30:00', '{"page": "/products"}');
```

### Query with Aggregation

```sql
SELECT event_type, COUNT(*) AS cnt
FROM local_iceberg.demo.events
GROUP BY event_type
ORDER BY cnt DESC;
```

### Time Travel

```sql
-- Query data as it existed at a specific point in time
SELECT * FROM local_iceberg.demo.events
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:45:00';
```

### Table Maintenance

```sql
-- Compact small files for better read performance
COMPACT TABLE local_iceberg.demo.events;

-- Clean up old snapshots
VACUUM TABLE local_iceberg.demo.events RETAIN 24 HOURS;
```

### Cleanup

```sql
DROP TABLE local_iceberg.demo.events;
DROP SCHEMA local_iceberg.demo;
DROP CATALOG local_iceberg;
```

## Docker Quick Start

If you prefer Docker:

```bash
cd docs/develop/deployment/docker
docker compose up -d
```

Check health:

```bash
curl -f http://localhost:8080/v1/health
```

Stop:

```bash
docker compose down
```

---

## Distribution Layout

When Pangrova is packaged as a binary distribution, the directory structure is:

```
pangrova-{version}/
├── bin/                    ← Startup, stop, and operations scripts
│   ├── pangrova.sh           ← Unified entry point (subcommand delegation)
│   ├── pangrova-coordinator.sh
│   ├── pangrova-worker.sh
│   ├── pangrova-standalone.sh
│   ├── pangrova-stop.sh
│   ├── pangrova-cli.sh       ← Interactive SQL client
│   ├── pangrova-catalog.sh   ← Catalog management utility
│   ├── pangrova-health-check.sh
│   ├── pangrova-cluster-status.sh
│   └── pangrova-rolling-upgrade.sh
├── config/                 ← Configuration templates (copy before editing)
│   ├── coordinator.yml     ← Coordinator configuration
│   ├── worker.yml          ← Worker configuration
│   ├── standalone.yml      ← Standalone mode configuration
│   ├── pangrova-env.sh       ← Environment variable overrides
│   ├── logback.xml         ← Logging configuration
│   ├── jvm-coordinator.config  ← Coordinator JVM options
│   ├── jvm-worker.config       ← Worker JVM options
│   └── catalog/            ← Catalog configuration (one file per data source)
│       ├── iceberg-example.yml
│       └── paimon-example.yml
├── lib/                    ← JAR files (classpath)
├── logs/                   ← Log files and PID files
└── data/                   ← Local data (spill, cache)
```

| Directory | Purpose |
|-----------|---------|
| `bin/` | All executable scripts for starting, stopping, and managing the cluster |
| `config/` | Configuration templates; copy and modify for your environment |
| `lib/` | Application JAR files loaded onto the classpath at startup |
| `logs/` | Runtime log files and `.pid` files for daemon processes |
| `data/` | Local storage for spill-to-disk, native engine cache, and temporary data |

## Next Steps

- [SQL Reference](sql-reference.md) — Learn the full SQL syntax
- [Clients](clients.md) — Connect via JDBC, REST, or CLI
- [Deployment & Operations](../develop/README.md) — Deploy in distributed or Kubernetes environments
