---
title: Connectors
sidebar_position: 4
description: Iceberg and Paimon connector configuration
---

# Connectors and Catalog Management

Pangrova connects to data lakes through connector plugins. Currently supported connectors are Iceberg and Paimon.

---

## Iceberg Connector

Apache Iceberg provides ACID transactions, schema evolution, and time travel on data lakes.

### Catalog Configuration

```yaml
# config/catalog/my-iceberg.yml
connector: iceberg

properties:
  type: hadoop          # hadoop, hive, rest, jdbc
  warehouse: "s3://my-bucket/iceberg-warehouse"
  # uri: "thrift://hive-metastore:9083"  # Required when type=hive
```

### SQL Registration

```sql
CREATE CATALOG my_iceberg
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = 's3://my-bucket/iceberg-warehouse'
);
```

### Catalog Types

| Type | Description | Required Properties |
|------|-------------|-------------------|
| `hadoop` | File-system based catalog (default) | `warehouse` |
| `hive` | Hive Metastore backed catalog | `warehouse`, `uri` |
| `rest` | REST catalog (e.g., Polaris, Unity) | `uri` |
| `jdbc` | JDBC backed catalog | `uri`, `jdbc.user`, `jdbc.password` |

### Supported Iceberg Features

| Feature | Status | Notes |
|---------|--------|-------|
| Schema evolution | Supported | ADD/DROP/RENAME COLUMN |
| Partition evolution | Supported | SET PARTITION SPEC |
| Time travel | Supported | FOR SYSTEM_TIME AS OF / FOR SYSTEM_VERSION AS OF |
| MERGE INTO | Supported | Row-level deletes via position delete files |
| INSERT OVERWRITE | Supported | Dynamic and static overwrite |
| COMPACT / OPTIMIZE | Supported | Small file compaction |
| VACUUM | Supported | Snapshot expiration + orphan cleanup |
| Hidden partitioning | Supported | year/month/day/hour/bucket/truncate transforms |
| Metadata tables | 🔮 Planned | $snapshots, $manifests, $files |

---

## Paimon Connector

Apache Paimon provides streaming lakehouse capabilities with real-time data ingestion.

### Catalog Configuration

```yaml
# config/catalog/my-paimon.yml
connector: paimon

properties:
  warehouse: "s3://my-bucket/paimon-warehouse"
  # metastore: filesystem  # filesystem (default), hive
```

### SQL Registration

```sql
CREATE CATALOG my_paimon
USING paimon
WITH (
  'warehouse' = 's3://my-bucket/paimon-warehouse'
);
```

---

## Storage Backends

Both connectors support multiple storage backends:

### Amazon S3

```yaml
properties:
  warehouse: "s3://my-bucket/warehouse"
  s3.endpoint: "https://s3.amazonaws.com"
  s3.access-key-id: "${ENV:AWS_ACCESS_KEY_ID}"
  s3.secret-access-key: "${ENV:AWS_SECRET_ACCESS_KEY}"
```

### HDFS

```yaml
properties:
  warehouse: "hdfs://namenode:8020/warehouse"
```

Ensure `HADOOP_CONF_DIR` is set or `core-site.xml`/`hdfs-site.xml` are on the classpath.

### Local Filesystem

```yaml
properties:
  warehouse: "file:///data/warehouse"
```

Suitable for development and testing only.

---

## Catalog Management

### Method 1: SQL (Runtime)

Register catalogs dynamically via SQL:

```sql
CREATE CATALOG prod_iceberg
USING iceberg
WITH (
  'type' = 'hive',
  'warehouse' = 's3://prod-bucket/iceberg',
  'uri' = 'thrift://hive-metastore:9083'
);

SHOW CATALOGS;
DESCRIBE CATALOG prod_iceberg;
DROP CATALOG prod_iceberg;
```

### Method 2: Configuration Files (Startup)

Place YAML files in `config/catalog/` — one file per catalog. The filename (without `.yml`) becomes the catalog name.

```bash
# Copy from template
cp config/catalog/iceberg-example.yml config/catalog/prod-iceberg.yml

# Edit the configuration
vi config/catalog/prod-iceberg.yml
```

> **Note**: File-based catalog auto-registration at startup is 🔮 Planned. Currently, `config/catalog/` files serve as configuration references and are used by `bin/pangrova-catalog.sh list`.

### Catalog Management Utility

```bash
# List configured catalogs
bin/pangrova-catalog.sh list

# Test catalog connectivity (requires running Coordinator)
bin/pangrova-catalog.sh test my-iceberg
```

---

## Troubleshooting

### Catalog Registration Fails

| Error | Cause | Fix |
|-------|-------|-----|
| `ConnectorFactory not found: iceberg` | Missing connector JAR | Verify `pangrova-connector-iceberg-*.jar` is in `lib/` |
| `Cannot create warehouse directory` | Permission denied on warehouse path | Check filesystem permissions or S3 credentials |
| `Unable to connect to Hive Metastore` | Wrong URI or metastore down | Verify `uri` property and metastore availability |

### S3 Access Issues

| Error | Cause | Fix |
|-------|-------|-----|
| `Access Denied` | Wrong credentials | Verify `s3.access-key-id` and `s3.secret-access-key` |
| `NoSuchBucket` | Bucket doesn't exist | Create the S3 bucket first |
| `Connection refused` | Wrong endpoint | Verify `s3.endpoint` matches your S3-compatible service |

### Query Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `Table not found` | Wrong catalog/schema path | Use fully-qualified name: `catalog.schema.table` |
| `Schema mismatch` | Column type changed | Check schema evolution history with `DESCRIBE TABLE` |


---

## End-to-End Example: Multi-Catalog Analytics

Set up a production environment with both Iceberg and Paimon catalogs querying across data sources.

**Step 1: Register catalogs**

```sql
-- Iceberg catalog for historical analytics (S3 + Hive Metastore)
CREATE CATALOG prod_iceberg
USING iceberg
WITH (
  'type' = 'hive',
  'warehouse' = 's3://data-lake-prod/iceberg',
  'uri' = 'thrift://hive-metastore:9083'
);

-- Paimon catalog for real-time streaming data
CREATE CATALOG prod_paimon
USING paimon
WITH (
  'warehouse' = 's3://data-lake-prod/paimon'
);
```

**Step 2: Create schemas and tables**

```sql
CREATE SCHEMA prod_iceberg.analytics;
CREATE SCHEMA prod_paimon.realtime;

CREATE TABLE prod_iceberg.analytics.dim_customers (
  customer_id VARCHAR,
  name VARCHAR,
  email VARCHAR,
  region VARCHAR,
  tier VARCHAR,
  created_at TIMESTAMP
) PARTITIONED BY (region);

CREATE TABLE prod_paimon.realtime.live_orders (
  order_id BIGINT,
  customer_id VARCHAR,
  amount DECIMAL(10, 2),
  order_time TIMESTAMP,
  status VARCHAR
) PARTITIONED BY (status);
```

**Step 3: Cross-catalog join**

```sql
SELECT
  c.name,
  c.tier,
  COUNT(o.order_id) AS recent_orders,
  SUM(o.amount) AS recent_spend
FROM prod_paimon.realtime.live_orders o
JOIN prod_iceberg.analytics.dim_customers c
  ON o.customer_id = c.customer_id
WHERE o.order_time >= TIMESTAMP '2024-06-01 00:00:00'
GROUP BY c.name, c.tier
ORDER BY recent_spend DESC
LIMIT 20;
```

**Step 4: Table maintenance**

```sql
-- Compact small files in the Iceberg table
COMPACT prod_iceberg.analytics.dim_customers;

-- Clean up old snapshots
VACUUM prod_iceberg.analytics.dim_customers;

-- Check catalog status
SHOW CATALOGS;
DESCRIBE CATALOG prod_iceberg;
DESCRIBE CATALOG prod_paimon;
```

### Connector SPI

Pangrova uses a Connector SPI for pluggable data source integration. Each connector implements `ConnectorFactory`:

| Method | Purpose |
|--------|---------|
| `getName()` | Returns the connector type name (e.g., `iceberg`, `paimon`) |
| `create(catalogName, properties, context)` | Creates a connector instance with the given properties |

The `properties` map in `config/catalog/*.yml` is passed directly to `ConnectorFactory.create()`. This means any property supported by the underlying Iceberg or Paimon library can be set in the YAML file.
