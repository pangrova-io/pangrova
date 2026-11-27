---
title: "Catalog Management"
description: "Register, inspect, and drop catalogs via SQL, files, and the CLI utility"
---

# Catalog Management

Register catalogs through SQL at runtime. YAML files provide configuration references and input to the catalog utility; automatic startup registration remains planned.


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

### Configuration Files (Automatic Startup Loading Planned)

In a source checkout, keep reference YAML files in `etc/catalog/` (or
`$PANGROVA_CONF_DIR/catalog/` when overriding the configuration directory), one file per intended catalog. File creation alone does not register that catalog with the running Coordinator.

```bash
# Copy from template
cp etc/catalog/iceberg-example.yml etc/catalog/prod-iceberg.yml

# Edit the configuration
vi etc/catalog/prod-iceberg.yml
```

> **Note**: File-based catalog auto-registration at startup is 🔮 Planned. Currently, `etc/catalog/` files serve as configuration references and are used by `bin/pangrova-catalog.sh list`.

### Catalog Management Utility

```bash
# List configured catalogs
bin/pangrova-catalog.sh list

# List the schemas in a running Coordinator's registered catalog.
bin/pangrova-catalog.sh test my-iceberg \
  --coordinator-url https://coordinator.example.com:8080 --token-env PANGROVA_TOKEN
```

The probe uses the packaged SQL CLI to run `SHOW SCHEMAS` with the supplied
credentials. It exits nonzero if the catalog is unknown, the caller lacks
access, or the query fails.

---

### Catalog Registration Fails

| Error | Cause | Fix |
|-------|-------|-----|
| `ConnectorFactory not found: iceberg` | Missing connector JAR | Verify `pangrova-connector-iceberg-*.jar` is in `lib/` |
| `Cannot create warehouse directory` | Permission denied on warehouse path | Check filesystem permissions or S3 credentials |
| `Unable to connect to Hive Metastore` | Wrong URI or metastore down | Verify `uri` property and metastore availability |

---

### Query Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `Table not found` | Wrong catalog/schema path | Use fully-qualified name: `catalog.schema.table` |
| `Schema mismatch` | Column type changed | Check schema evolution history with `DESCRIBE TABLE` |

---

### Connector SPI

Pangrova uses a Connector SPI for pluggable data source integration. Each connector implements `ConnectorFactory`:

| Method | Purpose |
|--------|---------|
| `getName()` | Returns the connector type name (e.g., `iceberg`, `paimon`) |
| `apiVersion()` | Returns the API version compiled into the provider |
| `roles()` | Declares the process roles supported by the connector |
| `validateConfiguration(properties)` | Checks connector-owned settings before opening backend resources |
| `create(catalogName, properties, context)` | Creates a connector instance with the given properties |

The `properties` map in `etc/catalog/*.yml` is passed directly to `ConnectorFactory.create()`. This means any property supported by the underlying Iceberg or Paimon library can be set in the YAML file.
