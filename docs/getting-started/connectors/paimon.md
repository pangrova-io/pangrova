---
title: "Paimon"
description: "Apache Paimon connector configuration"
---

# Paimon

Configure the Paimon connector for streaming lakehouse tables.


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
