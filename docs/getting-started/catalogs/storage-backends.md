---
title: "Storage Backends"
description: "S3, HDFS, and local filesystem warehouse configuration"
---

# Storage Backends

Warehouse storage backends supported by both connectors.


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

### S3 Access Issues

| Error | Cause | Fix |
|-------|-------|-----|
| `Access Denied` | Wrong credentials | Verify `s3.access-key-id` and `s3.secret-access-key` |
| `NoSuchBucket` | Bucket doesn't exist | Create the S3 bucket first |
| `Connection refused` | Wrong endpoint | Verify `s3.endpoint` matches your S3-compatible service |
