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

## Iceberg deletion-file compatibility

The Native Iceberg v3 deletion-vector reader accepts Puffin containers with an
uncompressed JSON footer and uncompressed `deletion-vector-v1` blobs. It selects
the blob by the scanned data file's exact location, preserves positive 64-bit row
positions, and validates container framing, blob bounds, payload length, magic,
CRC and bitmap cardinality before applying deletes. A valid SDK-produced vector
with several blobs is covered by the Table Control lifecycle SQL acceptance.

| Encoding | Current behavior |
|---|---|
| Uncompressed Puffin footer; uncompressed DV blob | Supported by the Native reader, subject to the identity and integrity checks above. |
| LZ4-compressed Puffin footer | A valid optional Puffin encoding, currently outside Pangrova's supported DV read format. The read fails explicitly. External writers must use uncompressed footers for this deployment. |
| Compressed `deletion-vector-v1` blob | Rejected. The Iceberg specification requires this blob type to omit `compression-codec`; this is not a compression capability to enable. |
| Unknown footer flags | Rejected before the footer is interpreted. |
| Equality-delete files or equality/position/DV mixtures | Equality-delete read capability remains false; queries reject such inputs until the full equality-delete semantics and reference-engine matrix are qualified. |

These are limits of the Native deletion-vector path. They do not describe the
separate SDK path that reads Puffin statistics. See the
[Puffin footer specification](https://iceberg.apache.org/puffin-spec/#footer-structure)
and [DV blob requirements](https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type).

Release qualification must check the actual external writers' footer encoding
and exercise the supported and rejected cases on the shipped artifact. The
supported format boundary does not qualify Spark/Flink interoperability or the
complete Iceberg v3 feature matrix by itself.

---

### S3 Access Issues

| Error | Cause | Fix |
|-------|-------|-----|
| `Access Denied` | Wrong credentials | Verify `s3.access-key-id` and `s3.secret-access-key` |
| `NoSuchBucket` | Bucket doesn't exist | Create the S3 bucket first |
| `Connection refused` | Wrong endpoint | Verify `s3.endpoint` matches your S3-compatible service |
