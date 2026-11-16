---
title: "Connector"
description: "Iceberg, Paimon, S3, storage backend, and catalog file configuration"
---

# Connector

Connector and catalog configuration keys.


## Connector Configuration

Register connectors through SQL `CREATE CATALOG ... USING ... WITH (...)`.
Per-catalog YAML files in `${PANGROVA_CONF_DIR}/catalog/<name>.yml` describe a
`connector` and a flat `properties` map, but automatic startup registration is
planned. Creating a file does not create a runtime catalog. See
[Catalog Management](../getting-started/catalogs/catalog-management.md) and the
[configuration examples](#catalog-configuration-catalognameyml) below.

---

## Storage Backend Configuration

Storage settings depend on the connector SDK and Native storage implementation.
The provider variables below are configuration references, not evidence that every
provider works across every read/write/AI path. Native reads and governed AI blobs
require the authorized catalog/tenant storage binding; SDK environment defaults alone
do not establish that binding. See [Worker Configuration](worker.md).

For SDK-backed catalog operations, configurations may include:

1. **Environment variables** in `pangrova-env.sh` — global defaults for all catalogs
2. **Per-catalog properties** in `catalog/<name>.yml` — override per data source

### AWS S3

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AWS_REGION` | string | Yes | AWS region (e.g. `us-east-1`) |
| `AWS_ACCESS_KEY_ID` | string | Static auth | S3 access key ID |
| `AWS_SECRET_ACCESS_KEY` | string | Static auth | S3 secret access key |
| `AWS_ROLE_ARN` | string | IAM auth | IAM role ARN for AssumeRole (preferred over static keys) |
| `AWS_SESSION_TOKEN` | string | No | Temporary session token (for STS-based auth) |
| `AWS_ENDPOINT_URL` | string | No | Custom S3 endpoint (for MinIO, Ceph, R2, LocalStack) |
| `s3.endpoint` | string | No | Per-catalog S3 endpoint override |
| `s3.access-key-id` | string | No | Per-catalog access key override |
| `s3.secret-access-key` | string | No | Per-catalog secret key override |
| `s3.region` | string | No | Per-catalog region override |
| `s3.path-style-access` | bool | No | Enable path-style access (required for MinIO/Ceph). Default: `false` |

**Credential resolution is connector- and execution-path-specific.** SDK defaults,
explicit catalog properties, temporary credentials, and instance roles do not have
one engine-wide priority order. Use property names consumed by the selected connector
and verify the authorized Worker storage binding; an environment credential is not
itself authorization to access a catalog or tenant's objects.

### Alibaba Cloud OSS

OSS is S3-compatible. Configure as S3 with a custom endpoint:

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AWS_ACCESS_KEY_ID` or `ALIBABA_CLOUD_ACCESS_KEY_ID` | string | Yes | OSS AccessKey ID |
| `AWS_SECRET_ACCESS_KEY` or `ALIBABA_CLOUD_ACCESS_KEY_SECRET` | string | Yes | OSS AccessKey Secret |
| `AWS_ENDPOINT_URL` | string | Yes | OSS endpoint (e.g. `https://oss-cn-hangzhou.aliyuncs.com`) |
| `s3.endpoint` | string | Yes | Per-catalog OSS endpoint |
| `s3.path-style-access` | bool | Yes | Must be `false` for OSS (virtual-hosted style) |
| `s3.region` | string | Yes | OSS region (e.g. `cn-hangzhou`) |

**OSS endpoint format**: `https://oss-{region}.aliyuncs.com` (public) or `https://oss-{region}-internal.aliyuncs.com` (VPC internal).

### MinIO / S3-Compatible

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | string | Yes | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | string | Yes | MinIO secret key |
| `AWS_ENDPOINT_URL` | string | Yes | MinIO endpoint (e.g. `http://minio:9000`) |
| `s3.path-style-access` | bool | Yes | Must be `true` for MinIO |
| `s3.region` | string | No | Region (default: `us-east-1`; MinIO ignores this) |

### Google Cloud Storage (GCS)

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | string | Yes | Path to GCP service account JSON key file |
| `gcs.project-id` | string | No | GCP project ID (auto-detected from credentials if omitted) |

### Azure ADLS Gen2

| Variable / Property | Type | Required | Description |
|---------------------|------|----------|-------------|
| `AZURE_STORAGE_ACCOUNT` | string | Yes | Storage account name |
| `AZURE_STORAGE_ACCESS_KEY` | string | Key auth | Storage account access key |
| `AZURE_TENANT_ID` | string | SP auth | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | string | SP auth | Service principal client ID |
| `AZURE_CLIENT_SECRET` | string | SP auth | Service principal client secret |

---

## Catalog Configuration (`catalog/<name>.yml`)

Reference files may be kept in `${PANGROVA_CONF_DIR}/catalog/`. Register the desired SQL name with `CREATE CATALOG`; the default startup path does not derive a registered catalog from the filename.

### YAML Structure

All catalog files share the same two-level structure:

```yaml
connector: <connector-type>   # "iceberg", "paimon", or "lance"
properties:
  <key>: <value>
  # ... connector-specific properties
```

- `connector` — selects the ConnectorFactory implementation (discovered via ServiceLoader)
- `properties` — flat key-value map passed directly to the ConnectorFactory

### Iceberg Connector (`connector: iceberg`)

The Iceberg connector supports **hadoop** (default) and **REST** catalog modes.

#### Mode selection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `catalog-type` | string | `"hadoop"` | Catalog implementation: `hadoop` or `rest` |

#### Hadoop mode properties

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `type` | string | `"hadoop"` | No | Iceberg catalog type passed to `CatalogUtil.buildIcebergCatalog()`. Alternatives: `hive`, `glue`, custom `catalog-impl` |
| `warehouse` | string | — | Yes (required for `hadoop`/`jdbc`) | Root storage path for table data and metadata (e.g. `s3://bucket/warehouse`) |
| `catalog-impl` | string | — | No | Fully-qualified class name for a custom Iceberg catalog implementation |

All other properties are passed through to Hadoop Configuration (supporting S3, HDFS, GCS, ADLS settings):

| Property | Type | Description |
|----------|------|-------------|
| `s3.endpoint` | string | S3-compatible endpoint URL |
| `s3.region` | string | AWS/OSS region |
| `s3.access-key-id` | string | Static access key |
| `s3.secret-access-key` | string | Static secret key |
| `s3.path-style-access` | bool | `true` for MinIO/Ceph, `false` for AWS/OSS |
| `fs.s3a.*` | string | Any Hadoop S3A filesystem property |

#### REST mode properties (`catalog-type: rest`)

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `uri` | string | — | Yes | Iceberg REST Catalog server endpoint |
| `warehouse` | string | — | No | Warehouse hint passed to REST catalog |
| `credential` | string | — | No | OAuth2 client credential for REST catalog |
| `oauth2-server-uri` | string | — | No | OAuth2 token endpoint URI |
| `scope` | string | — | No | OAuth2 scope |
| `token` | string | — | No | Pre-existing bearer token |

The current write path rejects Iceberg REST catalogs because its declaration
fence has only been verified with atomic whole-metadata pointer comparison on
JDBC and local Hadoop catalogs. REST table creation and metadata access remain
available, but `INSERT` and other writes require a catalog-specific atomic
declaration fence before they can be enabled safely.
This includes filtered `DELETE`: the connector checks the catalog capability
before publication and fences changes to the selected schema, partition spec,
location, format version, and generated/vector declaration properties. An
accepted filtered `DELETE` records the canonical declaration in its snapshot
summary for historical reads without copying unrelated table properties. An
Iceberg REST server backed by JDBC does not expose the backing JDBC metadata
pointer comparison through the standard REST commit requirements, so the
connector still rejects REST writes through that server.

#### Encryption (both modes)

| Property | Type | Description |
|----------|------|-------------|
| `encryption.accessible-keys` | string | Comma-separated list of encryption key IDs accessible by this catalog |

### Paimon Connector (`connector: paimon`)

The Paimon connector supports **filesystem** (default) and **REST** catalog modes.

#### Mode selection

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `metastore` | string | `"filesystem"` | Catalog implementation: `filesystem` or `rest` |

#### Filesystem mode properties

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `warehouse` | string | `$TMPDIR/paimon-warehouse` | Yes (prod) | Root storage path for Paimon table data |
| `temp-dir` | string | `$TMPDIR/paimon-temp` | No | Temporary directory for Paimon I/O operations |

All other properties are passed through to Paimon Options (supporting S3, HDFS, OSS, etc.).

#### REST mode properties (`metastore: rest`)

| Property | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `uri` | string | — | Yes | Paimon REST Catalog server endpoint |
| `warehouse` | string | — | Yes | Server-side catalog/instance identifier expected by the Paimon REST service, not a local filesystem path |

This endpoint must implement the Paimon REST protocol. An Iceberg REST endpoint,
including Gravitino's Iceberg REST service, is not interchangeable with it.

### Lance Connector (`connector: lance`)

The Lance connector serves managed Lance datasets. The warehouse holds dataset objects and
manifests; catalog identity, tenants, revisions, and publication evidence remain in the shared
metadata authority. The connector rejects `jdbc.url`, `jdbc.user`, and `jdbc.password`
properties and never opens its own database connection.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `warehouse` | string | Yes | Absolute local path, canonical `file:///` URI, or `s3://bucket/prefix` |

The warehouse path must be normalized and unencoded. `file:/`, relative paths, `%` escapes,
query strings, fragments, and user-info are rejected. Reads require the Native Lance FFI;
`CREATE TABLE`, `INSERT`, `INSERT OVERWRITE`, abort, and time travel are supported, while
`UPDATE`/`DELETE`/`MERGE`, column DDL, views, system tables, and maintenance are not.
See [Lance](../getting-started/connectors/lance.md) for the full capability matrix.

```yaml
connector: lance
properties:
  warehouse: "s3://analytics-lance/warehouse"
```

### Example: Iceberg on S3 (hadoop mode)

```yaml
connector: iceberg
properties:
  type: hadoop
  warehouse: "s3://my-bucket/warehouse"
  s3.region: "us-east-1"
  s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
  s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
```

### Example: Iceberg on OSS

```yaml
connector: iceberg
properties:
  type: hadoop
  warehouse: "s3://my-oss-bucket/warehouse"
  s3.endpoint: "https://oss-cn-hangzhou-internal.aliyuncs.com"
  s3.region: "cn-hangzhou"
  s3.path-style-access: "false"
  s3.access-key-id: "${ALIBABA_CLOUD_ACCESS_KEY_ID}"
  s3.secret-access-key: "${ALIBABA_CLOUD_ACCESS_KEY_SECRET}"
```

### Example: Iceberg REST Catalog

```yaml
connector: iceberg
properties:
  catalog-type: rest
  uri: "https://iceberg-rest-catalog.example.com"
  warehouse: "s3://my-bucket/warehouse"
  credential: "${ICEBERG_REST_CREDENTIAL}"
  oauth2-server-uri: "https://auth.example.com/oauth/token"
```

### Example: Paimon on S3

```yaml
connector: paimon
properties:
  warehouse: "s3://my-bucket/paimon-warehouse"
  s3.endpoint: "https://s3.us-east-1.amazonaws.com"
  s3.region: "us-east-1"
  s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
  s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
```
