---
title: "REST Catalog API"
description: "Iceberg-style REST catalog endpoints served by the Coordinator"
---

# REST Catalog API

When the Coordinator runs with the metadata service enabled, it serves an
Iceberg-style REST catalog API under `/v1`. Catalog tooling, notebooks, and external
engines can list, create, and drop namespaces and tables over plain HTTP.

All endpoints produce and consume `application/json`. Identifier characters are
restricted to alphanumerics, underscore, dot, and dash.

:::note Namespace model
The API bridges Iceberg's two-level namespace model with Pangrova's
catalog/schema model: a single-level namespace maps to a **catalog**, and in a
two-level namespace the first level is the catalog and the second is the schema.
Namespace operations (create, load, drop, update properties) act on catalogs.
:::

Base URL:

```
http://<coordinator-host>:8080/v1
```

## Configuration

### GET /v1/config

Returns catalog configuration and defaults for clients.

```bash
curl http://localhost:8080/v1/config
```

## Namespaces

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/namespaces` | List namespaces (supports `parent` query parameter) |
| `POST` | `/v1/namespaces` | Create a namespace |
| `GET` | `/v1/namespaces/{namespace}` | Load namespace metadata |
| `HEAD` | `/v1/namespaces/{namespace}` | Check namespace existence (`204` / `404`) |
| `DELETE` | `/v1/namespaces/{namespace}` | Drop a namespace |
| `POST` | `/v1/namespaces/{namespace}/properties` | Update namespace properties |

**Examples:**

```bash
# List namespaces
curl http://localhost:8080/v1/namespaces

# Create a namespace
curl -X POST http://localhost:8080/v1/namespaces \
  -H 'Content-Type: application/json' \
  -d '{"namespace": ["analytics"], "properties": {"owner": "data-team"}}'

# Check existence
curl -I http://localhost:8080/v1/namespaces/analytics

# Drop
curl -X DELETE http://localhost:8080/v1/namespaces/analytics
```

## Tables

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/namespaces/{namespace}/tables` | List tables in a namespace |
| `POST` | `/v1/namespaces/{namespace}/tables` | Create a table |
| `GET` | `/v1/namespaces/{namespace}/tables/{table}` | Load table metadata |
| `HEAD` | `/v1/namespaces/{namespace}/tables/{table}` | Check table existence (`204` / `404`) |
| `DELETE` | `/v1/namespaces/{namespace}/tables/{table}` | Drop a table |

**Examples:**

```bash
# List tables
curl http://localhost:8080/v1/namespaces/analytics/tables

# Load table metadata
curl http://localhost:8080/v1/namespaces/analytics/tables/events

# Drop a table
curl -X DELETE http://localhost:8080/v1/namespaces/analytics/tables/events
```

## Views

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/namespaces/{namespace}/views` | List views in a namespace |

```bash
curl http://localhost:8080/v1/namespaces/analytics/views
```

## Authentication

When security is enabled, obtain a bearer token from the OAuth2 token endpoint and
pass it on every request:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=<id>&client_secret=<secret>' \
  | jq -r .access_token)

curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/namespaces
```

See [Security Configuration](../../configuration/security.md) for
authentication setup.

## Error Handling

Errors follow the Iceberg REST error model: a JSON body with `error.message`,
`error.type`, and `error.code`. Common status codes:

| Code | Meaning |
|------|---------|
| `400` | Invalid identifier or request body (`BadRequestException`) |
| `401` | Missing or invalid token |
| `403` | Access denied |
| `404` | Namespace or table not found (`NoSuchNamespaceException`, `NoSuchTableException`) |

**See Also:** [REST API](rest-api.md) for query submission,
[Catalog Management](../catalogs/catalog-management.md) for SQL-based catalog registration.
