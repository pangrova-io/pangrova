---
title: "Security"
description: "Authentication and internal authentication secret configuration"
---

# Security

The security configuration surface consists of a feature flag, the
authenticator type, and the shared internal authentication secret. All keys
below are verified against `CoordinatorOptions`, `WorkerOptions`, and the
coordinator `SecurityBootstrap`.

## Enabling Security

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `security.enabled` | bool | `false` | Enable the security module. When `true`, `security.auth.type` must be configured or the coordinator fails startup |
| `security.auth.type` | string | — | ID of an installed authenticator factory. The distribution includes `OAUTH2`, `LDAP`, and the development-only `NOOP` |

### Startup behavior

- `security.enabled=true` without `security.auth.type` → startup fails with
  `CONFIG_INVALID_VALUE`.
- `security.auth.type=NOOP` accepts anonymous client requests and logs a
  `CRITICAL` security alarm. Tenant isolation and privilege administration require
  an authenticated deployment. Internal control endpoints verify their own secret.
- `security.enabled=false` → the startup banner prints a
  `SECURITY DISABLED` warning.

The Coordinator resolves `security.auth.type` through Java service discovery on
its process classpath. A provider registers its
`org.pangrova.security.api.authentication.AuthenticatorFactory` in
`META-INF/services`, declares a unique uppercase ID, API version and accepted
YAML properties, and creates a fresh authenticator on startup. Duplicate IDs,
incompatible factories, missing providers and unknown provider properties fail
configuration. Only the reserved `NOOP` ID can enable authentication bypass.
The Coordinator closes the selected authenticator on shutdown or initialization
failure.

## Stable row-policy ownership

Production row-policy reads and saves resolve the current controlled table ID.
`row_policy.table_id` is the owner; catalog, schema and table names are diagnostic.
A table rename keeps its policies without rewriting them. A different table created
under the old name starts with a different identity and does not inherit them.
Updates use `(table_id, policy_name)` and retain the database-issued policy ID;
deleting a captured policy ID cannot delete another table's policy.

V100 preserves historical name-only rows with a NULL table ID. These rows are not
automatically adopted from a current name: enabled unbound policies reject reads
until an administrator verifies their intended owner and explicitly rebinds or
retires them. An unproven binding must not be repaired by assigning every old row
the ID currently found at its name. The same fail-closed identity projection runs
inside the existing authorization transaction for background artifact publication.
Catalog replacement and pending rename also reject reads.

## Internal Authentication (Coordinator ↔ Worker)

Coordinator-to-worker control calls carry an `X-Internal-Auth` header verified
against a shared secret on each worker.
Worker registration, heartbeat and task reports authenticate to the Coordinator
with the same secret as a Bearer token; it does not grant client SQL permissions.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `coordinator.internal-auth-secret` | string | — | Shared internal secret; the production Coordinator requires a nonblank value of at least 32 UTF-8 bytes even when client authentication is disabled |
| `worker.internal-auth-secret` | string | — | Shared secret for the `X-Internal-Auth` header. Must be identical on all nodes |

The production Coordinator and Worker YAML templates bind the environment variable
`PANGROVA_INTERNAL_AUTH_SECRET` through an explicit placeholder. The generic
underscore-to-dot override mapping does not map this variable to either hyphenated
secret key. Generate a strong secret, for example:

```bash
openssl rand -base64 48
```

## Example

```yaml
# coordinator.yml
coordinator:
  internal-auth-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"
security:
  enabled: true
  auth:
    type: OAUTH2
```

```yaml
# worker.yml
worker:
  internal-auth-secret: "${PANGROVA_INTERNAL_AUTH_SECRET}"
```

**See Also:** [Coordinator](coordinator.md), [Worker](worker.md),
[Configuration model](index.md)
