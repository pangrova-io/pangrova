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
| `security.auth.type` | string | — | Authenticator type: `OAUTH2` or `LDAP` for production, `NOOP` for development only |

### Startup behavior

- `security.enabled=true` without `security.auth.type` → startup fails with
  `CONFIG_MISSING_KEY`.
- `security.auth.type=NOOP` → all requests are accepted; the coordinator logs
  a `CRITICAL` security alarm. Development use only.
- `security.enabled=false` → the startup banner prints a
  `SECURITY DISABLED` warning.

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
