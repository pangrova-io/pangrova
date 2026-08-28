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

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `worker.internal-auth-secret` | string | — | Shared secret for the `X-Internal-Auth` header. Must be identical on all nodes |

The secret can also be supplied through the environment variable
`PANGROVA_INTERNAL_AUTH_SECRET` (see [Configuration model](index.md) for the `PANGROVA_*`
override mechanism). Generate a strong secret, for example:

```bash
openssl rand -base64 48
```

## Example

```yaml
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
