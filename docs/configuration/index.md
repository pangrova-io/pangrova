---
title: "Configuration"
description: "Configuration syntax, priority, environment variables, and validation"
---

# Configuration

How Pangrova configuration is structured, layered, and validated.


## Configuration Syntax

Coordinator and worker processes read a single YAML file (passed via
`--config <path>` on the startup scripts in `bin/`). Nested YAML keys are
flattened to dot-separated configuration keys:

```yaml
coordinator:
  port: 8080        # → coordinator.port
  query:
    timeout: "5m"   # → coordinator.query.timeout
```

All YAML values support environment variable substitution:

```yaml
# Required variable — startup fails if not set
metadata:
  jdbc-url: "jdbc:postgresql://${PANGROVA_DB_HOST}:5432/pangrova_meta"

# Optional variable with fallback default
coordinator:
  id: "${HOSTNAME:-coordinator-0}"
```

Duration values use human-readable suffixes: `500ms`, `30s`, `5m`, `1h`, `2d`
(ISO-8601 `PT...` form is also accepted; a bare number is interpreted as
milliseconds).

Byte sizes are expressed as plain numbers (e.g. `536870912`).

### Supported Value Types

Typed `ConfigOption<T>` definitions live in the `pangrova-config` module
(`CoordinatorOptions`, `WorkerOptions`, `CommonOptions`). Supported types:
`string`, `int`, `long`, `boolean`, `double`, `duration`, and `enum`.

---

## Configuration Priority

Values are resolved in the following order (highest priority first):

```
1. PANGROVA_* environment variables
2. CLI YAML file (--config path/to/file.yml)
3. Classpath default YAML bundled in pangrova-config (coordinator.yml / worker.yml)
4. ConfigOption.defaultValue()
```

### PANGROVA_* Environment Variable Overrides

Any environment variable starting with `PANGROVA_` overrides the matching
configuration key:

1. Strip the `PANGROVA_` prefix
2. Replace `__` (double underscore) with a literal `_`
3. Replace remaining `_` with `.`
4. Lowercase the result

Example: `PANGROVA_COORDINATOR_PORT=9090` overrides `coordinator.port`.

---

## Configuration Validation

At startup, required options and numeric ranges are validated by
`ConfigValidator` (typed constraints: required, int range, long range).
Violations produce a startup error that names the offending key.

Additionally, `security.auth.type` must be set whenever `security.enabled=true`,
and worker registration requires `worker.coordinator-url`.

---

## Environment Variables (bin/ scripts)

Environment variables understood by the launch scripts in `bin/`:

| Variable | Default | Description |
|----------|---------|-------------|
| `PANGROVA_HOME` | (auto-detected from script location) | Installation root |
| `PANGROVA_CONF_DIR` | `$PANGROVA_HOME/etc` | Configuration directory |
| `PANGROVA_LOG_DIR` | `$PANGROVA_HOME/logs` | Log and PID file directory |
| `PANGROVA_JAVA_OPTS` | — | Override all JVM options |
| `PANGROVA_ROLE` | — | Node role for the unified `pangrova.sh` launcher |
| `PANGROVA_COORDINATOR_URL` | — | Coordinator URL (worker registration, CLI) |
| `JAVA_HOME` | (system) | Path to a JDK 17+ installation |

Any `PANGROVA_*` variable not listed above is interpreted as a configuration
override per the rules above (e.g. `PANGROVA_COORDINATOR_PORT`).

The external shuffle service is a separate process with its own strict YAML schema; it does
not merge the Coordinator or Worker files and does not use the `PANGROVA_*` dot-path
overrides. See [External Shuffle Service](shuffle-service.md).
