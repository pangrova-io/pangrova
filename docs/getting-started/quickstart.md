---
title: "Quickstart"
description: "Build Pangrova from source and run a first query"
---

# Quickstart

This guide walks you through building Pangrova from source, launching a standalone instance, and running your first query. A first native build also compiles external dependencies;
its duration and resource needs depend on the platform and available caches.

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| JDK | 17 for the supported build chain |
| Maven | 3.9+ |
| Gradle | 8.14.4 for the patched Iceberg SDK |
| Python | 3.10+ for contract generation |
| OS | Linux (x86_64/aarch64) or macOS (aarch64) |
| Native toolchain | CMake, Ninja, and a C++20 compiler |
| Capacity | Allow space for native dependency sources, builds, Maven and Cargo caches; size JVM/native memory for your host |

## Build from Source

```bash
git clone https://github.com/pangrova-io/pangrova.git
cd pangrova
.pangrova/scripts/build-iceberg-sdk-candidate.sh
./pangrova-worker-native/bin/build-native.sh
mvn clean package -DskipTests
```

The build produces JAR files in each module's `target/` directory.

The Iceberg script requires JDK 17 and Gradle 8.14.4 (`GRADLE_BIN` can select its executable)
and publishes the pinned patched SDK to Maven Local. The native build runs prerequisite
setup, builds Lance and packages JNI resources. If the build fails, check:

| Symptom | Fix |
|---------|-----|
| `javac: invalid target release: 17` | Install JDK 17+ and set `JAVA_HOME` |
| `Could not resolve dependencies` | Build the pinned Iceberg SDK first, then check network/proxy and the failing dependency |
| Native build failure | Check the native prerequisites in the [Developer Guide](../contributing/developer-guide.md); Workers require the packaged native library |

### Verify the Build

```bash
# Check that key JARs were produced
ls pangrova-coordination/pangrova-coordination-control/target/pangrova-coordination-control-*.jar
ls pangrova-server/pangrova-server-worker/target/pangrova-server-worker-*.jar
ls pangrova-client/pangrova-client-cli/target/pangrova-client-cli-*-exec.jar
```

The CLI's `-exec.jar` is the standalone executable used by `bin/pangrova-cli.sh`.
The JAR without a classifier contains only CLI classes and remains the ordinary
Maven dependency. Distributions place the executable in `cli/`, separate from the
services' shared `lib/` classpath, so embedded client dependencies cannot override
the services' protocol classes.

## Start Standalone Mode

Standalone mode runs both Coordinator and Worker in a single JVM — ideal for development and testing.

### Configure the Runtime

The shipped `etc/standalone.yml` is a reference template, not a zero-configuration
runnable profile. Copy it to a local file and supply the deployment prerequisites:

| Setting | Required preparation |
|---|---|
| `coordinator.internal-auth-secret` / `worker.internal-auth-secret` | The same secret of at least 32 UTF-8 bytes, supplied through explicit YAML environment placeholders |
| `metadata.jdbc-url`, `metadata.jdbc-username`, `metadata.jdbc-password` | A reachable PostgreSQL authority and an account allowed to initialize its schema |
| `security.enabled`, `security.auth.type` | Configure authentication and provision the required authority; `NOOP` accepts unauthenticated users and is only for isolated development |
| `tenant.enabled` | Keep `false` with `NOOP`; multi-tenancy requires a real authenticator and provisioned membership |
| `worker.advertised-host` | A concrete non-loopback address reachable by the Coordinator and client |
| `worker.shuffle.service.spill-directory` | An existing, writable absolute directory with space for the configured quota and headroom |
| `indexing.enabled` | Disable for a first relational query, or supply the required `-Dpangrova.index.basePath` and index runtime prerequisites |

See [Coordinator configuration](../configuration/coordinator.md),
[Worker configuration](../configuration/worker.md), and
[Security configuration](../configuration/security.md) for the complete settings.
The production JVM configuration is loaded by default and currently requests a
16 GiB heap; do not mistake the launcher's smaller fallback for the loaded value.
For a smaller development host, explicitly size the JVM, leaving room for Native
execution and the database:

```bash
export JAVA_HOME=/path/to/jdk-17
export PANGROVA_JAVA_OPTS="-Xms1g -Xmx4g -XX:MaxDirectMemorySize=2g --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
```

These illustrative JVM limits are not a whole-host memory guarantee. The
standalone launcher does not append the Arrow module-access flags automatically,
so retain them when overriding its JVM options.

### Launch

Pass the configured copy explicitly:

```bash
bin/pangrova-standalone.sh --config /path/to/standalone-local.yml
```

You should see output like:

```
Starting Pangrova Standalone (Coordinator + Worker)...
  PANGROVA_HOME:     /path/to/pangrova
  PANGROVA_CONF_DIR: /path/to/pangrova/etc
  JAVA:            /path/to/jdk-17/bin/java (version 17)
  JVM_CONFIG:      /path/to/pangrova/etc/jvm-coordinator.config (loaded)
```

The Coordinator listens on port **8080** by default. Check HTTP liveness first:

```bash
curl -fsS http://localhost:8080/v1/health/live
# Output: {"live":true}
```

After a Worker has registered and a catalog has been created, check readiness:

```bash
bin/pangrova-health-check.sh localhost:8080
```

The `/v1/health` readiness endpoint can return HTTP 503 before those
prerequisites are met. A fresh instance without a catalog can be live but not
ready. Liveness alone does not prove Native execution, authorization, or a
successful SQL query.

### Using a Custom Configuration

```bash
bin/pangrova-standalone.sh --config /path/to/standalone.yml
```

The standalone config must contain both `coordinator.*` and `worker.*` sections. See `etc/standalone.yml` for the template.

### Running as a Daemon

```bash
bin/pangrova-standalone.sh --config /path/to/standalone-local.yml --daemon
# PID written to logs/standalone.pid
```

Check the daemon log:

```bash
tail -f logs/standalone.out
```

### Common Startup Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ERROR: Java 17+ required` | Wrong JDK version | Set `JAVA_HOME` to JDK 17+ |
| `ERROR: No packaged standalone runtime` | Packaged application or resolved runtime classpath is missing | Run `mvn clean package -DskipTests` after building native prerequisites |
| `ERROR: Standalone already running` | Previous instance still alive | Run `bin/pangrova-stop.sh --all` |
| `Port 8080 already in use` | Another process on port 8080 | `lsof -i :8080` to find it, or change port in `etc/standalone.yml` |

## Run Your First Query

### Using the CLI

```bash
bin/pangrova-cli.sh --host localhost --port 8080
```

At the Pangrova prompt:

```sql
SELECT 1 + 1 AS result;
```

### Using curl (REST API)

```bash
curl -X POST http://localhost:8080/v1/query/submit \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT 1 + 1 AS result"}'
```

The response includes a `query_id` and `state`. See [REST API](clients/rest-api.md) for the full REST API reference.

## Stop the Server

```bash
bin/pangrova-stop.sh --all
```

## Run the Iceberg Trial in One Command

After the [source build](#build-from-source) and [standalone startup](#start-standalone-mode),
run from the checkout root with Python 3.10+:

```bash
python3 examples/iceberg-smoke.py --local-standalone-port 8080
```

Use the port of a **disposable, host-local standalone instance**, with one
active Worker and no catalogs. Run it on an isolated development machine;
the script does not start, reconfigure or stop Pangrova. In particular,
restrict the Java/Flight listeners to a trusted environment before starting
the development instance; a loopback CLI connection does not isolate the server.
Do not use a port forwarded to a container, shared cluster or production service.

The script uses a unique catalog and temporary local warehouse. It checks each
batch SQL result receipt, requires both aggregate rows, and drops only the
objects it confirmed it created. Expect
`Iceberg trial passed: click=2, purchase=1`. If a catalog change or cleanup
cannot be confirmed, the script reports the generated catalog name and keeps
the diagnostic files for inspection. This is a smoke check, not an installer,
distributed correctness test or production qualification.

## End-to-End Example: Query Iceberg Data

Run this example in the CLI against a **local standalone instance** as described
above. It uses three sample rows, a local warehouse, and no cloud credentials.
Use an isolated development instance: the example creates and later drops a
catalog, schema, and table. From the checkout root, you can run the
[SQL fixture](https://github.com/pangrova-io/pangrova/blob/main/examples/iceberg-smoke.sql)
in one pass:

```bash
bin/pangrova-cli.sh --host localhost --port 8080 < examples/iceberg-smoke.sql
```

Check the output for errors and the two expected rows below. Piped input runs
the interactive CLI, which continues after a statement error, so its exit
status alone is not evidence of success. For step-by-step execution, run the
following SQL blocks in order instead.

### Register a Local Iceberg Catalog

```sql
-- In the CLI
CREATE CATALOG local_iceberg
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = 'file:///tmp/iceberg-warehouse'
);
```

### Create a Table and Insert Data

```sql
CREATE SCHEMA local_iceberg.demo;

CREATE TABLE local_iceberg.demo.events (
  event_id BIGINT,
  event_type VARCHAR,
  user_id VARCHAR,
  event_time TIMESTAMP,
  payload VARCHAR
) PARTITIONED BY (event_type);

INSERT INTO local_iceberg.demo.events VALUES
  (1, 'click', 'user-001', TIMESTAMP '2024-01-15 10:30:00', '{"page": "/home"}'),
  (2, 'purchase', 'user-002', TIMESTAMP '2024-01-15 11:00:00', '{"item": "widget"}'),
  (3, 'click', 'user-001', TIMESTAMP '2024-01-15 11:30:00', '{"page": "/products"}');
```

### Query with Aggregation

```sql
SELECT event_type, COUNT(*) AS cnt
FROM local_iceberg.demo.events
GROUP BY event_type
ORDER BY cnt DESC;
```

Expected result (column formatting depends on the client):

| event_type | cnt |
|------------|----:|
| click      |   2 |
| purchase   |   1 |

This is a basic standalone SQL check, not a benchmark or a test of distributed
writes, vector retrieval, or access controls. You can skip straight to
[cleanup](#cleanup) after checking the result.

### Time Travel

```sql
-- SYSTEM_TIME currently resolves a numeric snapshot identifier.
SELECT * FROM local_iceberg.demo.events
FOR SYSTEM_TIME AS OF 4827391056;
```

### Table Maintenance

```sql
-- Compact small files for better read performance
COMPACT local_iceberg.demo.events;

-- Clean up orphaned files (VACUUM does not expire snapshots)
VACUUM local_iceberg.demo.events;
```

### Cleanup

```sql
DROP TABLE local_iceberg.demo.events;
DROP SCHEMA local_iceberg.demo;
DROP CATALOG local_iceberg;
```

## Docker Quick Start

The Docker Compose profile is for isolated development only: it publishes
service ports and uses a development authentication mode that accepts
unauthenticated clients. Do not expose it to untrusted networks. For the local
warehouse SQL example above, use standalone mode.

Follow [Docker deployment](../deployment/docker.md) to prepare the image and
required database password/internal secret before starting Compose. From the
repository root, prepare a local environment file:

```bash
cd deploy/docker
if [ ! -e .env ]; then
  cp .env.example .env
fi
```

Set a unique `PANGROVA_DB_PASSWORD` and a random
`PANGROVA_INTERNAL_AUTH_SECRET` of at least 32 bytes in `.env` (for example,
generate the secret with `openssl rand -base64 48`). Do not commit the populated
file.

Before starting Compose, confirm you have a locally built or pullable Pangrova
image for your platform, matching `pangrova/pangrova:$PANGROVA_VERSION`. Set
`PANGROVA_VERSION` in `.env` to that exact tag; do not assume `latest` is
available. To build and load a local image, follow the Buildx/Bake instructions
in the [Docker deployment guide](../deployment/docker.md), using the same tag
for `IMAGE`. If a compatible image is not available, use the standalone build
above.

```bash
docker compose config --quiet
docker compose up -d
docker compose ps
```

Check Coordinator liveness (replace `8080` if you changed `COORDINATOR_PORT`):

```bash
curl -f http://localhost:8080/v1/health/live
```

Liveness confirms the process responds. SQL readiness is checked separately at
`/v1/health` and can return HTTP 503 until a catalog is configured. See the
[Docker deployment guide](../deployment/docker.md) for configuration and
troubleshooting.

Stop:

```bash
docker compose down
```

---

## Distribution Layout

When Pangrova is packaged as a binary distribution, the directory structure is:

![pangrova-{version}/](../../website/static/img/diagrams/generated/quickstart-23-f2bcbe9fac.svg)

| Directory | Purpose |
|-----------|---------|
| `bin/` | All executable scripts for starting, stopping, and managing the cluster |
| `config/` | Configuration templates; copy and modify for your environment |
| `lib/` | Application JAR files loaded onto the classpath at startup |
| `logs/` | Runtime log files and `.pid` files for daemon processes |
| `data/` | Local storage for spill-to-disk, native engine cache, and temporary data |

## Next Steps

- [SQL Reference](../sql-reference/queries.md) — Learn the full SQL syntax
- [Clients](clients/jdbc.md) — Connect via JDBC, REST, or CLI
- [Deployment & Operations](../operations/index.md) — Deploy in distributed or Kubernetes environments
