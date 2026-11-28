---
title: "Docker"
description: "Docker Compose single-node and cluster deployment, image builds, health checks"
---

# Docker

Run Pangrova with Docker Compose for development and testing, and build container images from source.


## Docker — Single Node (Development)

```bash
cd deploy/docker
cp .env.example .env
# Set PANGROVA_DB_PASSWORD and a random >=32-byte PANGROVA_INTERNAL_AUTH_SECRET in .env.
docker compose up -d
docker compose ps
```

Compose starts PostgreSQL, one Coordinator, and one Worker with a small development
configuration mounted read-only from `deploy/docker/config/`. The Coordinator is
available at `http://localhost:8080`; the Worker registers after Coordinator
HTTP startup. The password and internal secret must be supplied in `.env` and
must not be committed. This profile explicitly uses `security.auth.type: NOOP`,
which accepts unauthenticated clients; keep it isolated from untrusted networks.
SQL readiness remains 503 until a catalog is created.

To stop:

```bash
docker compose down
```

## Docker — Distributed Cluster

```bash
cd deploy/docker
docker compose -f docker-compose-cluster.yml up -d
docker compose -f docker-compose-cluster.yml ps
```

This starts PostgreSQL, 2 independent non-HA Coordinators (ports 8080, 8082),
and 3 Workers (ports 8081, 8083, 8084). The development configuration disables
HA: Workers 1 and 2 register with Coordinator 1, Worker 3 with Coordinator 2.
The two endpoints have separate Worker registries and do not provide failover.
Use a shared JDBC state store and the production HA configuration before
claiming failover behavior.
Each Coordinator has a separate admission directory on the persistent
`coordinator-data` volume so a container replacement can prove the former
process exited.

## Docker — Build Image from Source

```bash
# From the project root; Docker Buildx/Bake is required.
docker buildx bake -f deploy/docker/docker-bake.hcl --print
docker buildx bake -f deploy/docker/docker-bake.hcl --load

# Publish both Linux architectures through the same build graph.
PLATFORMS=linux/amd64,linux/arm64 IMAGE=registry.example.com/pangrova:release \
    docker buildx bake -f deploy/docker/docker-bake.hcl --push
```

Bake builds the Native target and Java runtime image from the same source context
and target platform. The Java image receives Native output through a target
context, not a host library or independently selected image tag. Source equality
is checked before packaging. Host `target/`, Native build trees and development
JNI resources are excluded from the Docker context; new source modules are not.
The Native build consumes both Protocol schema trees, the function-contract
resources and their Python generators. Java-only Protocol changes reuse the
same Native layers and rebuild the Java runtime.

A standalone build compiles and tests the locked, patched Iceberg SDK with
Gradle 8.14.4 before resolving the Pangrova Java reactor. CI passes the tested
same-run SDK as a named context and grants Bake read access to that directory.
Both paths work on a fresh builder without a host Maven repository.

Hosted CI passes Maven's strict checksum option and the repository's CI Maven
settings to the Java image build. Those settings use Google Cloud's public
Maven Central mirror for Central artifacts. Standalone Bake invocations use the
Maven image's normal repository settings.

CI passes the Git revision as `SOURCE_COMMIT` and checks the resulting runtime
image’s `org.opencontainers.image.revision` label before staging its libraries.

The Ubuntu build base, Maven build base, and Temurin runtime base are pinned to
image digests. Refresh those digests deliberately as one reviewed dependency
update, then rebuild and rerun the Linux image and startup/SQL acceptance checks
for each published architecture. Updating a floating base tag implicitly can
invalidate the full Native dependency cache and change the produced binary.

Native image builds default to one compilation job. The explicit limit reaches
both Pangrova's dependency setup and the upstream dependency installer, as well
as the final CMake build. CPU count alone is not a safe memory budget. On a
builder with measured sufficient memory, override the same limit explicitly:

```bash
docker buildx bake -f deploy/docker/docker-bake.hcl native-build \
    --set native-build.args.NATIVE_BUILD_JOBS=2
```

The setup script rejects non-positive or malformed limits. For a non-container
build, set `CMAKE_BUILD_PARALLEL_LEVEL`; `BUILD_THREADS` is consulted only when
that variable is absent. Increasing parallelism requires measuring peak builder
memory, including resident services; it does not change runtime query admission.
GitHub's production regression uses two build jobs on its 16 GiB Linux runner.

Linux production enables `PANGROVA_PRODUCTION_BUILD=ON`: Arrow, Parquet, Flight
and other third-party dependencies must be static and position-independent. The
packaging check verifies the Lance FFI symbol, ELF architecture, absence of
build-tree RPATH/RUNPATH and a strict system-ABI-only `DT_NEEDED` list. It rejects
missing archives or third-party shared-library dependencies rather than copying
them into the image. The runtime uses the same Ubuntu 22.04 ABI baseline.
The production Native link takes pinned static curl and libunwind from the
build dependency prefix. Velox's Boost build and Pangrova's curl build enable
PIC explicitly. Both builders verify the archives with shared-library links
that reject text relocations. The libunwind preflight links both archives in
a group so the linker can resolve their cross-archive references.
The builder also rebuilds bzip2, Zstandard, libevent, ICU and libxml2 with
PIC. It obtains the exact installed packages' source versions from Ubuntu's
signed repositories and retains their distribution patches. A shared-library
link checks those archives with the JNI export map before Velox is compiled.
The Linux ARM64 build requires CPUs with ARM CRC32 instructions: the pinned
Velox and Folly archives use F14's SIMD+CRC mode, and Pangrova compiles their
header consumers for the same mode. Deploy `linux/arm64` images only to nodes
with that instruction set.

The Linux classifier JAR is built and installed explicitly, then Java is built
with `native-runtime`. Runtime dependencies are copied from the actual Maven
dependency graph, including the Iceberg, Paimon and Lance providers and JDBC drivers.
Each packaged role requires its ordered `lib/<role>/classpath.list`. Staging checks
the process entry point and role-specific class and native library exclusions inside
the JARs, including multi-release entries. The Coordinator
classpath excludes Worker Core and its JNI bundle. The Worker classpath excludes
the planner and Coordinator. The flat JAR inventory under `lib/` is the
Standalone closure used for frozen-artifact regression. Native loading
remains classpath resource extraction followed by `System.load()`; no environment
library path or host JNI fallback is used.
The Coordinator and Worker include the SLF4J 2 Logback provider in the runtime
classpath, so startup failures and process logs remain visible. The image omits
Hadoop's legacy Jersey 1 server stack, whose shaded classes conflict with the
Jersey 4 HTTP server, while retaining the Hadoop client libraries. Both process
entry points remain in the foreground until their owned services shut down.

`--print` validates only the Bake graph. Release acceptance also requires the
actual Linux image build, Coordinator/Worker startup and SQL-through-JNI container
E2E; a host Maven test is not a substitute. A static or ABI gate failure blocks
the image until the dependency is rebuilt correctly.

---

## Configuration

All services read configuration from `/opt/pangrova/config/` inside the container. Override via:

- Environment variables used by YAML placeholders (`PANGROVA_COORDINATOR_URL`,
  `PANGROVA_ADVERTISED_HOST`, `PANGROVA_INTERNAL_AUTH_SECRET`, etc.)
- Helm `values.yaml` — `config.coordinator` and `config.worker` embed full YAML content
- Kubernetes ConfigMap — mounted as `coordinator.yml` / `worker.yml`

See the [Configuration Reference](../configuration/reference.md) for the complete configuration reference.

## Health Checks

All services expose `GET /v1/health` for readiness and `GET /v1/health/live`
for liveness. Coordinator readiness requires its HTTP server, a registered
Worker, and a catalog; it can return 503 while its live endpoint returns 200.
Compose and the image healthcheck use liveness so Workers can start and register.
After registration, each Worker sends its initialized resource heartbeat immediately;
the scheduler admits tasks only after receiving enough reported free memory.
Coordinator readiness confirms a registered Worker, while task admission also
checks its current resource report.
The image detects the running node role and probes its default management port
(8080 for Coordinator, 8081 for Worker). If a mounted configuration changes
that port, set `PANGROVA_HEALTH_PORT` to the same value. Both health routes are
accessible without authentication, including when a client IP is locked out.
Workers must have an existing writable `worker.shuffle.service.spill-directory`
with capacity for their configured spill quota and headroom. The Compose
configuration points it at the Worker data volume. The production
`etc/worker.yml` defaults to the explicitly provisioned `/mnt/spill/pangrova`
mount, with `PANGROVA_SHUFFLE_SERVICE_SPILL_DIRECTORY` for override.

| Component   | Default Port | Readiness | Liveness |
|-------------|-------------|-----------|----------|
| Coordinator | 8080 | `/v1/health` | `/v1/health/live` |
| Worker | 8081 | `/v1/health` | `/v1/health/live` |

## Image Versioning

Docker Compose files use `${PANGROVA_VERSION:-latest}`. Override via:

```bash
PANGROVA_VERSION=0.1.0 docker compose up -d
```

Helm chart uses `image.tag` in `values.yaml`.
