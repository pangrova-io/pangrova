# ============================================================
# Pangrova — Multi-stage Dockerfile
# Builds a container image containing both Coordinator and Worker.
# Supports Docker Buildx multi-arch: linux/amd64, linux/arm64.
#
# Usage:
#   docker build -t pangrova/pangrova:latest -f deploy/docker/Dockerfile .
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       -t pangrova/pangrova:latest -f deploy/docker/Dockerfile .
# ============================================================

# ----------------------------------------------------------
# Stage 1: builder — compile Java modules
# ----------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS builder

ARG TARGETARCH

WORKDIR /build

# Copy POM files first to cache dependencies
COPY pom.xml ./
COPY pangrova-common/pom.xml pangrova-common/
COPY pangrova-spi/pom.xml pangrova-spi/
COPY pangrova-protocol/pom.xml pangrova-protocol/
COPY pangrova-config/pom.xml pangrova-config/
COPY pangrova-parser/pom.xml pangrova-parser/
COPY pangrova-planner/pom.xml pangrova-planner/
COPY pangrova-metadata/pom.xml pangrova-metadata/
COPY pangrova-indexing/pom.xml pangrova-indexing/
COPY pangrova-coordinator/pom.xml pangrova-coordinator/
COPY pangrova-worker/pom.xml pangrova-worker/
COPY pangrova-worker-native/pom.xml pangrova-worker-native/
COPY pangrova-native-bundle/pom.xml pangrova-native-bundle/
COPY pangrova-shuffle-server/pom.xml pangrova-shuffle-server/
COPY pangrova-client/pom.xml pangrova-client/
COPY pangrova-client/pangrova-client-rest/pom.xml pangrova-client/pangrova-client-rest/
COPY pangrova-client/pangrova-client-jdbc/pom.xml pangrova-client/pangrova-client-jdbc/
COPY pangrova-client/pangrova-client-cli/pom.xml pangrova-client/pangrova-client-cli/
COPY pangrova-connector/pom.xml pangrova-connector/
COPY pangrova-connector/pangrova-connector-iceberg/pom.xml pangrova-connector/pangrova-connector-iceberg/
COPY pangrova-connector/pangrova-connector-paimon/pom.xml pangrova-connector/pangrova-connector-paimon/
COPY pangrova-observability/pom.xml pangrova-observability/
COPY pangrova-governance/pom.xml pangrova-governance/
COPY pangrova-security/pom.xml pangrova-security/
COPY pangrova-resilience/pom.xml pangrova-resilience/
COPY pangrova-streaming/pom.xml pangrova-streaming/
COPY pangrova-operations/pom.xml pangrova-operations/
COPY pangrova-resource/pom.xml pangrova-resource/
COPY pangrova-testing/pom.xml pangrova-testing/
COPY pangrova-testing/pangrova-test-e2e/pom.xml pangrova-testing/pangrova-test-e2e/

RUN mvn dependency:go-offline -B -q || true

# Copy full source and build
COPY . .
RUN mvn clean package -DskipTests -B -q \
    && mkdir -p /dist/lib /dist/bin /dist/config/catalog \
    && find . -path "*/target/*.jar" \
        -not -name "*-sources.jar" \
        -not -name "*-tests.jar" \
        -exec cp {} /dist/lib/ \; \
    && cp -r bin/* /dist/bin/ \
    && cp -r etc/* /dist/config/

# ----------------------------------------------------------
# Stage 2: runtime — JRE only, minimal image
# ----------------------------------------------------------
FROM eclipse-temurin:17-jre

ARG TARGETARCH

LABEL maintainer="Pangrova Contributors" \
      description="Pangrova distributed SQL query engine" \
      org.opencontainers.image.source="https://github.com/pangrova/pangrova"

ENV PANGROVA_HOME=/opt/pangrova \
    PANGROVA_CONF_DIR=/opt/pangrova/config \
    PANGROVA_LOG_DIR=/opt/pangrova/logs

RUN groupadd -r pangrova && useradd -r -g pangrova -d /opt/pangrova pangrova \
    && mkdir -p /opt/pangrova/logs /opt/pangrova/data \
    && apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /dist/lib/ /opt/pangrova/lib/
COPY --from=builder /dist/bin/ /opt/pangrova/bin/
COPY --from=builder /dist/config/ /opt/pangrova/config/

RUN chmod +x /opt/pangrova/bin/*.sh \
    && chown -R pangrova:pangrova /opt/pangrova

USER pangrova
WORKDIR /opt/pangrova

# Default to Coordinator; override with PANGROVA_ROLE=worker
ENV PANGROVA_ROLE=coordinator

EXPOSE 8080 8081

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/v1/health || curl -f http://localhost:8081/v1/health || exit 1

ENTRYPOINT ["/opt/pangrova/bin/pangrova.sh"]
CMD ["start", "coordinator"]
