# syntax=docker/dockerfile:1
# The canonical Bake graph binds this context to the matching Native target.
FROM native_build AS native-release
WORKDIR /workspace
COPY pangrova-worker-native/ /expected/pangrova-worker-native/
COPY pangrova-protocol/ /expected/pangrova-protocol/
COPY deploy/docker/verify-native-source.sh /verify-native-source.sh
RUN bash /verify-native-source.sh /expected /workspace \
    && pangrova-worker-native/bin/package-native-lib.sh --production \
        /workspace/pangrova-worker-native/build/libpangrova_native.so

FROM maven:3.9-eclipse-temurin-17 AS builder
ARG TARGETARCH
WORKDIR /build
# The complete reactor defines dependencies; there is no stale manual POM cache inventory.
COPY . .
COPY --from=native-release /workspace/pangrova-worker-native/build/libpangrova_native.so /native/libpangrova_native.so
RUN --mount=type=cache,target=/root/.m2 bash -euo pipefail -c '\
    case "$TARGETARCH" in \
        amd64) classifier=linux-x86_64; resource_arch=x86_64 ;; \
        arm64) classifier=linux-aarch_64; resource_arch=aarch64 ;; \
        *) echo "Unsupported Linux architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    mvn -B -q -pl pangrova-native-bundle -am package -DskipTests \
        -Dnative.platform.classifier="$classifier" \
        -Dnative.resource.dir=/native -Dnative.resource.os=linux \
        -Dnative.resource.arch="$resource_arch" -Dnative.library.path=/native/libpangrova_native.so; \
    bundles=(pangrova-native-bundle/target/pangrova-native-bundle-*-$classifier.jar); \
    test "${#bundles[@]}" -eq 1; bundle="${bundles[0]}"; test -f "$bundle"; \
    jar tf "$bundle" | grep -Fx "META-INF/native/linux/$resource_arch/libpangrova_native.so"; \
    mvn -B -q install:install-file -Dfile="$bundle" -DpomFile=pangrova-native-bundle/pom.xml \
        -Dclassifier="$classifier"; \
    mvn -B -q -Pnative-runtime -DskipTests \
        -pl pangrova-standalone,pangrova-client/pangrova-client-cli,pangrova-connector/pangrova-connector-iceberg,pangrova-connector/pangrova-connector-paimon,pangrova-connector/pangrova-connector-lance,\!pangrova-native-bundle \
        -am install; \
    mkdir -p /dist/lib /dist/bin /dist/config/catalog; \
    mvn -B -q -Pnative-runtime \
        -pl pangrova-standalone,pangrova-coordinator,pangrova-worker,pangrova-client/pangrova-client-cli,pangrova-connector/pangrova-connector-iceberg,pangrova-connector/pangrova-connector-paimon,pangrova-connector/pangrova-connector-lance \
        dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/dist/lib; \
    for module in pangrova-standalone pangrova-coordinator pangrova-worker \
        pangrova-client/pangrova-client-cli pangrova-connector/pangrova-connector-iceberg \
        pangrova-connector/pangrova-connector-paimon pangrova-connector/pangrova-connector-lance; do \
        final_name="$(mvn -B -q -pl "$module" help:evaluate \
            -Dexpression=project.build.finalName -DforceStdout)"; \
        [[ "$final_name" =~ ^[A-Za-z0-9._-]+$ ]] || { \
            echo "Invalid Maven principal artifact name for $module: $final_name" >&2; exit 1; }; \
        principal="$module/target/$final_name.jar"; test -f "$principal" || { \
            echo "Missing Maven principal release JAR: $principal" >&2; exit 1; }; \
        cp "$principal" /dist/lib/; \
    done; \
    test -f "/dist/lib/$(basename "$bundle")"; \
    lance_jars=(/dist/lib/pangrova-connector-lance-*.jar); \
    test "${#lance_jars[@]}" -eq 1; \
    provider_check="$(mktemp -d)"; \
    (cd "$provider_check"; jar --extract --file "${lance_jars[0]}" \
        META-INF/services/org.pangrova.spi.connector.ConnectorFactory; \
        grep -Fx org.pangrova.connector.lance.LanceConnectorFactory \
            META-INF/services/org.pangrova.spi.connector.ConnectorFactory); \
    rm -r "$provider_check"; \
    cp -r bin/. /dist/bin/; cp -r etc/. /dist/config/'

# The system ABI baseline matches the native Ubuntu 22.04 build.
FROM eclipse-temurin:17-jre-jammy
ARG SOURCE_COMMIT
LABEL description="Pangrova distributed SQL query engine"
LABEL org.opencontainers.image.revision="${SOURCE_COMMIT}"
ENV PANGROVA_HOME=/opt/pangrova \
    PANGROVA_CONF_DIR=/opt/pangrova/config \
    PANGROVA_LOG_DIR=/opt/pangrova/logs
RUN groupadd -r pangrova && useradd -r -g pangrova -d /opt/pangrova pangrova \
    && mkdir -p /opt/pangrova/logs /opt/pangrova/data \
    && apt-get update && apt-get install -y --no-install-recommends curl libstdc++6 libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /dist/ /opt/pangrova/
RUN chmod +x /opt/pangrova/bin/*.sh && chown -R pangrova:pangrova /opt/pangrova
USER pangrova
WORKDIR /opt/pangrova
EXPOSE 8080 8081
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/v1/health || curl -f http://localhost:8081/v1/health || exit 1
ENTRYPOINT ["/opt/pangrova/bin/pangrova.sh"]
CMD ["start", "coordinator"]
