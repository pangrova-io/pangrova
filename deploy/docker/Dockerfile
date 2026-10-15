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
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && python3 -c 'import sys; assert sys.version_info >= (3, 10), sys.version' \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
# The complete reactor defines dependencies; there is no stale manual POM cache inventory.
COPY . .
COPY --from=native-release /workspace/pangrova-worker-native/build/libpangrova_native.so /native/libpangrova_native.so
# Hadoop's Jersey 1 server contains shaded ASM classes also present in Jersey 4.
# Keep its client/core dependencies, but omit the unused legacy server stack.
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
        dependency:copy-dependencies -DincludeScope=runtime \
        -DexcludeArtifactIds=slf4j-log4j12,slf4j-reload4j \
        -DoutputDirectory=/dist/lib; \
    rm -f /dist/lib/jersey-server-1.*.jar /dist/lib/jersey-servlet-1.*.jar \
        /dist/lib/jersey-guice-1.*.jar; \
    jersey_servers=(/dist/lib/jersey-server-*.jar); \
    test "${#jersey_servers[@]}" -eq 1; \
    jar tf "${jersey_servers[0]}" | grep -Fx \
        org/glassfish/jersey/server/internal/scanning/AnnotationAcceptingListener\$AnnotatedClassVisitor.class; \
    logback_classic=(/dist/lib/logback-classic-*.jar); \
    logback_core=(/dist/lib/logback-core-*.jar); \
    test "${#logback_classic[@]}" -eq 1 && test "${#logback_core[@]}" -eq 1; \
    test -f "${logback_classic[0]}" && test -f "${logback_core[0]}"; \
    jar tf "${logback_classic[0]}" | grep -Fx \
        META-INF/services/org.slf4j.spi.SLF4JServiceProvider; \
    test ! -e /dist/lib/slf4j-log4j12-*.jar; \
    test ! -e /dist/lib/slf4j-reload4j-*.jar; \
    for module in pangrova-standalone pangrova-coordinator pangrova-worker \
        pangrova-client/pangrova-client-cli pangrova-connector/pangrova-connector-iceberg \
        pangrova-connector/pangrova-connector-paimon pangrova-connector/pangrova-connector-lance; do \
        properties="$module/target/maven-archiver/pom.properties"; test -f "$properties"; \
        artifact_id="$(sed -n 's/^artifactId=//p' "$properties")"; \
        artifact_version="$(sed -n 's/^version=//p' "$properties")"; \
        test -n "$artifact_id" && test -n "$artifact_version"; \
        main_jar="$module/target/$artifact_id-$artifact_version.jar"; \
        test -f "$main_jar"; cp "$main_jar" /dist/lib/; \
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
LABEL description="Pangrova distributed SQL query engine"
ENV PANGROVA_HOME=/opt/pangrova \
    PANGROVA_CONF_DIR=/opt/pangrova/config \
    PANGROVA_LOG_DIR=/opt/pangrova/logs
RUN groupadd -r pangrova && useradd -r -g pangrova -d /opt/pangrova pangrova \
    && mkdir -p /opt/pangrova/logs /opt/pangrova/data /mnt/spill/pangrova \
    && apt-get update && apt-get install -y --no-install-recommends curl libstdc++6 libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /dist/ /opt/pangrova/
RUN chmod +x /opt/pangrova/bin/*.sh && chown -R pangrova:pangrova /opt/pangrova /mnt/spill/pangrova
USER pangrova
WORKDIR /opt/pangrova
EXPOSE 8080 8081
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/v1/health/live || curl -f http://localhost:8081/v1/health/live || exit 1
ENTRYPOINT ["/opt/pangrova/bin/pangrova.sh"]
CMD ["start", "coordinator"]
