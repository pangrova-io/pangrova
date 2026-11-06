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

FROM maven:3.9-eclipse-temurin-17@sha256:1a352420f7aba21f5ad08df31bab55f74c013fb491f1ae8ab1dd7ff9ed698584 AS builder
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
    mkdir -p /dist/lib /dist/cli /dist/bin /dist/config/catalog; \
    mvn -B -q -Pnative-runtime \
        -pl pangrova-standalone,pangrova-server/pangrova-server-coordinator,pangrova-coordination/pangrova-coordination-core,pangrova-worker/pangrova-worker-core,pangrova-client/pangrova-client-cli,pangrova-connector/pangrova-connector-iceberg,pangrova-connector/pangrova-connector-paimon,pangrova-connector/pangrova-connector-lance \
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
    test ! -e /dist/lib/pangrova-testing-kit-*.jar; \
    test ! -e /dist/lib/pangrova-testing-integration-tests-*.jar; \
    for module in pangrova-standalone pangrova-server/pangrova-server-coordinator \
        pangrova-coordination/pangrova-coordination-core \
        pangrova-worker/pangrova-worker-core pangrova-ai/pangrova-ai-worker \
        pangrova-indexing/pangrova-indexing-api pangrova-indexing/pangrova-indexing-strategies-builtin \
        pangrova-file/pangrova-file-api pangrova-file/pangrova-file-core \
        pangrova-file/pangrova-file-worker-adapter \
        pangrova-security/pangrova-security-api \
        pangrova-security/pangrova-security-core \
        pangrova-security/pangrova-security-auth-builtin \
        pangrova-security/pangrova-security-auth-ldap \
        pangrova-security/pangrova-security-auth-oauth2 \
        pangrova-security/pangrova-security-policy-builtin \
        pangrova-security/pangrova-security-secret-env \
        pangrova-security/pangrova-security-secret-file \
        pangrova-connector/pangrova-connector-api pangrova-connector/pangrova-connector-runtime \
        pangrova-persistence/pangrova-persistence-security-postgresql pangrova-persistence/pangrova-persistence-security-memory pangrova-persistence/pangrova-persistence-security-api pangrova-persistence/pangrova-persistence-query-api pangrova-persistence/pangrova-persistence-jdbc-api \
        pangrova-persistence/pangrova-persistence-generation-api pangrova-persistence/pangrova-persistence-generation-postgresql \
        pangrova-persistence/pangrova-persistence-file-api pangrova-persistence/pangrova-persistence-file-postgresql \
        pangrova-persistence/pangrova-persistence-table-api \
        pangrova-persistence/pangrova-persistence-table-postgresql \
        pangrova-persistence/pangrova-persistence-table-memory \
        pangrova-filesystem/pangrova-filesystem-api pangrova-filesystem/pangrova-filesystem-local \
        pangrova-filesystem/pangrova-filesystem-s3 pangrova-filesystem/pangrova-filesystem-oss \
        pangrova-client/pangrova-client-cli pangrova-connector/pangrova-connector-iceberg \
        pangrova-connector/pangrova-connector-paimon pangrova-connector/pangrova-connector-lance; do \
        properties="$module/target/maven-archiver/pom.properties"; test -f "$properties"; \
        artifact_id="$(sed -n 's/^artifactId=//p' "$properties")"; \
        artifact_version="$(sed -n 's/^version=//p' "$properties")"; \
        test -n "$artifact_id" && test -n "$artifact_version"; \
        main_jar="$module/target/$artifact_id-$artifact_version.jar"; \
        test -f "$main_jar"; cp "$main_jar" /dist/lib/; \
    done; \
    ai_worker_jars=(/dist/lib/pangrova-ai-worker-*.jar); \
    test "${#ai_worker_jars[@]}" -eq 1; \
    provider_check="$(mktemp -d)"; \
    (cd "$provider_check"; jar --extract --file "${ai_worker_jars[0]}" \
        META-INF/services/org.pangrova.function.spi.registry.FunctionFactory \
        META-INF/services/org.pangrova.function.spi.runtime.FunctionCallbackScope \
        META-INF/services/org.pangrova.function.spi.runtime.FunctionFailureProjector \
        META-INF/services/org.pangrova.protocol.execution.AiTaskLifecycle \
        META-INF/services/org.pangrova.ai.spi.model.AiModelCatalog; \
        grep -Fx org.pangrova.ai.worker.function.AiFunctionFactory \
            META-INF/services/org.pangrova.function.spi.registry.FunctionFactory; \
        grep -Fx org.pangrova.ai.worker.context.WorkerAiCallbackScope \
            META-INF/services/org.pangrova.function.spi.runtime.FunctionCallbackScope; \
        grep -Fx org.pangrova.ai.worker.function.AiFunctionFailureProjector \
            META-INF/services/org.pangrova.function.spi.runtime.FunctionFailureProjector; \
        grep -Fx org.pangrova.ai.worker.invocation.WorkerAiTaskLifecycle \
            META-INF/services/org.pangrova.protocol.execution.AiTaskLifecycle; \
        grep -Fx org.pangrova.ai.worker.provider.WorkerAiModelCatalog \
            META-INF/services/org.pangrova.ai.spi.model.AiModelCatalog); \
    rm -r "$provider_check"; \
    cli_exec=(pangrova-client/pangrova-client-cli/target/pangrova-client-cli-*-exec.jar); \
    test "${#cli_exec[@]}" -eq 1; test -f "${cli_exec[0]}"; cp "${cli_exec[0]}" /dist/cli/; \
    test -f "/dist/lib/$(basename "$bundle")"; \
    lance_jars=(/dist/lib/pangrova-connector-lance-*.jar); \
    test "${#lance_jars[@]}" -eq 1; \
    provider_check="$(mktemp -d)"; \
    (cd "$provider_check"; jar --extract --file "${lance_jars[0]}" \
        META-INF/services/org.pangrova.connector.spi.plugin.ConnectorFactory; \
        grep -Fx org.pangrova.connector.lance.LanceConnectorFactory \
            META-INF/services/org.pangrova.connector.spi.plugin.ConnectorFactory); \
    rm -r "$provider_check"; \
    cp -r bin/. /dist/bin/; cp -r etc/. /dist/config/'

# The system ABI baseline matches the native Ubuntu 22.04 build.
FROM eclipse-temurin:17-jre-jammy@sha256:b3054a20e1a851574cdb1692dd0db5310c647e62214a2a7abecbec1b9e3ae8f1
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
    CMD /opt/pangrova/bin/pangrova-container-health.sh
ENTRYPOINT ["/opt/pangrova/bin/pangrova.sh"]
CMD ["start", "coordinator"]
