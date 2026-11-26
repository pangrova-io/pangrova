# syntax=docker/dockerfile:1
# The canonical Bake graph binds this context to the matching Native target.
FROM native_build AS native-release
WORKDIR /workspace
COPY pangrova-worker-native/ /expected/pangrova-worker-native/
COPY pangrova-protocol/src/main/proto/ /expected/pangrova-protocol/src/main/proto/
COPY pangrova-protocol/src/main/proto-imports/ /expected/pangrova-protocol/src/main/proto-imports/
COPY pangrova-protocol/src/main/resources/function-contracts/ /expected/pangrova-protocol/src/main/resources/function-contracts/
COPY pangrova-protocol/tools/function-contracts/ /expected/pangrova-protocol/tools/function-contracts/
COPY deploy/docker/verify-native-source.sh /verify-native-source.sh
RUN bash /verify-native-source.sh /expected /workspace \
    && pangrova-worker-native/bin/package-native-lib.sh --production \
        /workspace/pangrova-worker-native/build/libpangrova_native.so

FROM maven:3.9-eclipse-temurin-17@sha256:1a352420f7aba21f5ad08df31bab55f74c013fb491f1ae8ab1dd7ff9ed698584 AS java-build-base
WORKDIR /build

FROM java-build-base AS iceberg-sdk-build
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip patch libdigest-sha-perl \
    && rm -rf /var/lib/apt/lists/*
RUN curl --fail --location --retry 3 --output /tmp/gradle.zip \
        https://services.gradle.org/distributions/gradle-8.14.4-bin.zip \
    && echo "f1771298a70f6db5a29daf62378c4e18a17fc33c9ba6b14362e0cdf40610380d  /tmp/gradle.zip" | sha256sum -c - \
    && unzip -q /tmp/gradle.zip -d /opt && rm /tmp/gradle.zip
COPY .pangrova/scripts/build-iceberg-sdk-candidate.sh .pangrova/scripts/
COPY pangrova-connector/pangrova-connector-iceberg/patches/ pangrova-connector/pangrova-connector-iceberg/patches/
RUN --mount=type=cache,target=/root/.gradle \
    GRADLE_BIN=/opt/gradle-8.14.4/bin/gradle PANGROVA_ICEBERG_MAVEN_REPOSITORY=/sdk \
    bash .pangrova/scripts/build-iceberg-sdk-candidate.sh

# CI overrides this stage with the verified same-run SDK as a named context.
FROM scratch AS iceberg-sdk
COPY --from=iceberg-sdk-build /sdk/ /

FROM java-build-base AS builder
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && python3 -c 'import sys; assert sys.version_info >= (3, 10), sys.version' \
    && rm -rf /var/lib/apt/lists/*
COPY --from=iceberg-sdk /org/apache/iceberg/ /pinned-iceberg/iceberg/
# The complete reactor defines dependencies; there is no stale manual POM cache inventory.
COPY . .
COPY --from=native-release /workspace/pangrova-worker-native/build/libpangrova_native.so /native/libpangrova_native.so
ARG MAVEN_ARGS
# Hadoop's Jersey 1 server contains shaded ASM classes also present in Jersey 4.
# Keep its client/core dependencies, but omit the unused legacy server stack.
RUN --mount=type=cache,target=/root/.m2 bash -euo pipefail -c '\
    mkdir -p /root/.m2/repository/org/apache; cp -a /pinned-iceberg/iceberg /root/.m2/repository/org/apache/; \
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
        -Dclassifier="$classifier" -Dpackaging=jar; \
    mvn -B -q -Pnative-runtime -DskipTests \
        -pl pangrova-standalone,pangrova-client/pangrova-client-cli,pangrova-connector/pangrova-connector-iceberg,pangrova-connector/pangrova-connector-paimon,pangrova-connector/pangrova-connector-lance,\!pangrova-native-bundle \
        -am install; \
    mkdir -p /dist/lib /dist/cli /dist/bin /dist/config/catalog; \
    python3 deploy/docker/stage-role-classpaths.py . /dist/lib; \
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
    ai_worker_jars=(/dist/lib/pangrova-ai-worker-*.jar); \
    test "${#ai_worker_jars[@]}" -eq 1; \
    provider_check="$(mktemp -d)"; \
    (cd "$provider_check"; jar --extract --file "${ai_worker_jars[0]}" \
        META-INF/services/org.pangrova.function.api.registry.FunctionFactory \
        META-INF/services/org.pangrova.function.api.runtime.FunctionCallbackScope \
        META-INF/services/org.pangrova.function.api.runtime.FunctionFailureProjector \
        META-INF/services/org.pangrova.protocol.execution.AiTaskLifecycle \
        META-INF/services/org.pangrova.ai.api.model.AiModelCatalog; \
        grep -Fx org.pangrova.ai.worker.function.AiFunctionFactory \
            META-INF/services/org.pangrova.function.api.registry.FunctionFactory; \
        grep -Fx org.pangrova.ai.worker.context.WorkerAiCallbackScope \
            META-INF/services/org.pangrova.function.api.runtime.FunctionCallbackScope; \
        grep -Fx org.pangrova.ai.worker.function.AiFunctionFailureProjector \
            META-INF/services/org.pangrova.function.api.runtime.FunctionFailureProjector; \
        grep -Fx org.pangrova.ai.worker.invocation.WorkerAiTaskLifecycle \
            META-INF/services/org.pangrova.protocol.execution.AiTaskLifecycle; \
        grep -Fx org.pangrova.ai.worker.provider.WorkerAiModelCatalog \
            META-INF/services/org.pangrova.ai.api.model.AiModelCatalog); \
    rm -r "$provider_check"; \
    cli_exec=(pangrova-client/pangrova-client-cli/target/pangrova-client-cli-*-exec.jar); \
    test "${#cli_exec[@]}" -eq 1; test -f "${cli_exec[0]}"; cp "${cli_exec[0]}" /dist/cli/; \
    test -f "/dist/lib/$(basename "$bundle")"; \
    lance_jars=(/dist/lib/pangrova-connector-lance-*.jar); \
    test "${#lance_jars[@]}" -eq 1; \
    provider_check="$(mktemp -d)"; \
    (cd "$provider_check"; jar --extract --file "${lance_jars[0]}" \
        META-INF/services/org.pangrova.connector.api.plugin.ConnectorFactory; \
        grep -Fx org.pangrova.connector.lance.LanceConnectorFactory \
            META-INF/services/org.pangrova.connector.api.plugin.ConnectorFactory); \
    rm -r "$provider_check"; \
    cp -r bin/. /dist/bin/; cp -r etc/. /dist/config/'

# The system ABI baseline matches the native Ubuntu 22.04 build.
FROM eclipse-temurin:17-jre-jammy@sha256:b3054a20e1a851574cdb1692dd0db5310c647e62214a2a7abecbec1b9e3ae8f1
ARG SOURCE_COMMIT
LABEL org.opencontainers.image.revision="${SOURCE_COMMIT}"
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
