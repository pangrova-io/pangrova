package org.pangrova.worker.bootstrap;

import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.jni.write.catalog.CatalogResourceResolver;
import org.pangrova.worker.jni.NativeFunctionConfig;

class WorkerBootstrapFailureLifecycleTest {

    @Test
    void enabledFileStartupFailureRollsBackOwnedCloseableResources() {
        WorkerBootstrapConfig defaults = WorkerBootstrapConfig.withDefaults("rollback-worker");
        NativeFunctionConfig incompleteFile = new NativeFunctionConfig(
                true, Map.of("ai-inference", Map.of("file.storage-backend", "s3")));
        WorkerBootstrap bootstrap = new WorkerBootstrap(
                TestShuffleServiceConfig.config(withFunctions(defaults, incompleteFile)));
        AtomicInteger closeCount = new AtomicInteger();
        bootstrap.setCdcConsumer(closeCount::incrementAndGet);

        assertThatThrownBy(bootstrap::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file.capability-secret-reference is required");

        assertThat(bootstrap.isStarted()).isFalse();
        assertThat(closeCount).hasValue(1);
        bootstrap.shutdown();
        assertThat(closeCount).hasValue(1);
    }

    @Test
    void startupFailureClosesInstalledCatalogRuntimeBeforeWriterRegistration() {
        WorkerBootstrapConfig defaults = WorkerBootstrapConfig.withDefaults("catalog-rollback-worker");
        NativeFunctionConfig incompleteFile = new NativeFunctionConfig(
                true, Map.of("ai-inference", Map.of("file.storage-backend", "s3")));
        WorkerBootstrap bootstrap = new WorkerBootstrap(
                TestShuffleServiceConfig.config(withFunctions(defaults, incompleteFile)));
        AtomicInteger closeCount = new AtomicInteger();
        bootstrap.setCatalogResourceResolver(new ClosingCatalogResolver(closeCount));

        assertThatThrownBy(bootstrap::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file.capability-secret-reference is required");
        assertThat(closeCount).hasValue(1);
    }

    private record ClosingCatalogResolver(AtomicInteger closeCount)
            implements CatalogResourceResolver, AutoCloseable {
        @Override public CatalogResources resolve(String taskId) {
            throw new UnsupportedOperationException();
        }
        @Override public void bindTask(String taskId, org.pangrova.protocol.v2.TaskRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public void releaseTask(String taskId) {}
        @Override public void close() { closeCount.incrementAndGet(); }
    }

    private static WorkerBootstrapConfig withFunctions(
            WorkerBootstrapConfig source, NativeFunctionConfig functions) {
        return new WorkerBootstrapConfig(
                source.workerId(), source.httpPort(), source.transportPort(),
                source.coordinatorUrl(), source.heartbeatIntervalMs(),
                source.observabilityEnabled(), source.observabilityBootstrapConfig(),
                source.resourceConfig(), source.resultRetentionSeconds(),
                source.cdcEnabled(), source.registrationRetryConfig(), source.httpHost(),
                source.internalAuthSecret(), source.shuffleMode(), source.cacheConfig(),
                source.advertisedHost(), source.shuffleAdvertisedHost(),
                functions, source.transportSecurityConfig(), org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
    }
}
