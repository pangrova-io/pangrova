package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import org.pangrova.common.lifecycle.BootstrapFailedException;
import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.nativebridge.connector.spi.CatalogResourceResolver;
import org.pangrova.worker.nativebridge.jni.NativeFunctionConfig;

class WorkerBootstrapFailureLifecycleTest {

    private static final String UNREACHABLE_COORDINATOR_URL = "http://127.0.0.1:1";

    @Test
    void missingCatalogRuntimeFailsClosedAndRollsBackOwnedResources() {
        WorkerBootstrapConfig defaults = WorkerBootstrapConfig.withDefaults("rollback-worker");
        WorkerBootstrap bootstrap = new WorkerBootstrap(
                TestShuffleServiceConfig.config(withFunctions(defaults, NativeFunctionConfig.DEFAULT)));
        AtomicInteger closeCount = new AtomicInteger();
        bootstrap.setCdcConsumer(closeCount::incrementAndGet);

        assertThatThrownBy(bootstrap::start)
                .isInstanceOf(BootstrapFailedException.class)
                .hasMessageContaining("Worker catalog runtime is not configured");

        assertThat(bootstrap.isStarted()).isFalse();
        assertThat(closeCount).hasValue(1);
        bootstrap.shutdown();
        assertThat(closeCount).hasValue(1);
    }

    @Test
    void startupFailureAfterCatalogInstallationClosesCatalogRuntime() {
        WorkerBootstrapConfig defaults = WorkerBootstrapConfig.withDefaults("catalog-rollback-worker");
        WorkerBootstrap bootstrap = new WorkerBootstrap(
                TestShuffleServiceConfig.config(withFunctions(defaults, NativeFunctionConfig.DEFAULT)));
        AtomicInteger closeCount = new AtomicInteger();
        bootstrap.setCatalogResourceResolver(new ClosingCatalogResolver(closeCount));

        assertThatThrownBy(bootstrap::start)
                .isInstanceOf(BootstrapFailedException.class)
                .hasMessageContaining("register failed");
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
                UNREACHABLE_COORDINATOR_URL, source.heartbeatIntervalMs(),
                source.observabilityEnabled(), source.observabilityBootstrapConfig(),
                source.resourceConfig(), source.resultRetentionSeconds(),
                source.cdcEnabled(), source.registrationRetryConfig(), source.httpHost(),
                source.internalAuthSecret(), source.shuffleMode(), source.cacheConfig(),
                source.advertisedHost(), source.shuffleAdvertisedHost(),
                functions, source.transportSecurityConfig(),
                org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
    }
}
