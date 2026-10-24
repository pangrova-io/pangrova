package org.pangrova.coordinator.testinfra;

import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.worker.bootstrap.WorkerBootstrap;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.jni.write.catalog.CatalogResourceResolver;

import java.nio.file.Path;

/** Configures real Native startup without accepting catalog execution in lifecycle tests. */
public final class NativeWorkerLifecycleFixture {
    private NativeWorkerLifecycleFixture() {}

    /** Uses a caller-owned temporary directory and the production resource budget defaults. */
    public static WorkerBootstrap create(WorkerBootstrapConfig original, Path shuffleDirectory) {
        var resource = original.resourceConfig();
        var configured = new WorkerBootstrapConfig.ResourceConfig(
                resource.memoryBudgetBytes(), resource.systemReservedBytes(),
                resource.queryThreads(), resource.ioThreads(), resource.schedulerThreads(),
                resource.spillDirectory(), resource.nodeMemoryBytes(),
                resource.pressureThresholdLow(), resource.pressureThresholdMedium(),
                resource.pressureThresholdHigh(), resource.pressureThresholdCritical(),
                resource.shuffleRetentionBytes(), resource.shuffleTransitBytes(),
                resource.shuffleSpillBytes(), resource.shuffleSpillFiles(),
                shuffleDirectory.toAbsolutePath().toString(), resource.shuffleChannelLimits());
        var worker = new WorkerBootstrap(new WorkerBootstrapConfig(
                original.workerId(), original.httpPort(), original.transportPort(),
                original.coordinatorUrl(), original.heartbeatIntervalMs(),
                original.observabilityEnabled(), original.observabilityBootstrapConfig(),
                configured, original.resultRetentionSeconds(), original.cdcEnabled(),
                original.registrationRetryConfig(), original.httpHost(), original.internalAuthSecret(),
                original.shuffleMode(), original.cacheConfig(), original.advertisedHost(),
                original.shuffleAdvertisedHost(),
                original.nativeFunctionConfig(), original.transportSecurityConfig(),
                original.admissionStateDirectory()));
        worker.setCatalogResourceResolver(new LifecycleCatalogGuard());
        return worker;
    }

    private static final class LifecycleCatalogGuard implements CatalogResourceResolver {
        @Override
        public CatalogResources resolve(String taskId) {
            throw new AssertionError("Lifecycle fixture cannot resolve catalog tasks");
        }

        @Override
        public void bindTask(String taskId, TaskRequest request) {
            throw new AssertionError("Lifecycle fixture cannot accept catalog tasks");
        }

        @Override
        public void releaseTask(String taskId) {
            // This fixture never binds a task or acquires a catalog lease.
        }
    }
}
