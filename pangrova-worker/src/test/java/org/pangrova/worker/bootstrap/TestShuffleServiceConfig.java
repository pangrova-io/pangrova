package org.pangrova.worker.bootstrap;

import java.io.IOException;
import java.nio.file.Files;

/** Explicit, isolated spill directory for tests that start a Native Worker. */
public final class TestShuffleServiceConfig {

    private TestShuffleServiceConfig() {}

    public static WorkerBootstrapConfig.ResourceConfig resources(
            WorkerBootstrapConfig.ResourceConfig original) {
        final String spillDirectory;
        try {
            var path = Files.createTempDirectory("pangrova-test-native-shuffle-");
            path.toFile().deleteOnExit();
            spillDirectory = path.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create isolated Native shuffle spill directory", e);
        }
        return new WorkerBootstrapConfig.ResourceConfig(
                original.memoryBudgetBytes(), original.systemReservedBytes(),
                original.queryThreads(), original.ioThreads(), original.schedulerThreads(),
                original.spillDirectory(), original.nodeMemoryBytes(),
                original.pressureThresholdLow(), original.pressureThresholdMedium(),
                original.pressureThresholdHigh(), original.pressureThresholdCritical(),
                original.shuffleRetentionBytes(), original.shuffleTransitBytes(),
                original.shuffleSpillBytes(), original.shuffleSpillFiles(), spillDirectory);
    }

    public static WorkerBootstrapConfig config(WorkerBootstrapConfig original) {
        return new WorkerBootstrapConfig(
                original.workerId(), original.httpPort(), original.transportPort(),
                original.coordinatorUrl(), original.heartbeatIntervalMs(),
                original.observabilityEnabled(), original.observabilityBootstrapConfig(),
                resources(original.resourceConfig()), original.resultRetentionSeconds(),
                original.cdcEnabled(), original.registrationRetryConfig(), original.httpHost(),
                original.internalAuthSecret(), original.shuffleMode(), original.cacheConfig(),
                original.advertisedHost(), original.nativeFunctionConfig(),
                original.transportSecurityConfig());
    }
}
