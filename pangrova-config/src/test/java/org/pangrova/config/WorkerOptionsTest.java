package org.pangrova.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WorkerOptions}.
 * Verifies all typed config keys, default values, and the R1.3 requirement
 * that feature flags default to true except security.enabled.
 */
class WorkerOptionsTest {

    // ---- Server defaults ----

    @Test
    void workerHostDefaultsToAllInterfaces() {
        assertThat(WorkerOptions.WORKER_HOST.defaultValue()).isEqualTo("0.0.0.0");
        assertThat(WorkerOptions.WORKER_HOST.typeClass()).isEqualTo(String.class);
    }

    @Test
    void workerPortDefaultsTo8081() {
        assertThat(WorkerOptions.WORKER_PORT.defaultValue()).isEqualTo(8081);
        assertThat(WorkerOptions.WORKER_PORT.typeClass()).isEqualTo(Integer.class);
    }

    @Test
    void workerAdvertisedHostHasNoDefault() {
        assertThat(WorkerOptions.WORKER_ADVERTISED_HOST.defaultValue()).isNull();
        assertThat(WorkerOptions.WORKER_ADVERTISED_HOST.hasDefaultValue()).isFalse();
    }

    @Test
    void workerIdDefaultsToWorker1() {
        assertThat(WorkerOptions.WORKER_ID.defaultValue()).isEqualTo("worker-1");
        assertThat(WorkerOptions.WORKER_ID.typeClass()).isEqualTo(String.class);
    }

    // ---- Coordinator connection ----

    @Test
    void coordinatorUrlHasNoDefault() {
        assertThat(WorkerOptions.COORDINATOR_URL.defaultValue()).isNull();
        assertThat(WorkerOptions.COORDINATOR_URL.hasDefaultValue()).isFalse();
    }

    @Test
    void heartbeatIntervalDefaultsTo10Seconds() {
        assertThat(WorkerOptions.HEARTBEAT_INTERVAL.defaultValue()).isEqualTo(Duration.ofSeconds(10));
        assertThat(WorkerOptions.HEARTBEAT_INTERVAL.typeClass()).isEqualTo(Duration.class);
    }

    // ---- Transport ----

    @Test
    void transportPortDefaultsToZero() {
        assertThat(WorkerOptions.TRANSPORT_PORT.defaultValue()).isEqualTo(0);
    }

    // ---- Task execution ----

    @Test
    void resultRetentionDefaultsTo300Seconds() {
        assertThat(WorkerOptions.RESULT_RETENTION_SECONDS.defaultValue()).isEqualTo(300);
    }

    // ---- Shuffle ----

    @Test
    void shuffleBufferCapacityDefaultsTo100() {
        assertThat(WorkerOptions.SHUFFLE_BUFFER_CAPACITY.defaultValue()).isEqualTo(100);
    }

    @Test
    void shuffleMaxMemoryDefaultsTo512MB() {
        assertThat(WorkerOptions.SHUFFLE_MAX_MEMORY_BYTES.defaultValue()).isEqualTo(512L * 1024 * 1024);
        assertThat(WorkerOptions.SHUFFLE_MAX_MEMORY_BYTES.typeClass()).isEqualTo(Long.class);
    }

    @Test
    void shuffleSpillThresholdDefaultsTo0Point8() {
        assertThat(WorkerOptions.SHUFFLE_SPILL_THRESHOLD.defaultValue()).isEqualTo(0.8);
        assertThat(WorkerOptions.SHUFFLE_SPILL_THRESHOLD.typeClass()).isEqualTo(Double.class);
    }

    // ---- Memory / Resources ----

    @Test
    void memoryBudgetDefaultsTo512MB() {
        assertThat(WorkerOptions.MEMORY_BUDGET_BYTES.defaultValue()).isEqualTo(512L * 1024 * 1024);
    }

    @Test
    void memorySystemReservedDefaultsTo64MB() {
        assertThat(WorkerOptions.MEMORY_SYSTEM_RESERVED_BYTES.defaultValue()).isEqualTo(64L * 1024 * 1024);
    }

    @Test
    void memoryNodeBytesDefaultsTo1GB() {
        assertThat(WorkerOptions.MEMORY_NODE_BYTES.defaultValue()).isEqualTo(1024L * 1024 * 1024);
    }

    @Test
    void threadsQueryDefaultsTo4() {
        assertThat(WorkerOptions.THREADS_QUERY.defaultValue()).isEqualTo(4);
    }

    @Test
    void threadsIoDefaultsTo2() {
        assertThat(WorkerOptions.THREADS_IO.defaultValue()).isEqualTo(2);
    }

    @Test
    void threadsSchedulerDefaultsTo1() {
        assertThat(WorkerOptions.THREADS_SCHEDULER.defaultValue()).isEqualTo(1);
    }

    @Test
    void spillDirectoryHasNoDefault() {
        assertThat(WorkerOptions.SPILL_DIRECTORY.defaultValue()).isNull();
        assertThat(WorkerOptions.SPILL_DIRECTORY.hasDefaultValue()).isFalse();
    }

    // ---- Native engine ----

    @Test
    void nativePathHasNoDefault() {
        assertThat(WorkerOptions.NATIVE_PATH.defaultValue()).isNull();
        assertThat(WorkerOptions.NATIVE_PATH.hasDefaultValue()).isFalse();
    }

    @Test
    void nativeWorkdirHasNoDefault() {
        assertThat(WorkerOptions.NATIVE_WORKDIR.defaultValue()).isNull();
    }

    @Test
    void nativeDeleteAfterLoadDefaultsToTrue() {
        assertThat(WorkerOptions.NATIVE_DELETE_AFTER_LOAD.defaultValue()).isTrue();
    }

    // ---- Registration retry ----

    @Test
    void registrationMaxAttemptsDefaultsTo5() {
        assertThat(WorkerOptions.REGISTRATION_MAX_ATTEMPTS.defaultValue()).isEqualTo(5);
    }

    @Test
    void registrationInitialBackoffDefaultsTo1Second() {
        assertThat(WorkerOptions.REGISTRATION_INITIAL_BACKOFF.defaultValue()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void registrationMaxBackoffDefaultsTo30Seconds() {
        assertThat(WorkerOptions.REGISTRATION_MAX_BACKOFF.defaultValue()).isEqualTo(Duration.ofSeconds(30));
    }

    // ---- Feature flags: all true except security (R1.3) ----

    @Test
    void featureFlagsDefaultTrueExceptSecurity() {
        List<ConfigOption<Boolean>> enabledByDefault = List.of(
                WorkerOptions.OBSERVABILITY_ENABLED,
                WorkerOptions.CDC_ENABLED);

        for (ConfigOption<Boolean> flag : enabledByDefault) {
            assertThat(flag.defaultValue())
                    .as("Feature flag '%s' should default to true", flag.key())
                    .isTrue();
            assertThat(flag.typeClass()).isEqualTo(Boolean.class);
        }
    }

    @Test
    void securityEnabledDefaultsToFalse() {
        assertThat(WorkerOptions.SECURITY_ENABLED.defaultValue()).isFalse();
        assertThat(WorkerOptions.SECURITY_ENABLED.key()).isEqualTo("security.enabled");
    }

    // ---- Observability config ----

    @Test
    void observabilityServiceNameHasNoDefault() {
        assertThat(WorkerOptions.OBSERVABILITY_SERVICE_NAME.defaultValue()).isNull();
    }

    @Test
    void observabilityOtlpEndpointHasNoDefault() {
        assertThat(WorkerOptions.OBSERVABILITY_OTLP_ENDPOINT.defaultValue()).isNull();
    }

    // ---- R1.3: Empty config enables feature flags except security ----

    @Test
    void emptyConfigEnablesFeaturesExceptSecurity() {
        PangrovaConfiguration emptyConfig = PangrovaConfiguration.fromYaml(Map.of());

        assertThat(emptyConfig.getBoolean(WorkerOptions.OBSERVABILITY_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(WorkerOptions.CDC_ENABLED)).isTrue();
        assertThat(emptyConfig.getBoolean(WorkerOptions.SECURITY_ENABLED)).isFalse();
    }
}
