package org.pangrova.worker.bootstrap.phase;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.config.WorkerOptions;
import org.pangrova.protocol.v2.RuntimeInitConfig;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.pangrova.worker.cache.NativeCacheConfig;
import org.junit.jupiter.api.Test;

/**
 * Engine spill must be reachable by default. An unset spill directory previously
 * left spill disabled, so a memory-denied allocation could never trigger operator
 * spill and large joins/aggregations failed instead of spilling.
 */
class NativeSpillGateTest {

    @Test
    void defaultsEnableEngineSpill() throws Exception {
        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-spill-default");
        RuntimeInitConfig parsed = RuntimeInitConfig.parseFrom(
                NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, NativeCacheConfig.DISABLED));

        assertThat(WorkerOptions.SPILL_DIRECTORY.defaultValue()).isNotBlank();
        assertThat(parsed.getSpillEnabled()).isTrue();
        assertThat(parsed.getSpillDirectory()).isNotBlank();
    }

    @Test
    void spillDirectoryIsTrimmedBeforeTheGateIsEvaluated() throws Exception {
        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-spill-trim");
        RuntimeInitConfig parsed = RuntimeInitConfig.parseFrom(
                NativeRuntimeBootstrap.buildNativeInitConfigBytes(config, NativeCacheConfig.DISABLED));

        assertThat(parsed.getSpillDirectory()).isEqualTo(parsed.getSpillDirectory().trim());
        assertThat(parsed.getSpillEnabled()).isEqualTo(!parsed.getSpillDirectory().isEmpty());
    }
}