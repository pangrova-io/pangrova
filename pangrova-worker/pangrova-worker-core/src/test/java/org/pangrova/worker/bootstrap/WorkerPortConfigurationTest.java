package org.pangrova.worker.bootstrap;

import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;

import org.junit.jupiter.api.Test;
import org.pangrova.config.PangrovaConfiguration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration boundary inputs only; actual Native listeners are verified separately. */
class WorkerPortConfigurationTest {
    @Test
    void zeroRemainsAValidAutomaticPortRequest() {
        assertThat(load(0).transportPort()).isZero();
    }

    @Test
    void largestTcpPortRetainsItsExactConfiguredValue() {
        assertThat(load(65_535).transportPort()).isEqualTo(65_535);
    }

    @Test
    void overflowingTcpPortCannotReachNativeOrWire() {
        assertThatThrownBy(() -> load(65_536)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transportPort must be in [0, 65535]");
    }

    @Test
    void negativeTcpPortIsRejectedBeforeNativeStartup() {
        assertThatThrownBy(() -> load(-1)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transportPort must be in [0, 65535]");
    }

    private static WorkerBootstrapConfig load(int port) {
        return PangrovaWorkerMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("worker.transport.port", Integer.toString(port))));
    }
}
