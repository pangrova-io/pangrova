package org.pangrova.worker.copy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopySourceReadClientTest {
    @Test
    void productionBearerClientRejectsPlaintextAndTestTransportRejectsRemoteHost() {
        assertThatThrownBy(() -> new CopySourceReadClient(
                "http://127.0.0.1:8080", "private-secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires HTTPS");
        assertThatThrownBy(() -> new CopySourceReadClient(
                "http://worker.example:8080", "private-secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires HTTPS");
        assertThatThrownBy(() -> CopySourceReadClient.forLoopbackTest(
                "http://worker.example:8080", "private-secret"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires HTTPS");
    }
}
