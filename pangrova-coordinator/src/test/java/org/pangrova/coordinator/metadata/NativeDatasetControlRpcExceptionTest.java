package org.pangrova.coordinator.metadata;

import org.pangrova.coordinator.metadata.nativecontrol.NativeDatasetControlRpcException;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class NativeDatasetControlRpcExceptionTest {

    @Test
    void uncertaintyPreservesTransportCause() {
        var failure = new IOException("connection terminated");
        var uncertainty = new NativeDatasetControlRpcException("publication outcome unknown", failure);
        assertThat(uncertainty).hasMessage("publication outcome unknown").hasCause(failure);
    }
}
