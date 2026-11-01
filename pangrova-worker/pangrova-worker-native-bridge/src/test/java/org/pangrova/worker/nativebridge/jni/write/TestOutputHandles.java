package org.pangrova.worker.nativebridge.jni.write;

import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;

/** Shared exact output handle for Worker write-adapter unit tests. */
public final class TestOutputHandles {

    private static final ConnectorOutputTableHandle HANDLE =
            new ConnectorOutputTableHandle() {};

    private TestOutputHandles() {}

    public static ConnectorOutputTableHandle outputHandle() {
        return HANDLE;
    }
}
