package org.pangrova.worker.jni.write.catalog;

import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.WriteOutputBinding;

/** Resolves the sole admitted write-output variant for Worker-local connector control. */
final class WorkerWriteOutputs {
    private WorkerWriteOutputs() {}

    static SerializedConnectorOutputHandle outputHandle(WriteOutputBinding binding) {
        return switch (binding.getBindingCase()) {
            case LANCE -> binding.getLance().getOutputHandle();
            case PHYSICAL -> binding.getPhysical().getOutputHandle();
            case CONNECTOR -> binding.getConnector().getOutputHandle();
            default -> throw new IllegalArgumentException("Write output binding is missing");
        };
    }

    static CatalogExecutionBinding catalog(WriteOutputBinding binding) {
        return switch (binding.getBindingCase()) {
            case LANCE -> binding.getLance().getCatalog();
            case PHYSICAL -> binding.getPhysical().getCatalog();
            case CONNECTOR -> binding.getConnector().getCatalog();
            default -> throw new IllegalArgumentException("Write catalog binding is missing");
        };
    }
}
