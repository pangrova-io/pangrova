package org.pangrova.coordinator.exception;

/** No registered Native Worker was available to receive a dataset control request. */
public final class NativeDatasetWorkerUnavailableException extends IllegalStateException {

    public NativeDatasetWorkerUnavailableException() {
        super("No active Native Worker for metadata control");
    }
}
