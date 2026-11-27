package org.pangrova.coordinator.result;

import java.util.function.Consumer;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;

/** Captures the batch passed to a test Flight listener while the caller holds its lease. */
public final class CoordinatorResultTestAccess {
    private CoordinatorResultTestAccess() { }

    public static VectorSchemaRoot streamedRoot(Consumer<ServerStreamListener> stream) {
        var listener = new CapturingListener();
        stream.accept(listener);
        return listener.root;
    }

    private static final class CapturingListener implements ServerStreamListener {
        private VectorSchemaRoot root;

        @Override public boolean isCancelled() { return false; }
        @Override public boolean isReady() { return true; }
        @Override public void setOnCancelHandler(Runnable handler) { }
        @Override public void start(VectorSchemaRoot value, DictionaryProvider dictionaries, IpcOption option) {
            root = value;
        }
        @Override public void putNext() { }
        @Override public void putNext(ArrowBuf metadata) { throw new UnsupportedOperationException(); }
        @Override public void putMetadata(ArrowBuf metadata) { throw new UnsupportedOperationException(); }
        @Override public void error(Throwable failure) { throw new AssertionError(failure); }
        @Override public void completed() { }
    }
}
