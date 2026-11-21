package org.pangrova.shuffle.nativewire;

import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;
import org.pangrova.shuffle.api.worker.ShuffleReader;

/** Takes validated Native wire bindings and opens readers scoped to the given consumer. */
public interface NativeShuffleReaders extends AutoCloseable {
    ShuffleReader<BatchChunk> open(ExternalShuffleInput binding, ExchangeTaskAttempt consumer);
    @Override void close();
}
