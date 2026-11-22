package org.pangrova.shuffle.nativewire;

import org.pangrova.protocol.v2.BatchChunk;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;
import org.pangrova.shuffle.api.worker.ShuffleReader;

/** Takes Native wire bindings and prepares readers scoped to their original published partitions. */
public interface NativeShuffleReaders extends AutoCloseable {
    PartitionReaders prepare(ExternalShuffleInput binding, ExchangeTaskAttempt consumer);

    @FunctionalInterface
    interface PartitionReaders {
        ShuffleReader<BatchChunk> open(int publicationIndex);
    }

    @Override void close();
}
