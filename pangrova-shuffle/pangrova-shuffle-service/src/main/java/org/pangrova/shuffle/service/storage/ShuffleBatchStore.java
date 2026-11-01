package org.pangrova.shuffle.service.storage;

import java.util.List;

import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.service.model.ShuffleServiceModel;

import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CommitCommand;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.CommitResult;
import static org.pangrova.shuffle.service.model.ShuffleServiceModel.StoredBatch;

/** Durable accepted-batch and producer-attempt transaction boundary. */
public interface ShuffleBatchStore {

    String beginWriteIntent(StoredBatch batch, long recoveryDelayMs);

    void completeWriteIntent(String reservationId, StoredBatch batch);

    void abortWriteIntent(String reservationId);

    List<StoredBatch> readAttemptPage(
            String shuffleId,
            TaskAttemptId attempt,
            int outputPartitionId,
            long batchAfter,
            int limit);

    CommitResult commitAttempt(CommitCommand command);

    long committedMetadataVersion(String shuffleId);

    List<StoredBatch> readCommittedPage(
            String shuffleId,
            int outputPartitionId,
            int producerAfter,
            long batchAfter,
            int limit);

    List<StoredBatch> readFinalizedPage(
            String shuffleId,
            int outputPartitionId,
            long startSequence,
            int limit);
}
