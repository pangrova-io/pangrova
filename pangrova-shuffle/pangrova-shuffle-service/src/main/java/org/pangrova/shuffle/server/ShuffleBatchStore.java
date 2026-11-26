package org.pangrova.shuffle.server;

import java.util.List;

import org.pangrova.protocol.v2.TaskAttemptId;

import static org.pangrova.shuffle.server.ShuffleServiceModel.CommitCommand;
import static org.pangrova.shuffle.server.ShuffleServiceModel.CommitResult;
import static org.pangrova.shuffle.server.ShuffleServiceModel.Allocation;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

/** Durable accepted-batch and producer-attempt transaction boundary. */
interface ShuffleBatchStore {

    WriteGrant openWriteWindow(String shuffleId, TaskAttemptId attempt, long recoveryDelayMs);

    void publishWriteWindow(WriteGrant grant, String shuffleId, TaskAttemptId attempt, List<StoredBatch> batches);

    void settleWriteWindow(WriteGrant grant, String shuffleId, TaskAttemptId attempt);

    record WriteGrant(String reservation, int partitionCount, long protectedForMillis) {}

    List<StoredBatch> readAttemptPage(
            String shuffleId,
            TaskAttemptId attempt,
            int outputPartitionId,
            long batchAfter,
            int limit);

    CommitResult commitAttempt(CommitCommand command);

    /** Ordered winner inventory for building a publication under the selected metadata version. */
    List<StoredBatch> readCommittedPage(Allocation selected, BatchIdentity after, int limit);
}
