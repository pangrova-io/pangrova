package org.pangrova.shuffle.api.worker;

import java.nio.ByteBuffer;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Synchronous encoded-batch delivery to an already admitted shuffle producer attempt.
 *
 * <p>The binding owner installs an immutable, authorized producer binding before any callback.
 * {@code queryId} and {@code taskId} select that binding; they are not permission to create one,
 * select a newer attempt, or replace its generation, fencing token or access capability. The
 * implementation rejects batches and completion for absent or mismatched bindings.
 *
 * <p>Calls may arrive concurrently for different partitions and tasks. The caller orders batches
 * for each partition and starts completion only after its last batch. Implementations coordinate
 * concurrent cancellation with in-flight writes and completion, and retain ownership of resources
 * whose cleanup has not completed. A write may block under bounded backpressure, but cancellation
 * and the admitted deadline must bound the wait.
 *
 * <p>This is a delivery boundary, not a provider factory, registration API or exchange commit
 * receipt. Implementations use the provider's producer lifecycle. Successful producer completion
 * does not select the winning attempt or publish the full exchange to consumers. Remote timeout
 * may leave a commit outcome unknown; callers must not infer rollback, replay batches blindly,
 * or delete possibly published data.
 *
 * <p>The C++ direct transport does not use this SPI. A Java implementation can receive callbacks
 * from Native or another encoded-batch producer without exposing SDK, executor or SQL types here.
 */
@Experimental(reason = "Encoded-batch producer delivery; provider sessions and receipts evolve separately")
public interface ShuffleByteEgress {

    /**
     * Delivers one nonempty encoded batch. Row count is positive and is not a batch sequence.
     *
     * <p>The caller owns the buffer. The implementation must not modify its position, limit or
     * contents, and must consume or copy the readable bytes before returning (including exceptional
     * return). It must not retain a view of caller-owned memory for asynchronous work. Returning
     * successfully acknowledges delivery of this batch, not producer or exchange commit. Repeating
     * this method is another batch, not an idempotent retry; replay requires a separate provider
     * identity/sequence protocol.
     *
     * @throws IllegalArgumentException if the batch, row count or partition is invalid
     * @throws RuntimeException if admission, deadline, cancellation or delivery fails
     */
    void onPartitionBatch(String queryId, String taskId, int partitionId,
                          long rowCount, ByteBuffer serializedBatch);

    /**
     * Completes the admitted producer, including a producer with no output batches. This waits
     * for its accepted writes and the provider's producer completion boundary. The owner invokes
     * it once; duplicate completion after binding retirement is rejected, not reported as a new
     * successful commit. Exchange sealing, selected attempts and consumer visibility remain with
     * the exchange owner.
     *
     * @throws RuntimeException if completion fails, is cancelled, or cannot be confirmed
     */
    void onTaskCompleted(String queryId, String taskId);

    /**
     * Cancels the matching producer and drains its local operations. A missing/retired binding
     * is an idempotent no-op; an existing task bound to another query is rejected. Failed cleanup
     * remains owned for a later retry. This operation does not revoke a possibly completed remote
     * commit and must not remove published exchange data.
     *
     * @throws RuntimeException if identity verification or cleanup fails
     */
    void onTaskAborted(String queryId, String taskId, String reason);
}
