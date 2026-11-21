package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.util.List;

/** Durable source commit events claimed and acknowledged under a maintenance lease. */
@Experimental
public interface IndexCommitLog {
    /** Records a commit event in the shared log. */
    default void recordCommitEvent(CommitEventRequest request) {
        throw new UnsupportedOperationException(
                "Index metadata store does not support durable commit events");
    }

    /** Claims a bounded, fair batch under a durable maintenance-lease epoch. */
    default List<CommitLogEntry> pollUnprocessedCommitEvents(int limit, long leaseEpoch) {
        throw new UnsupportedOperationException(
                "Index metadata store does not support durable commit-event polling");
    }

    /** Acknowledges only the claimed head event under the same lease epoch. */
    default void markCommitEventProcessed(long eventId, long leaseEpoch) {
        throw new UnsupportedOperationException(
                "Index metadata store does not support durable commit-event acknowledgement");
    }
}
