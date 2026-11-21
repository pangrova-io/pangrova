package org.pangrova.streaming.cdc;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

/**
 * Exact inclusive-start, exclusive-end source interval for one Worker CDC task.
 *
 * <p>This is the execution form of the canonical {@code CdcSourceRange}; it never
 * reopens an interval after the Coordinator admits the task.</p>
 */
@Experimental
public record CdcTaskRange(String topic, int partition, long startOffset, long endOffset) {

    public CdcTaskRange {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("CDC task range requires a topic");
        }
        if (partition < 0 || startOffset < 0 || endOffset <= startOffset) {
            throw new IllegalArgumentException("CDC task range is not a bounded topic partition interval");
        }
    }

    public TopicPartitionOffset partitionKey() {
        return new TopicPartitionOffset(topic, partition);
    }

    public boolean contains(long offset) {
        return offset >= startOffset && offset < endOffset;
    }
}
