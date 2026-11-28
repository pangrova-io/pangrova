package org.pangrova.protocol.task;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.http.TaskRuntimeFilterDelivery;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TaskAttemptId;

/** Bounded HTTP encoding of a runtime filter scoped to one exact task attempt. */
public final class TaskRuntimeFilterWire {
    public static final String MEDIA_TYPE = "application/x-protobuf";
    public static final int MAX_BODY_BYTES = 64 * 1024;

    private TaskRuntimeFilterWire() {}

    /** Validates and serializes one exact Worker delivery. */
    public static byte[] encode(TaskAttemptId attempt, InjectRuntimeFilterRequest update) {
        var delivery = TaskRuntimeFilterDelivery.newBuilder().setAttempt(attempt).setUpdate(update).build();
        validate(delivery);
        return delivery.toByteArray();
    }

    /** Reads the bounded HTTP message and validates its exact delivery identity. */
    public static TaskRuntimeFilterDelivery decode(byte[] bytes) throws InvalidProtocolBufferException {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("Runtime filter delivery has invalid size");
        }
        var delivery = TaskRuntimeFilterDelivery.parseFrom(bytes);
        validate(delivery);
        return delivery;
    }

    private static void validate(TaskRuntimeFilterDelivery delivery) {
        if (delivery.getSerializedSize() > MAX_BODY_BYTES || !delivery.hasAttempt()
                || !delivery.getAttempt().hasWorkerExecutionIdentity() || !delivery.hasUpdate()
                || !delivery.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Runtime filter requires a bounded exact Worker delivery");
        }
        CanonicalTaskRequests.validateAttempt(delivery.getAttempt());
        var update = delivery.getUpdate();
        if (update.getPlanNodeId().isBlank() || update.getFiltersCount() == 0
                || update.getFiltersCount() > CanonicalTaskRequests.MAX_BUILD_KEY_COLUMNS
                || !update.getUnknownFields().asMap().isEmpty()
                || update.getFiltersList().stream().anyMatch(TaskRuntimeFilterWire::invalidFilter)) {
            throw new IllegalArgumentException("Runtime filter update is missing its scan or typed predicates");
        }
    }

    private static boolean invalidFilter(RuntimeFilter filter) {
        return filter.getColumnName().isBlank()
                || filter.getScope() != RuntimeFilterScope.RUNTIME_FILTER_SCOPE_ROW_FILTER
                || filter.getPredicateCase() == RuntimeFilter.PredicateCase.PREDICATE_NOT_SET
                || !filter.getUnknownFields().asMap().isEmpty();
    }
}
