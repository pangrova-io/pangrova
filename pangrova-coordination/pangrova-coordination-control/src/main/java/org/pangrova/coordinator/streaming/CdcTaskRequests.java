package org.pangrova.coordinator.streaming;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.CdcIngestBinding;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;

/**
 * Builds the only canonical CDC task request admitted by the Worker boundary.
 *
 * <p>The request carries exact source ranges, the bound connector write output,
 * and the durable commit intent. It never carries a Substrait plan, splits,
 * operators, exchanges, runtime filters, or source credentials.</p>
 */
public final class CdcTaskRequests {

    private CdcTaskRequests() {}

    /** Creates and validates one exact CDC ingest task. */
    public static TaskRequest create(
            TaskAttemptId attempt,
            SecurityContext security,
            StageResourceGrant grant,
            long deadlineEpochMs,
            CdcIngestBinding binding) {
        TaskRequest request = TaskRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(attempt)
                .setTaskKind(TaskKind.TASK_KIND_CDC_INGEST)
                .setSecurity(security)
                .setResourceGrant(grant)
                .setDeadlineEpochMs(deadlineEpochMs)
                .setCdcIngest(binding)
                .build();
        CanonicalTaskRequests.validateForAdmission(request);
        return request;
    }
}
