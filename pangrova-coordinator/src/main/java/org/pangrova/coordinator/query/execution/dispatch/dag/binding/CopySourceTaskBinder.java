package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import com.google.protobuf.ByteString;
import org.pangrova.metadata.storage.JdbcCopySourceReadAdmissions;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.spi.coordination.ExecutionAnchor;

import java.util.HexFormat;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/** Binds a query-owned frozen source to the final admitted task, without reading source data. */
public final class CopySourceTaskBinder implements UnaryOperator<SubmitTaskRequest> {
    private final JdbcCopySourceReadAdmissions admissions;
    private final Function<String, ExecutionAnchor> anchors;

    public CopySourceTaskBinder(JdbcCopySourceReadAdmissions admissions, Function<String, ExecutionAnchor> anchors) {
        this.admissions = Objects.requireNonNull(admissions, "admissions");
        this.anchors = Objects.requireNonNull(anchors, "anchors");
    }

    @Override
    public SubmitTaskRequest apply(SubmitTaskRequest request) {
        Objects.requireNonNull(request, "request");
        if (request.copySourceRead() != null) return request;
        var canonical = request.taskRequest();
        boolean copy = canonical.getSplitsList().stream().anyMatch(split -> "copy".equals(split.getConnectorId()));
        if (!copy) return request;
        if (canonical.getSplitsCount() != 1 || canonical.hasWorkerLocalCopyInput()
                || (canonical.getTaskKind() != TaskKind.TASK_KIND_READ
                    && canonical.getTaskKind() != TaskKind.TASK_KIND_WRITE)) {
            throw new IllegalArgumentException("COPY task requires one frozen relational source");
        }
        var attempt = canonical.getAttempt();
        var anchor = Objects.requireNonNull(anchors.apply(attempt.getQueryOrJobId()), "execution anchor");
        requireCurrentOwner(attempt, anchor);
        var split = canonical.getSplits(0);
        var bound = admissions.issueFrozenInput(new JdbcCopySourceReadAdmissions.FrozenInputAttempt(
                canonical.getSecurity().getTenantId(), canonical.getSecurity().getPrincipal(),
                attempt.getQueryOrJobId(), split.getSourceId(), attempt.getTaskId(),
                attempt.getControlEpoch(), canonical.getDeadlineEpochMs()), anchor);
        var scope = bound.scope();
        requireFrozenSplit(split, bound);
        var wire = new CopySourceReadHttp.Scope(scope.tenantId(), scope.principal(), scope.profileName(),
                scope.profileIncarnationId(), scope.profileRevision(), scope.canonicalSourceUrl(),
                scope.managedStorageRootUri(), scope.managedBackendIdentitySha256(), scope.executionId(),
                scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(), scope.controlEpoch(),
                scope.ownerFencingToken(), scope.deadlineEpochMs());
        var task = canonical.toBuilder().setCopySourceStagingCommandSha256(Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(wire.commandSha256())))).build();
        return SubmitTaskRequest.from(task, new CopySourceReadHttp.RedeemRequest(bound.ticket().nonce(), wire));
    }

    private static void requireCurrentOwner(org.pangrova.protocol.v2.TaskAttemptId attempt, ExecutionAnchor anchor) {
        var owner = attempt.getExecutionOwner();
        if (!attempt.hasExecutionOwner() || !anchor.ownerId().equals(owner.getOwnerId())
                || !anchor.ownerIncarnation().equals(owner.getOwnerIncarnation())
                || anchor.fencingToken() != owner.getFencingToken()) {
            throw new IllegalStateException("COPY task final execution owner is not current");
        }
    }

    private static void requireFrozenSplit(org.pangrova.protocol.v2.DataSplit split,
                                           JdbcCopySourceReadAdmissions.FrozenInput bound) {
        if (!bound.scope().canonicalSourceUrl().equals(split.getDataObjectId())
                || !bound.scope().canonicalSourceUrl().equals(split.getOpaqueLocation())
                || !bound.proof().sourceVersionId().equals(split.getSourceSnapshot())
                || bound.proof().contentSize() != split.getLength() || split.getStartOffset() != 0) {
            throw new IllegalArgumentException("COPY split differs from its frozen source proof");
        }
    }
}
