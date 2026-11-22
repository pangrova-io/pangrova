package org.pangrova.execution.binding;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.google.protobuf.ByteString;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.execution.identity.StageTaskIdentity;
import org.pangrova.execution.api.deadline.StageTaskDeadline;
import org.pangrova.execution.split.SchemaDescriptorDeduplicator;
import org.pangrova.execution.api.stage.TaskStage;
import org.pangrova.execution.api.enums.TaskPurpose;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.shuffle.api.ShuffleHandle;

/**
 * Builds SubmitTaskRequest instances for task dispatch.
 * Scan splits travel in the signed task request.
 */
public final class TaskRequestFactory {

    private static final long DEFAULT_TASK_TIMEOUT_SECONDS = 300L;
    private static final String POLICY_CONTEXT_PROPERTY =
            "pangrova.policy_context_hash";

    private TaskRequestFactory() {}

    /** Exact immutable Substrait bytes and digest shared across a bound stage's tasks. */
    public static final class FrozenStagePlan {
        private final ByteString bytes;
        private final Checksum checksum;

        public FrozenStagePlan(byte[] planBytes) {
            bytes = ByteString.copyFrom(Objects.requireNonNull(planBytes, "planBytes must not be null"));
            checksum = ProtocolChecksums.sha256(bytes);
        }
    }

    /** Builds the single fenced WRITE request used by distributed DML. */
    public static SubmitTaskRequest buildDmlWriteRequest(DmlWriteTask task) {
        QuerySession session = task.session();
        String taskId = UUID.randomUUID().toString();
        long deadline = deadlineEpochMs(task.taskTimeoutSeconds());
        List<OperatorBinding> bindings = bindOperatorBindings(
                task.operatorBindings(), deadline, session.queryId() + ":0:0");
        TaskRequest request = canonicalRequest(new CanonicalTaskSpec(
                taskId, new StageExecutionContext.Live(session), 0, 0, TaskKind.TASK_KIND_WRITE,
                task.plan(), task.splits(), bindings,
                bindResourceGrant(taskId, task.resourceGrant(), bindings),
                deadline, task.attempt(), task.exchangeInputs(),
                List.of(), TaskExecutionOptionsFactory.build(
                        session.sessionProperties(), task.writeConfig(), null,
                        false, null, 0L)));
        return SubmitTaskRequest.from(request);
    }

    public record DmlWriteTask(
            QuerySession session,
            FrozenStagePlan plan,
            StageResourceGrant resourceGrant,
            List<ExchangeInputBinding> exchangeInputs,
            List<OperatorBinding> operatorBindings,
            List<DataSplit> splits,
            WriteConfig writeConfig,
            long taskTimeoutSeconds,
            TaskAttemptDescriptor attempt) {

        public DmlWriteTask {
            session = Objects.requireNonNull(session, "session must not be null");
            plan = Objects.requireNonNull(plan, "plan must not be null");
            resourceGrant = Objects.requireNonNull(
                    resourceGrant, "resourceGrant must not be null");
            exchangeInputs = List.copyOf(exchangeInputs);
            operatorBindings = List.copyOf(operatorBindings);
            splits = List.copyOf(splits);
            writeConfig = Objects.requireNonNull(
                    writeConfig, "writeConfig must not be null");
            attempt = Objects.requireNonNull(attempt, "attempt must not be null");
        }
    }

    /** Builds a canonical fenced task from one immutable dispatch specification. */
    public static SubmitTaskRequest buildLeafTaskRequest(LeafTask task) {
        return buildLeafTaskRequest(task, java.util.OptionalInt.empty());
    }

    /** Collects the requested build-key range from the exact output of a live leaf producer. */
    public static SubmitTaskRequest buildLeafTaskRequest(LeafTask task, java.util.OptionalInt buildKeyOrdinal) {
        StageExecutionContext execution = task.execution();
        if (execution instanceof StageExecutionContext.Durable) {
            throw new IllegalArgumentException("A durable task requires an exact currently claimed partition");
        }
        if (buildKeyOrdinal.isPresent() && !(execution instanceof StageExecutionContext.Live)) {
            throw new IllegalArgumentException("Runtime-filter build key requires live execution");
        }
        TaskStage stage = task.stage();
        String taskId = execution instanceof StageExecutionContext.Deferred deferred
                ? deferredTaskId(deferred, task) : UUID.randomUUID().toString();
        long deadline = task.deadline().epochMillis();
        if (execution instanceof StageExecutionContext.Deferred deferred
                && deadline > deferred.lease().expiresAt().toEpochMilli()) {
            throw new IllegalArgumentException("Task deadline exceeds the exact durable partition lease");
        }
        List<OperatorBinding> bindings = bindOperatorBindings(
                task.operatorBindings(), deadline,
                execution.executionId() + ":" + stage.stageId() + ":" + task.partition());
        TaskRequest request = canonicalRequest(new CanonicalTaskSpec(
                taskId, execution, stage.stageId(), task.partition(),
                leafTaskKind(task), task.plan(), task.splits(),
                bindings, bindResourceGrant(taskId, task.resourceGrant(), bindings),
                deadline, task.attempt(), task.exchangeInputs(),
                task.resultColumnTypes(),
                TaskExecutionOptionsFactory.build(
                        execution.executionProperties(), null, task.shuffleConfig(),
                        execution instanceof StageExecutionContext.Live
                        && stage.purpose() == TaskPurpose.OUTPUT, task.shuffleHandle(),
                        task.attempt().planEpoch())), buildKeyOrdinal);
        return SubmitTaskRequest.from(request);
    }

    /** Immutable input for one leaf-stage task request. */
    public record LeafTask(
            StageExecutionContext execution,
            FrozenStagePlan plan,
            TaskStage stage,
            int partition,
            StageResourceGrant resourceGrant,
            List<ExchangeInputBinding> exchangeInputs,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig,
            List<DataSplit> splits,
            StageTaskDeadline deadline,
            TaskAttemptDescriptor attempt,
            List<OperatorBinding> operatorBindings,
            ShuffleHandle shuffleHandle,
            List<ResultColumnLogicalType> resultColumnTypes) {

        /** Validates and snapshots mutable request inputs. */
        public LeafTask {
            execution = Objects.requireNonNull(execution, "execution must not be null");
            plan = Objects.requireNonNull(plan, "plan must not be null");
            stage = Objects.requireNonNull(stage, "stage must not be null");
            resourceGrant = Objects.requireNonNull(resourceGrant, "resourceGrant must not be null");
            exchangeInputs = List.copyOf(exchangeInputs);
            deadline = Objects.requireNonNull(deadline, "deadline must not be null");
            splits = List.copyOf(splits);
            attempt = Objects.requireNonNull(attempt, "attempt must not be null");
            operatorBindings = List.copyOf(operatorBindings);
            resultColumnTypes = List.copyOf(resultColumnTypes);
        }
    }

    private static SecurityContext securityContext(
            StageExecutionContext execution, List<OperatorBinding> bindings) {
        String tenantId = execution.tenantId();
        String principal = execution.principal();
        if (!authenticatedIdentity(tenantId, principal)) {
            throw new IllegalArgumentException(
                    "Task dispatch requires an authenticated tenant and principal");
        }
        String policyContext = execution.executionProperties().getOrDefault(
                POLICY_CONTEXT_PROPERTY, "");
        boolean governedReuse = bindings.stream()
                .anyMatch(TaskRequestFactory::governedReuse);
        if (governedReuse && policyContext.isBlank()) {
            throw new IllegalArgumentException(
                    "Reusable operator dispatch requires a secured plan policy signature");
        }
        SecurityContext.Builder security = SecurityContext.newBuilder()
                .setTenantId(tenantId)
                .setPrincipal(principal)
                .setPolicyContextHash(policyContext);
        security.addAllActiveRoles(execution.activeRoles().stream().sorted().toList());
        return security.build();
    }

    private static boolean authenticatedIdentity(String tenantId, String principal) {
        return tenantId != null && !tenantId.isBlank()
                && principal != null && !principal.isBlank();
    }

    private static boolean governedReuse(OperatorBinding binding) {
        return binding.hasReusePolicy() && binding.getReusePolicy().getEnabled();
    }

    private static long deadlineEpochMs(long taskTimeoutSeconds) {
        long effectiveTimeoutSeconds = taskTimeoutSeconds > 0
                ? taskTimeoutSeconds
                : DEFAULT_TASK_TIMEOUT_SECONDS;
        long timeoutMillis = Math.multiplyExact(effectiveTimeoutSeconds, 1_000L);
        long deadlineEpochMs = Math.addExact(System.currentTimeMillis(), timeoutMillis);
        return deadlineEpochMs;
    }

    private static List<OperatorBinding> bindOperatorBindings(
            List<OperatorBinding> bindings,
            long deadlineEpochMs,
            String logicalTaskKey) {
        return bindings.stream().map(binding -> {
            var policy = binding.getExecutionPolicy().toBuilder()
                    .setDeadlineEpochMs(deadlineEpochMs);
            if (policy.getRetryClass()
                    == org.pangrova.protocol.v2.RetryClass
                            .RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY) {
                policy.setIdempotencyKey(
                        logicalTaskKey + ":" + binding.getPlanNodeId());
            }
            return binding.toBuilder().setExecutionPolicy(policy).build();
        }).toList();
    }

    private static TaskRequest canonicalRequest(CanonicalTaskSpec spec) {
        return canonicalRequest(spec, java.util.OptionalInt.empty());
    }

    private static TaskRequest canonicalRequest(CanonicalTaskSpec spec, java.util.OptionalInt buildKeyOrdinal) {
        FrozenStagePlan plan = spec.plan();
        var splitPlan = SchemaDescriptorDeduplicator.deduplicate(spec.splits());
        TaskRequest.Builder request = TaskRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId(spec.execution().executionId())
                        .setStageId(spec.stageId())
                        .setPartitionId(spec.partitionId())
                        .setPlanEpoch(spec.attempt().planEpoch())
                        .setAttemptId(spec.attempt().attemptId())
                        .setFencingToken(spec.attempt().fencingToken())
                        .setTaskId(spec.taskId()))
                .setTaskKind(spec.taskKind())
                .setTraceContext(traceContext(spec.execution()))
                .setSecurity(securityContext(
                        spec.execution(), spec.operatorBindings()))
                .setSubstraitPlan(plan.bytes)
                .setSubstraitPlanChecksum(plan.checksum)
                .addAllSplits(splitPlan.splits())
                .addAllOperatorBindings(spec.operatorBindings())
                .addAllResultColumnTypes(spec.resultColumnTypes())
                .setResourceGrant(spec.resourceGrant())
                .setDeadlineEpochMs(spec.deadlineEpochMs())
                .addAllExchangeInputs(exchangeInputs(
                        spec.exchangeInputs(), spec.deadlineEpochMs()));
        if (!spec.executionOptions().equals(TaskExecutionOptions.getDefaultInstance())) {
            request.setExecutionOptions(spec.executionOptions());
        }
        request.addAllSchemaDescriptors(splitPlan.descriptors());
        buildKeyOrdinal.ifPresent(request::setBuildKeyOrdinal);
        bindDeferredIdentity(request, spec.execution());
        return request.build();
    }

    private static org.pangrova.protocol.v2.TraceContext traceContext(StageExecutionContext execution) {
        var context = execution.traceContext();
        return org.pangrova.protocol.v2.TraceContext.newBuilder()
                .setTraceId(context.traceId()).setSpanId(context.spanId())
                .setTraceState(context.traceState()).setTraceFlags(context.traceFlags()).build();
    }

    private static List<ExchangeInputBinding> exchangeInputs(
            List<ExchangeInputBinding> bindings, long deadlineEpochMs) {
        if (bindings == null || bindings.isEmpty()) {
            return List.of();
        }
        return bindings.stream().map(binding -> {
            if (!binding.getNoMoreSources()
                    || binding.getExpectedSourcePartitions() == 0
                    || binding.getTransportCase()
                            == ExchangeInputBinding.TransportCase.TRANSPORT_NOT_SET) {
                throw new IllegalArgumentException(
                        "Exchange input is incomplete: " + binding.getExchangeId());
            }
            if (binding.hasExternal()) {
                return binding.toBuilder().setExternal(
                        binding.getExternal().toBuilder()
                                .setDeadlineEpochMs(deadlineEpochMs)).build();
            }
            return binding;
        }).toList();
    }

    private static TaskKind taskKind(TaskPurpose taskType) {
        return switch (taskType) {
            case COMPUTE, OUTPUT -> TaskKind.TASK_KIND_READ;
            case WRITE -> TaskKind.TASK_KIND_WRITE;
            case BACKGROUND_BUILD -> TaskKind.TASK_KIND_BACKGROUND_BUILD;
            case BACKGROUND_VALIDATE -> TaskKind.TASK_KIND_BACKGROUND_VALIDATE;
            case BACKGROUND_EXPORT -> TaskKind.TASK_KIND_BACKGROUND_EXPORT;
        };
    }

    private record CanonicalTaskSpec(
            String taskId,
            StageExecutionContext execution,
            int stageId,
            int partitionId,
            TaskKind taskKind,
            FrozenStagePlan plan,
            List<DataSplit> splits,
            List<OperatorBinding> operatorBindings,
            StageResourceGrant resourceGrant,
            long deadlineEpochMs,
            TaskAttemptDescriptor attempt,
            List<ExchangeInputBinding> exchangeInputs,
            List<ResultColumnLogicalType> resultColumnTypes,
            TaskExecutionOptions executionOptions) {
    }

    private static TaskKind leafTaskKind(LeafTask task) {
        // The issued task kind routes completion. Live shuffle producers use TASK_KIND_READ.
        if (task.execution() instanceof StageExecutionContext.Deferred) {
            var deferred = (StageExecutionContext.Deferred) task.execution();
            return switch (deferred.lease().kind()) {
                case RELATIONAL_COMPUTE -> TaskKind.TASK_KIND_SHUFFLE_WRITE;
                case RELATIONAL_PARTITION -> TaskKind.TASK_KIND_BACKGROUND_EXPORT;
                case SOURCE_MAP, DEPENDENCY_REDUCE, GENERATION_FINALIZE -> TaskKind.TASK_KIND_BACKGROUND_BUILD;
            };
        }
        return taskKind(task.stage().purpose());
    }

    private static String deferredTaskId(StageExecutionContext.Deferred deferred, LeafTask task) {
        var lease = deferred.lease();
        if (lease.stageId() != task.stage().stageId() || lease.partitionId() != task.partition()
                || lease.attemptId() != task.attempt().attemptId()
                    || lease.fencingToken() != task.attempt().fencingToken()) {
            throw new IllegalArgumentException("Deferred Leaf request changes the exact claimed partition or attempt");
        }
        return StageTaskIdentity.fromLease(lease);
    }

    private static void bindDeferredIdentity(TaskRequest.Builder request, StageExecutionContext execution) {
        if (execution instanceof StageExecutionContext.Deferred deferred) {
            var anchor = deferred.authority().executionAnchor();
            var owner = org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId())
                    .setOwnerIncarnation(anchor.ownerIncarnation()).setFencingToken(anchor.fencingToken()).build();
            request.setAttempt(request.getAttempt().toBuilder().setControlEpoch(deferred.lease().ownerEpoch())
                    .setExecutionOwner(owner));
            if (request.getExecutionOptions().hasShuffle()
                && request.getExecutionOptions().getShuffle().hasExecutionBinding()) {
                request.setExecutionOptions(request.getExecutionOptions().toBuilder().setShuffle(
                        request.getExecutionOptions().getShuffle().toBuilder().setExecutionBinding(
                                request.getExecutionOptions().getShuffle().getExecutionBinding()
                            .toBuilder().setExecutionOwner(owner))));
            }
        }
    }

    static StageResourceGrant bindResourceGrant(
            String taskId,
            StageResourceGrant template,
            List<OperatorBinding> operatorBindings) {
        validateResourceGrant(template, operatorBindings);
        CandidateBudget candidate = candidateBudget(operatorBindings);
        ProviderBudget provider = providerBudget(operatorBindings);
        if (provider.concurrency() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "operator provider concurrency exceeds protocol range");
        }
        if (provider.objectStoreReadBytes() > template.getObjectStoreReadBytes()
                || provider.objectStoreWriteBytes()
                        > template.getObjectStoreWriteBytes()) {
            throw new IllegalArgumentException(
                    "operator object-store requirements exceed the admitted stage grant");
        }
        var grant = template.toBuilder()
                .setGrantId(taskId + ":grant")
                .setCandidateRows(candidate.rows())
                .setFillKeys(candidate.fillKeys())
                .setProviderConcurrency(Math.toIntExact(provider.concurrency()))
                .setProviderItems(provider.items())
                .setProviderAttempts(provider.attempts())
                .setProviderRequestBytes(provider.requestBytes())
                .setProviderResponseBytes(provider.responseBytes())
                .setMediaBytes(Math.max(provider.mediaBytes(), template.getMediaBytes()));
        if (candidate.rows() > 0) {
            grant.setCandidateBytes(candidate.bytes() > 0
                    ? candidate.bytes() : template.getNativeMemoryBytes());
        }
        return grant.build();
    }

    private static void validateResourceGrant(
            StageResourceGrant template, List<OperatorBinding> bindings) {
        if (template == null || template.getCpuSlots() == 0
                || template.getNativeMemoryBytes() == 0) {
            throw new IllegalArgumentException(
                    "task dispatch requires a positive canonical resource grant");
        }
        if (bindings.stream().anyMatch(binding -> !binding.hasResources())) {
            throw new IllegalArgumentException(
                    "every typed operator requires a canonical resource envelope");
        }
    }

    private static CandidateBudget candidateBudget(List<OperatorBinding> bindings) {
        return new CandidateBudget(
                sum(bindings, binding -> binding.getResources().getCandidateRows()),
                sum(bindings, binding -> binding.getResources().getCandidateBytes()),
                sum(bindings, binding -> binding.getResources().getFillKeys()));
    }

    private static ProviderBudget providerBudget(List<OperatorBinding> bindings) {
        return new ProviderBudget(
                sum(bindings, binding -> binding.getResources().getProviderConcurrency()),
                sum(bindings, binding -> binding.getResources().getProviderItems()),
                sum(bindings, binding -> binding.getResources().getProviderAttempts()),
                sum(bindings, binding -> binding.getResources().getProviderRequestBytes()),
                sum(bindings, binding -> binding.getResources().getProviderResponseBytes()),
                sum(bindings, binding -> binding.getResources().getMediaBytes()),
                sum(bindings, binding -> binding.getResources().getObjectStoreReadBytes()),
                sum(bindings, binding -> binding.getResources().getObjectStoreWriteBytes()));
    }

    private record CandidateBudget(long rows, long bytes, long fillKeys) {}

    private record ProviderBudget(
            long concurrency,
            long items,
            long attempts,
            long requestBytes,
            long responseBytes,
            long mediaBytes,
            long objectStoreReadBytes,
            long objectStoreWriteBytes) {}

    private static long sum(
            List<OperatorBinding> bindings,
            java.util.function.ToLongFunction<OperatorBinding> value) {
        return bindings.stream().mapToLong(value).reduce(0L, Math::addExact);
    }

}
