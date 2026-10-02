package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageTaskIdentity;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageTaskDeadline;
import org.pangrova.coordinator.query.execution.dispatch.dag.split.CanonicalSplitParser;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.protocol.v2.RuntimeFilterBinding;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.shuffle.ShuffleHandle;

/**
 * Builds SubmitTaskRequest instances for task dispatch.
 * Supports push mode (splits embedded in request) and pull mode (empty splits).
 */
public final class TaskRequestFactory {

    private static final long DEFAULT_TASK_TIMEOUT_SECONDS = 300L;
    private static final String POLICY_CONTEXT_PROPERTY =
            "pangrova.policy_context_hash";

    private TaskRequestFactory() {}

    /** Builds the single fenced WRITE request used by distributed DML. */
    static SubmitTaskRequest buildDmlWriteRequest(DmlWriteTask task) {
        CoordinatorSessionContext session = task.session();
        String taskId = UUID.randomUUID().toString();
        long deadline = deadlineEpochMs(task.taskTimeoutSeconds());
        List<OperatorBinding> bindings = bindOperatorBindings(
                task.operatorBindings(), deadline, session.queryId() + ":0:0");
        TaskRequest request = canonicalRequest(new CanonicalTaskSpec(
                taskId, new StageExecutionContext.Live(session), 0, 0, TaskKind.TASK_KIND_WRITE,
                task.planBytes(), task.serializedSplits(), bindings,
                bindResourceGrant(taskId, task.resourceGrant(), bindings),
                deadline, task.attempt(), task.exchangeInputs(),
                task.dynamicFilters(), List.of(), TaskExecutionOptionsFactory.build(
                        session.sessionProperties(), task.writeConfig(), null,
                        false, null, 0L)));
        return SubmitTaskRequest.from(request);
    }

    record DmlWriteTask(
            CoordinatorSessionContext session,
            byte[] planBytes,
            StageResourceGrant resourceGrant,
            List<ExchangeInputBinding> exchangeInputs,
            List<OperatorBinding> operatorBindings,
            List<DynamicFilterPayload> dynamicFilters,
            List<String> serializedSplits,
            WriteConfig writeConfig,
            long taskTimeoutSeconds,
            TaskAttemptDescriptor attempt) {

        DmlWriteTask {
            session = Objects.requireNonNull(session, "session must not be null");
            planBytes = Objects.requireNonNull(
                    planBytes, "planBytes must not be null").clone();
            resourceGrant = Objects.requireNonNull(
                    resourceGrant, "resourceGrant must not be null");
            exchangeInputs = List.copyOf(exchangeInputs);
            operatorBindings = List.copyOf(operatorBindings);
            dynamicFilters = List.copyOf(dynamicFilters);
            serializedSplits = List.copyOf(serializedSplits);
            writeConfig = Objects.requireNonNull(
                    writeConfig, "writeConfig must not be null");
            attempt = Objects.requireNonNull(attempt, "attempt must not be null");
        }

        @Override
        public byte[] planBytes() {
            return planBytes.clone();
        }
    }

    /** Builds a canonical fenced task from one immutable dispatch specification. */
    public static SubmitTaskRequest buildLeafTaskRequest(LeafTask task) {
        StageExecutionContext execution = task.execution();
        if (execution instanceof StageExecutionContext.Durable) {
            throw new IllegalArgumentException("A durable task requires an exact currently claimed partition");
        }
        StageNode stage = task.stage();
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
                leafTaskKind(task), task.planBytes(), task.serializedSplits(),
                bindings, bindResourceGrant(taskId, task.resourceGrant(), bindings),
                deadline, task.attempt(), task.exchangeInputs(), task.dynamicFilters(),
                task.resultColumnTypes(),
                TaskExecutionOptionsFactory.build(
                        execution.executionProperties(), null, task.shuffleConfig(),
                        execution instanceof StageExecutionContext.Live && "OUTPUT".equals(stage.taskType()), task.shuffleHandle(),
                        task.attempt().planEpoch())));
        return SubmitTaskRequest.from(request);
    }

    /** Immutable input for one leaf-stage task request. */
    public record LeafTask(
            StageExecutionContext execution,
            byte[] planBytes,
            StageNode stage,
            int partition,
            StageResourceGrant resourceGrant,
            List<ExchangeInputBinding> exchangeInputs,
            List<DynamicFilterPayload> dynamicFilters,
            org.pangrova.protocol.rest.config.ShuffleConfig shuffleConfig,
            List<String> serializedSplits,
            StageTaskDeadline deadline,
            TaskAttemptDescriptor attempt,
            List<OperatorBinding> operatorBindings,
            ShuffleHandle shuffleHandle,
            List<ResultColumnLogicalType> resultColumnTypes) {

        /** Validates and snapshots mutable request inputs. */
        public LeafTask {
            execution = Objects.requireNonNull(execution, "execution must not be null");
            planBytes = Objects.requireNonNull(planBytes, "planBytes must not be null").clone();
            stage = Objects.requireNonNull(stage, "stage must not be null");
            resourceGrant = Objects.requireNonNull(resourceGrant, "resourceGrant must not be null");
            exchangeInputs = List.copyOf(exchangeInputs);
            dynamicFilters = List.copyOf(dynamicFilters);
            deadline = Objects.requireNonNull(deadline, "deadline must not be null");
            serializedSplits = serializedSplits == null ? List.of() : List.copyOf(serializedSplits);
            attempt = Objects.requireNonNull(attempt, "attempt must not be null");
            operatorBindings = List.copyOf(operatorBindings);
            resultColumnTypes = List.copyOf(resultColumnTypes);
        }

        @Override
        public byte[] planBytes() {
            return planBytes.clone();
        }
    }

    /**
     * Validates that multi-source splits carry generic source identity at dispatch time.
     * Fails fast before sending to a Worker if any split lacks {@code source_id}.
     *
     * @param serializedSplits Base64-encoded canonical v2 DataSplit bytes
     * @param queryId          query identifier for error context
     * @param stageId          stage identifier for error context
     */
    public static void validateSplitSources(List<String> serializedSplits,
                                            String queryId, int stageId) {
        CanonicalSplitParser.parseAll(serializedSplits, queryId, stageId);
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
        byte[] planBytes = spec.planBytes();
        TaskRequest.Builder request = TaskRequest.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId(spec.execution().executionId())
                        .setStageId(spec.stageId())
                        .setPartitionId(spec.partitionId())
                        .setPlanEpoch(spec.attempt().planEpoch())
                        .setAttemptId(spec.attempt().attemptId())
                        .setFencingToken(spec.attempt().fencingToken())
                        .setTaskId(spec.taskId()))
                .setTaskKind(spec.taskKind())
                .setSecurity(securityContext(
                        spec.execution(), spec.operatorBindings()))
                .setSubstraitPlan(ByteString.copyFrom(planBytes))
                .setSubstraitPlanChecksum(CanonicalTaskRequests.sha256(planBytes))
                .addAllSplits(CanonicalSplitParser.parseAll(
                        spec.serializedSplits(), spec.execution().executionId(), spec.stageId()))
                .addAllOperatorBindings(spec.operatorBindings())
                .addAllResultColumnTypes(spec.resultColumnTypes())
                .setResourceGrant(spec.resourceGrant())
                .setDeadlineEpochMs(spec.deadlineEpochMs())
                .addAllExchangeInputs(exchangeInputs(
                        spec.exchangeInputs(), spec.deadlineEpochMs()))
                .addAllInitialRuntimeFilters(runtimeFilters(spec.dynamicFilters()));
        if (!spec.executionOptions().equals(TaskExecutionOptions.getDefaultInstance())) {
            request.setExecutionOptions(spec.executionOptions());
        }
        bindDeferredIdentity(request, spec.execution());
        return request.build();
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

    private static List<RuntimeFilterBinding> runtimeFilters(
            List<DynamicFilterPayload> filters) {
        if (filters == null || filters.isEmpty()) {
            return List.of();
        }
        return filters.stream().map(TaskRequestFactory::runtimeFilter).toList();
    }

    private static RuntimeFilterBinding runtimeFilter(DynamicFilterPayload payload) {
        final InjectRuntimeFilterRequest decoded;
        try {
            decoded = InjectRuntimeFilterRequest.parseFrom(payload.filterData());
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "Invalid canonical runtime filter: " + payload.filterId(), failure);
        }
        if (decoded.getFiltersCount() == 0) {
            throw new IllegalArgumentException(
                    "Runtime filter must contain at least one predicate: "
                            + payload.filterId());
        }
        String planNodeId = decoded.getPlanNodeId().isBlank()
                ? "scan_" + decoded.getFilters(0).getColumnName()
                : decoded.getPlanNodeId();
        return RuntimeFilterBinding.newBuilder()
                .setPlanNodeId(planNodeId)
                .addAllFilters(decoded.getFiltersList())
                .build();
    }

    private static TaskKind taskKind(String taskType) {
        return switch (taskType) {
            case "COMPUTE", "OUTPUT" -> TaskKind.TASK_KIND_READ;
            case "WRITE" -> TaskKind.TASK_KIND_WRITE;
            case "BACKGROUND_BUILD" -> TaskKind.TASK_KIND_BACKGROUND_BUILD;
            case "BACKGROUND_VALIDATE" -> TaskKind.TASK_KIND_BACKGROUND_VALIDATE;
            case "BACKGROUND_EXPORT" -> TaskKind.TASK_KIND_BACKGROUND_EXPORT;
            default -> throw new IllegalArgumentException(
                    "Unsupported task type: " + taskType);
        };
    }

    private record CanonicalTaskSpec(
            String taskId,
            StageExecutionContext execution,
            int stageId,
            int partitionId,
            TaskKind taskKind,
            byte[] planBytes,
            List<String> serializedSplits,
            List<OperatorBinding> operatorBindings,
            StageResourceGrant resourceGrant,
            long deadlineEpochMs,
            TaskAttemptDescriptor attempt,
            List<ExchangeInputBinding> exchangeInputs,
            List<DynamicFilterPayload> dynamicFilters,
            List<ResultColumnLogicalType> resultColumnTypes,
            TaskExecutionOptions executionOptions) {

        private CanonicalTaskSpec {
            planBytes = planBytes.clone();
        }

        @Override
        public byte[] planBytes() {
            return planBytes.clone();
        }
    }

    private static TaskKind leafTaskKind(LeafTask task) {
        if (task.execution() instanceof StageExecutionContext.Deferred) {
            var deferred = (StageExecutionContext.Deferred) task.execution();
            return switch (deferred.lease().kind()) {
                case RELATIONAL_COMPUTE -> TaskKind.TASK_KIND_SHUFFLE_WRITE;
                case RELATIONAL_PARTITION -> TaskKind.TASK_KIND_BACKGROUND_EXPORT;
                case SOURCE_MAP, DEPENDENCY_REDUCE, GENERATION_FINALIZE -> TaskKind.TASK_KIND_BACKGROUND_BUILD;
            };
        }
        return taskKind(task.stage().taskType());
    }

    private static String deferredTaskId(StageExecutionContext.Deferred deferred, LeafTask task) {
        var lease = deferred.lease();
        if (lease.stageId() != task.stage().stageId() || lease.partitionId() != task.partition()
                || lease.attemptId() != task.attempt().attemptId() || lease.fencingToken() != task.attempt().fencingToken()) {
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
            if (request.getExecutionOptions().hasShuffle() && request.getExecutionOptions().getShuffle().hasExecutionBinding()) {
                request.setExecutionOptions(request.getExecutionOptions().toBuilder().setShuffle(
                        request.getExecutionOptions().getShuffle().toBuilder().setExecutionBinding(
                                request.getExecutionOptions().getShuffle().getExecutionBinding().toBuilder().setExecutionOwner(owner))));
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
                .setMediaBytes(provider.mediaBytes());
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
