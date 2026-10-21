package org.pangrova.protocol.task;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.google.protobuf.ByteString;

import org.pangrova.protocol.descriptor.CanonicalProtocolDescriptors;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ProtocolVersion;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.ExecutionCallbackContext;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;

/** Canonical v2 task construction and Worker-admission validation. */
public final class CanonicalTaskRequests {

    public static final int PROTOCOL_MAJOR = 2;
    public static final int PROTOCOL_MINOR = 0;
    public static final String ROLLOUT_EPOCH = "ai-native-function-resource-r3";
    public static final int MAX_COVERAGE_IDENTITIES = 4_096;
    private static final int SHA256_BYTES = 32;

    private CanonicalTaskRequests() {}

    /** Returns the exact protocol identity embedded in every task request. */
    public static ProtocolVersion protocolVersion() {
        return ProtocolVersion.newBuilder()
                .setMajor(PROTOCOL_MAJOR)
                .setMinor(PROTOCOL_MINOR)
                .setDescriptorSetSha256(ByteString.copyFrom(
                        CanonicalProtocolDescriptors.sha256()))
                .setRolloutEpoch(ROLLOUT_EPOCH)
                .build();
    }

    /** Returns a SHA-256 checksum for immutable task payload bytes. */
    public static Checksum sha256(byte[] bytes) {
        try {
            return Checksum.newBuilder()
                    .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                    .setDigest(ByteString.copyFrom(
                            MessageDigest.getInstance("SHA-256").digest(bytes)))
                    .build();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    /** Validates canonical usage against its exact grant in Java's representable accounting domain. */
    public static boolean usageWithinGrant(ResourceUsage usage, StageResourceGrant grant) {
        return usage.getCpuNanos() >= 0 && memoryWithinGrant(usage, grant) && exchangeWithinGrant(usage, grant)
                && invocationWithinGrant(usage, grant);
    }

    private static boolean memoryWithinGrant(ResourceUsage usage, StageResourceGrant grant) {
        return withinBudget(usage.getPeakNativeMemoryBytes(), grant.getNativeMemoryBytes())
                && withinBudget(usage.getPeakJvmHeapBytes(), grant.getJvmHeapBytes())
                && withinBudget(usage.getSpillBytes(), grant.getSpillBytes())
                && withinBudget(usage.getCandidateRows(), grant.getCandidateRows())
                && withinBudget(usage.getCandidateBytes(), grant.getCandidateBytes());
    }

    private static boolean exchangeWithinGrant(ResourceUsage usage, StageResourceGrant grant) {
        return withinBudget(usage.getShuffleSendBytes(), grant.getShuffleSendBytes())
                && withinBudget(usage.getShuffleReceiveBytes(), grant.getShuffleReceiveBytes())
                && withinBudget(usage.getObjectStoreReadBytes(), grant.getObjectStoreReadBytes())
                && withinBudget(usage.getObjectStoreWriteBytes(), grant.getObjectStoreWriteBytes());
    }

    private static boolean invocationWithinGrant(ResourceUsage usage, StageResourceGrant grant) {
        return withinBudget(usage.getProviderItems(), grant.getProviderItems())
                && withinBudget(usage.getProviderAttempts(), grant.getProviderAttempts())
                && withinBudget(usage.getProviderRequestBytes(), grant.getProviderRequestBytes())
                && withinBudget(usage.getProviderResponseBytes(), grant.getProviderResponseBytes())
                && usage.getObservedProviderResponseBytes() >= usage.getProviderResponseBytes()
                && withinBudget(usage.getMediaBytes(), grant.getMediaBytes());
    }

    private static boolean withinBudget(long value, long limit) {
        return value >= 0 && limit >= 0 && value <= limit;
    }

    /** Validates the same admitted task authority used by every Native callback. */
    public static ExecutionCallbackContext callbackFor(TaskRequest request) {
        validateForAdmission(request);
        var callback = ExecutionCallbackContext.newBuilder()
                .setProtocol(request.getProtocol()).setSecurity(request.getSecurity())
                .setAttempt(request.getAttempt()).setResourceGrant(request.getResourceGrant())
                .setDeadlineEpochMs(request.getDeadlineEpochMs());
        if (request.hasTraceContext()) {
            callback.setTraceContext(request.getTraceContext());
        }
        var context = callback.build();
        validateCallback(context);
        return context;
    }

    /** Validates the same admitted task authority used by every Native callback. */
    public static void validateCallback(ExecutionCallbackContext context) {
        if (context == null || !context.getUnknownFields().asMap().isEmpty()
                || !context.hasProtocol() || !context.hasAttempt() || !context.hasSecurity()
                || !context.hasResourceGrant()) {
            throw new IllegalArgumentException("Callback task authority is absent or contains unknown fields");
        }
        validateProtocol(context.getProtocol());
        validateAttempt(context.getAttempt());
        requireSecurityContext(context.getSecurity());
        requireDeadline(context.getDeadlineEpochMs());
        requireResourceGrant(context.getResourceGrant());
        if (!context.getTraceContext().getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Callback trace context contains unknown fields");
        }
    }

    private static void requireResourceGrant(StageResourceGrant grant) {
        if (grant.getGrantId().isBlank() || grant.getCpuSlots() <= 0 || grant.getNativeMemoryBytes() <= 0
                || !grant.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Canonical task resource grant is incomplete");
        }
        for (var field : grant.getDescriptorForType().getFields()) {
            Object value = grant.getField(field);
            if ((value instanceof Long longValue && longValue < 0)
                    || (value instanceof Integer integerValue && integerValue < 0)) {
                throw new IllegalArgumentException("Resource grant exceeds Java accounting domain: " + field.getName());
            }
        }
    }

    /**
     * Validates all identity and integrity fields required before Worker
     * admission. Native independently validates the same canonical request.
     */
    public static void validateForAdmission(TaskRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("TaskRequest must not be null");
        }
        if (request.getWorkerLocalStorageBindingsCount() != 0 || request.hasWorkerLocalCopyInput()) {
            throw new IllegalArgumentException("Network tasks must not contain Worker-local storage bindings");
        }
        validateProtocol(request.getProtocol());
        validateAttempt(request.getAttempt());
        requireTaskKind(request);
        validateCopySourceStagingCommand(request);
        requireSecurityContext(request.getSecurity());
        requireValidPlanChecksum(request);
        requireDeadline(request.getDeadlineEpochMs());
        requireResourceGrant(request.getResourceGrant());
        validateSerializedOutputGrant(request);
        validateShuffleExecution(request);
        validateUniqueBindings(request);
        validateCoverageIdentityBounds(request);
        validateExchangeInputs(request);
        validateRuntimeFilters(request);
    }

    private static void validateSerializedOutputGrant(TaskRequest request) {
        var options = request.getExecutionOptions();
        if ((options.getDirectResultOutput() || options.hasShuffle())
                && request.getResourceGrant().getShuffleBufferBytes() <= 0) {
            throw new IllegalArgumentException("Serialized output requires a positive serialized-buffer grant");
        }
    }

    static void validateCoverageIdentityBounds(TaskRequest request) {
        HashSet<SourceKey> sources = new HashSet<>();
        for (var split : request.getSplitsList()) {
            sources.add(new SourceKey(split.getSourceId(), split.getSourceSnapshot()));
            if (sources.size() > MAX_COVERAGE_IDENTITIES) {
                throw new IllegalArgumentException(
                        "TaskRequest has too many source snapshot coverage identities");
            }
        }
        long artifacts = 0;
        for (var binding : request.getOperatorBindingsList()) {
            artifacts += binding.getArtifactsCount();
            if (artifacts > MAX_COVERAGE_IDENTITIES) {
                throw new IllegalArgumentException(
                        "TaskRequest has too many artifact coverage identities");
            }
        }
    }

    private record SourceKey(String id, String snapshot) {}

    private static void requireTaskKind(TaskRequest request) {
        if (request.getTaskKindValue() == 0) {
            throw new IllegalArgumentException("TaskRequest task_kind is required");
        }
    }

    private static void validateCopySourceStagingCommand(TaskRequest request) {
        if (!request.hasCopySourceStagingCommandSha256()) {
            return;
        }
        var checksum = request.getCopySourceStagingCommandSha256();
        boolean sourceOrWrite = request.getTaskKind() == org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ
                || request.getTaskKind() == org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE;
        if (!sourceOrWrite
                || checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != SHA256_BYTES
                || !checksum.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException(
                    "COPY staging command requires a source-read or write task and SHA-256 digest");
        }
    }

    private static void requireSecurityContext(SecurityContext security) {
        if (security.getTenantId().isBlank() || security.getPrincipal().isBlank()
                || !security.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException(
                    "TaskRequest tenant and principal are required");
        }
    }

    private static void requireValidPlanChecksum(TaskRequest request) {
        if (request.getSubstraitPlan().isEmpty()
                || !request.hasSubstraitPlanChecksum()
                || request.getSubstraitPlanChecksum().getAlgorithm()
                        != Checksum.Algorithm.ALGORITHM_SHA256
                || !hasMatchingPlanDigest(request)) {
            throw new IllegalArgumentException(
                    "TaskRequest Substrait plan checksum is invalid");
        }
    }

    private static boolean hasMatchingPlanDigest(TaskRequest request) {
        return Arrays.equals(
                sha256(request.getSubstraitPlan().toByteArray()).getDigest().toByteArray(),
                request.getSubstraitPlanChecksum().getDigest().toByteArray());
    }

    private static void requireDeadline(long deadlineEpochMs) {
        if (deadlineEpochMs <= 0) {
            throw new IllegalArgumentException("TaskRequest deadline is required");
        }
    }

    private static void validateShuffleExecution(TaskRequest request) {
        if (!request.hasExecutionOptions()
                || !request.getExecutionOptions().hasShuffle()) {
            return;
        }
        var shuffle = request.getExecutionOptions().getShuffle();
        validateShuffleOptions(shuffle);
        boolean remote = shuffle.getPersistenceMode()
                == org.pangrova.protocol.v2.ShufflePersistenceMode
                        .SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE;
        if (remote != shuffle.hasExecutionBinding()) {
            throw invalidShuffleBinding();
        }
        if (remote) {
            validateRemoteShuffleBinding(request, shuffle.getExecutionBinding());
        }
    }

    private static void validateShuffleOptions(
            org.pangrova.protocol.v2.ShuffleTaskOptions shuffle) {
        if (shuffle.getSerde() != org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH
                || shuffle.getPartitionCount() == 0
                || !List.of("none", "lz4", "zstd").contains(shuffle.getCompressionCodec())
                || shuffle.getBatchSize() <= 0
                || shuffle.getPersistenceModeValue() == 0) {
            throw invalidShuffleBinding();
        }
    }

    private static void validateRemoteShuffleBinding(
            TaskRequest request,
            org.pangrova.protocol.v2.ShuffleExecutionBinding binding) {
        if (binding.getShuffleId().isBlank()
                || binding.getEndpointsCount() == 0
                || binding.getPlanEpoch() != request.getAttempt().getPlanEpoch()
                || !binding.hasExecutionOwner()
                || !binding.getExecutionOwner().equals(
                        request.getAttempt().getExecutionOwner())
                || binding.getEndpointsList().stream().anyMatch(endpoint ->
                        endpoint.getHost().isBlank() || endpoint.getPort() <= 0)) {
            throw invalidShuffleBinding();
        }
    }

    private static IllegalArgumentException invalidShuffleBinding() {
        return new IllegalArgumentException(
                "TaskRequest shuffle execution binding is incomplete or inconsistent");
    }

    static void validateProtocol(ProtocolVersion protocol) {
        if (!protocol.getUnknownFields().asMap().isEmpty()
                || protocol.getMajor() != PROTOCOL_MAJOR
                || protocol.getMinor() != PROTOCOL_MINOR
                || !ROLLOUT_EPOCH.equals(protocol.getRolloutEpoch())
                || !Arrays.equals(CanonicalProtocolDescriptors.sha256(),
                        protocol.getDescriptorSetSha256().toByteArray())) {
            throw new IllegalArgumentException(
                    "TaskRequest protocol identity does not match canonical v2");
        }
    }

    static void validateAttempt(TaskAttemptId attempt) {
        if (attempt.getQueryOrJobId().isBlank()
                || attempt.getTaskId().isBlank()
                || attempt.getPlanEpoch() <= 0
                || attempt.getFencingToken() <= 0
                || attempt.getControlEpoch() <= 0
                || attempt.getStageId() < 0 || attempt.getPartitionId() < 0 || attempt.getAttemptId() < 0
                || !attempt.getUnknownFields().asMap().isEmpty()) {
            throw incompleteAttempt();
        }
        validateExecutionOwner(attempt);
    }

    private static void validateExecutionOwner(
            org.pangrova.protocol.v2.TaskAttemptId attempt) {
        if (!attempt.hasExecutionOwner()
                || attempt.getExecutionOwner().getOwnerId().isBlank()
                || attempt.getExecutionOwner().getOwnerIncarnation().isBlank()
                || attempt.getExecutionOwner().getFencingToken() <= 0
                || !attempt.getExecutionOwner().getUnknownFields().asMap().isEmpty()) {
            throw incompleteAttempt();
        }
    }

    private static IllegalArgumentException incompleteAttempt() {
        return new IllegalArgumentException(
                "TaskRequest attempt and execution-owner identity are incomplete");
    }

    private static void validateUniqueBindings(TaskRequest request) {
        HashSet<String> planNodeIds = new HashSet<>();
        for (OperatorBinding binding : request.getOperatorBindingsList()) {
            if (binding.getPlanNodeId().isBlank()
                    || !planNodeIds.add(binding.getPlanNodeId())) {
                throw new IllegalArgumentException(
                        "TaskRequest operator binding identity is blank or duplicated");
            }
        }
    }

    private static void validateExchangeInputs(TaskRequest request) {
        HashSet<String> inputIds = new HashSet<>();
        for (var input : request.getExchangeInputsList()) {
            String inputId = input.getPlanNodeId() + '\u0000' + input.getExchangeId();
            if (!isValidExchangeInput(input, inputId, inputIds)) {
                throw new IllegalArgumentException(
                        "TaskRequest exchange input is incomplete or duplicated");
            }
            if (input.hasDirect()) {
                HashSet<String> producers = new HashSet<>();
                for (var source : input.getDirect().getSourcesList()) {
                    String producer = source.getSourceStageId() + ":"
                            + source.getSourcePartitionId();
                    if (!isValidExchangeSource(source, producer, producers)) {
                        throw new IllegalArgumentException(
                                "TaskRequest exchange source is incomplete or duplicated");
                    }
                }
            } else if (!validExternalInput(input.getExternal())) {
                throw new IllegalArgumentException(
                        "TaskRequest external shuffle input is incomplete");
            }
        }
    }

    private static boolean isValidExchangeInput(
            org.pangrova.protocol.v2.ExchangeInputBinding input,
            String inputId,
            HashSet<String> inputIds) {
        return !input.getPlanNodeId().isBlank()
                && !input.getExchangeId().isBlank()
                && inputIds.add(inputId)
                && input.getExpectedSourcePartitions() != 0
                && input.getTransportCase()
                        != org.pangrova.protocol.v2.ExchangeInputBinding
                                .TransportCase.TRANSPORT_NOT_SET
                && validTransportCardinality(input)
                && input.getReadModeValue() != 0;
    }

    private static boolean validTransportCardinality(
            org.pangrova.protocol.v2.ExchangeInputBinding input) {
        return input.hasDirect()
                ? input.getDirect().getSourcesCount()
                        == input.getExpectedSourcePartitions()
                : input.getExpectedSourcePartitions() == 1;
    }

    private static boolean validExternalInput(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        return validExternalIdentity(external)
                && validExternalReadCapability(external)
                && validExternalAddress(external)
                && exactPartitionCoverage(
                        external.getEndpointsList(), external.getPartitionCount());
    }

    private static boolean validExternalIdentity(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        return !external.getShuffleId().isBlank()
                && external.getOutputPartitionId() >= 0
                && external.getOutputPartitionId() < external.getPartitionCount()
                && external.getPlanEpoch() != 0
                && external.hasExecutionOwner()
                && external.getDeadlineEpochMs() != 0;
    }

    private static boolean validExternalReadCapability(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        return external.hasReadCapability()
                && !external.getReadCapability().getToken().isEmpty();
    }

    private static boolean validExternalAddress(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        return external.hasCommitAddress()
                && !external.getCommitAddress().getHost().isBlank()
                && external.getCommitAddress().getPort() > 0;
    }

    private static boolean exactPartitionCoverage(
            java.util.List<org.pangrova.protocol.v2.ShuffleServiceEndpoint> endpoints,
            int partitionCount) {
        int cursor = 0;
        var ordered = endpoints.stream()
                .sorted(java.util.Comparator.comparingInt(
                        org.pangrova.protocol.v2.ShuffleServiceEndpoint::getPartitionStart))
                .toList();
        for (var endpoint : ordered) {
            if (endpoint.getHost().isBlank() || endpoint.getPort() <= 0
                    || endpoint.getPartitionStart() != cursor
                    || endpoint.getPartitionEnd() <= cursor
                    || endpoint.getPartitionEnd() > partitionCount) {
                return false;
            }
            cursor = endpoint.getPartitionEnd();
        }
        return cursor == partitionCount;
    }

    private static boolean isValidExchangeSource(
            org.pangrova.protocol.v2.ExchangePartitionSource source,
            String producer,
            HashSet<String> producers) {
        return !source.getSourceTaskId().isBlank()
                && producers.add(producer)
                && source.getPlanEpoch() != 0
                && source.getFencingToken() != 0
                && !source.getHost().isBlank()
                && source.getPort() != 0
                && !source.getSignedTicket().isEmpty()
                && source.getTicketExpiresAtEpochMs() != 0;
    }

    private static void validateRuntimeFilters(TaskRequest request) {
        HashSet<String> planNodeIds = new HashSet<>();
        for (var binding : request.getInitialRuntimeFiltersList()) {
            if (binding.getPlanNodeId().isBlank()
                    || !planNodeIds.add(binding.getPlanNodeId())
                    || binding.getFiltersCount() == 0
                    || binding.getFiltersList().stream().anyMatch(filter ->
                            filter.getColumnName().isBlank()
                                    || filter.getScopeValue() == 0
                                    || filter.getPredicateCase()
                                            == org.pangrova.protocol.v2.RuntimeFilter
                                                    .PredicateCase.PREDICATE_NOT_SET)) {
                throw new IllegalArgumentException(
                        "TaskRequest initial runtime-filter binding is invalid");
            }
        }
    }
}
