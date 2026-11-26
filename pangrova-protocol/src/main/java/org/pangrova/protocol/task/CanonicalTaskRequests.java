package org.pangrova.protocol.task;

import org.pangrova.protocol.resource.StageResourceGrants;
import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;
import org.pangrova.protocol.checksum.ProtocolChecksums;

import java.util.HashSet;
import java.util.List;

import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.CdcWriteOperation;
import org.pangrova.protocol.v2.ExecutionCallbackContext;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.ShufflePartitionPublication;
import org.pangrova.protocol.v2.TaskAttemptId;

/** Canonical v2 task construction and Worker-admission validation. */
public final class CanonicalTaskRequests {
    public static final int MAX_BUILD_KEY_COLUMNS = 4;
    public static final int MAX_EXTERNAL_PARTITIONS_PER_INPUT = 32;

    public static final int MAX_COVERAGE_IDENTITIES = 4_096;
    private static final int SHA256_BYTES = 32;
    private static final int MAX_PORT = 65_535;

    private CanonicalTaskRequests() {}

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
        var callback = projectCallbackContext(request);
        validateCallback(callback);
        return callback;
    }

    static ExecutionCallbackContext projectCallbackContext(TaskRequest request) {
        var callback = ExecutionCallbackContext.newBuilder()
                .setProtocol(request.getProtocol()).setSecurity(request.getSecurity())
                .setAttempt(request.getAttempt()).setResourceGrant(request.getResourceGrant())
                .setDeadlineEpochMs(request.getDeadlineEpochMs());
        if (request.hasTraceContext()) {
            callback.setTraceContext(request.getTraceContext());
        }
        return callback.build();
    }

    /** Validates the same admitted task authority used by every Native callback. */
    public static void validateCallback(ExecutionCallbackContext context) {
        if (context == null || !context.getUnknownFields().asMap().isEmpty()
                || !context.hasProtocol() || !context.hasAttempt() || !context.hasSecurity()
                || !context.hasResourceGrant()) {
            throw new IllegalArgumentException("Callback task authority is absent or contains unknown fields");
        }
        CanonicalProtocolIdentity.validate(context.getProtocol());
        validateAttempt(context.getAttempt());
        requireSecurityContext(context.getSecurity());
        requireDeadline(context.getDeadlineEpochMs());
        StageResourceGrants.validate(context.getResourceGrant());
        if (!context.getTraceContext().getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Callback trace context contains unknown fields");
        }
    }

    /** Validates preparation authority independently of relational task execution. */
    public static void validateSourcePreparation(org.pangrova.protocol.v2.SourcePreparationRequest request) {
        if (request == null || !request.getUnknownFields().asMap().isEmpty()
                || !request.hasProtocol() || !request.hasAttempt() || !request.hasSecurity()
                || !request.hasResourceGrant() || !request.hasSourceCommandSha256()) {
            throw new IllegalArgumentException("Source preparation authority is incomplete");
        }
        CanonicalProtocolIdentity.validate(request.getProtocol());
        validateAttempt(request.getAttempt());
        requireSecurityContext(request.getSecurity());
        StageResourceGrants.validateSourcePreparation(request.getResourceGrant());
        requireDeadline(request.getDeadlineEpochMs());
        if (!request.getTraceContext().getUnknownFields().asMap().isEmpty()
                || !request.getSourceCommandSha256().getUnknownFields().asMap().isEmpty()
                || request.getSourceCommandSha256().getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || request.getSourceCommandSha256().getDigest().size() != SHA256_BYTES) {
            throw new IllegalArgumentException("Source preparation trace or command digest is invalid");
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
        CanonicalProtocolIdentity.validate(request.getProtocol());
        validateAttempt(request.getAttempt());
        requireTaskKind(request);
        validateCopySourceStagingCommand(request);
        requireSecurityContext(request.getSecurity());
        if (request.getTaskKind() == org.pangrova.protocol.v2.TaskKind.TASK_KIND_CDC_INGEST) {
            validateCdcIngest(request);
        } else {
            requireValidPlanChecksum(request);
        }
        requireDeadline(request.getDeadlineEpochMs());
        StageResourceGrants.validate(request.getResourceGrant());
        validateSerializedOutputGrant(request);
        validateShuffleExecution(request);
        validateUniqueBindings(request);
        validateCoverageIdentityBounds(request);
        validateExchangeInputs(request);
        validateRuntimeFilters(request);
        validateBuildKeyOrdinals(request);
    }

    private static void validateBuildKeyOrdinals(TaskRequest request) {
        var ordinals = request.getBuildKeyOrdinalsList();
        if (ordinals.isEmpty()) return;
        if (ordinals.size() > MAX_BUILD_KEY_COLUMNS
                || request.getTaskKind() != org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ
                || !request.getExecutionOptions().hasShuffle()
                || request.getExecutionOptions().getDirectResultOutput()
                || request.getExchangeInputsCount() != 0) {
            throw new IllegalArgumentException("Runtime-filter build keys require a bounded leaf shuffle producer");
        }
        int previous = -1;
        for (int ordinal : ordinals) {
            if (ordinal <= previous) {
                throw new IllegalArgumentException("Runtime-filter build key ordinals must be sorted and distinct");
            }
            previous = ordinal;
        }
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
        SourceKey previous = null;
        for (var split : request.getSplitsList()) {
            String source = split.getSourceId();
            String snapshot = split.getSourceSnapshot();
            if (previous != null && previous.id().equals(source) && previous.snapshot().equals(snapshot)) {
                continue;
            }
            previous = new SourceKey(source, snapshot);
            sources.add(previous);
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
        boolean copySplit = request.getSplitsList().stream()
                .anyMatch(split -> "copy".equals(split.getConnectorId()));
        if (!request.hasCopySourceStagingCommandSha256()) {
            if (copySplit) throw new IllegalArgumentException("COPY source split requires its private staging command");
            return;
        }
        // The staging RPC has no splits; executing inputs are one exact COPY object.
        if (request.getSplitsCount() != 0 && (request.getSplitsCount() != 1 || !copySplit)) {
            throw new IllegalArgumentException("COPY source command requires one COPY connector split");
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

    private static void validateCdcIngest(TaskRequest request) {
        if (!request.hasCdcIngest()) {
            throw new IllegalArgumentException("CDC ingest task requires its exact bounded source binding");
        }
        if (!request.getSubstraitPlan().isEmpty() || request.getSplitsCount() != 0
                || request.getOperatorBindingsCount() != 0 || request.getExchangeInputsCount() != 0
                || request.getInitialRuntimeFiltersCount() != 0) {
            throw new IllegalArgumentException(
                    "CDC ingest task cannot carry a plan, splits, operators, exchanges or runtime filters");
        }
        var cdc = request.getCdcIngest();
        if (cdc.getStreamId().isBlank() || cdc.getTenantId().isBlank()
                || cdc.getSourceOwnershipEpoch().isBlank() || cdc.getSourceProvider().isBlank()
                || cdc.getFormatId().isBlank() || cdc.getTargetTableUuid().isBlank()
                || cdc.getCommitIntentId().isBlank() || cdc.getBaseSnapshotId() < -1) {
            throw new IllegalArgumentException("CDC ingest task is missing its durable source/write identity");
        }
        if (!cdc.getTenantId().equals(request.getSecurity().getTenantId())) {
            throw new IllegalArgumentException("CDC ingest tenant differs from the task security tenant");
        }
        var operations = cdc.getAllowedOperationsList();
        if (operations.isEmpty() || operations.stream().distinct().count() != operations.size()
                || operations.contains(CdcWriteOperation.CDC_WRITE_OPERATION_UNSPECIFIED)
                || operations.contains(CdcWriteOperation.UNRECOGNIZED)) {
            throw new IllegalArgumentException("CDC ingest task requires explicit, unique authorized operations");
        }
        if (cdc.getRangesCount() == 0) {
            throw new IllegalArgumentException("CDC ingest task requires at least one bounded source range");
        }
        for (var range : cdc.getRangesList()) {
            if (range.getTopic().isBlank() || range.getPartition() < 0
                    || range.getStartOffset() < 0 || range.getEndOffset() <= range.getStartOffset()) {
                throw new IllegalArgumentException("CDC ingest range is not a bounded topic partition interval");
            }
        }
        if (!cdc.hasWrite()) {
            throw new IllegalArgumentException("CDC ingest task requires its exact connector write binding");
        }
        var checksum = cdc.getCommitIdentitySha256();
        if (cdc.getCommitIdentitySha256().getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != SHA256_BYTES
                || !checksum.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("CDC ingest commit identity must be canonical SHA-256");
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
        return ProtocolChecksums.sha256(request.getSubstraitPlan()).getDigest()
                .equals(request.getSubstraitPlanChecksum().getDigest());
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
        if (attempt.hasWorkerExecutionIdentity()) {
            org.pangrova.protocol.execution.identity.WorkerExecutionIdentity.from(attempt.getWorkerExecutionIdentity());
        }
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
            } else if (!validExternalInput(input.getExternal())
                    || !input.getExternal().getExecutionOwner().equals(request.getAttempt().getExecutionOwner())
                    || input.getExternal().getDeadlineEpochMs() != request.getDeadlineEpochMs()) {
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
                && input.getNoMoreSources()
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

    /** Checks the complete external input wire envelope including its exact endpoint coverage and publication. */
    public static boolean validExternalInput(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        return validExternalIdentity(external)
                && validPublications(external.getPublicationsList())
                && external.getExpectedProducerPartitions() > 0
                && external.getExpectedProducerSetChecksum().getAlgorithm() == Checksum.Algorithm.ALGORITHM_SHA256
                && external.getExpectedProducerSetChecksum().getDigest().size() == SHA256_BYTES
                && validExternalReadCapability(external)
                && validExternalAddress(external)
                && exactPartitionCoverage(
                        external.getEndpointsList(), external.getPartitionCount());
    }

    private static boolean validPublications(java.util.List<ShufflePartitionPublication> publications) {
        long batches = 0;
        for (var publication : publications) {
            if (!validPartitionPublication(publication)
                    || publication.getLastSequence() + 1 > Long.MAX_VALUE - batches) {
                return false;
            }
            batches += publication.getLastSequence() + 1;
        }
        return true;
    }

    /** Checks coordinator-attested partition bounds before a Worker opens any external read. */
    public static boolean validPartitionPublication(ShufflePartitionPublication evidence) {
        return evidence.hasLastSequence() && evidence.getLastSequence() >= -1
                && evidence.hasTotalBytes() && evidence.getTotalBytes() >= 0
                && evidence.hasTotalRows() && evidence.getTotalRows() >= 0
                && evidence.getManifestSha256().size() == SHA256_BYTES
                && (evidence.getLastSequence() == -1
                        ? evidence.getTotalBytes() == 0 && evidence.getTotalRows() == 0
                        : evidence.getLastSequence() < Math.min(evidence.getTotalBytes(), evidence.getTotalRows()));
    }

    private static boolean validExternalIdentity(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        var owner = external.getExecutionOwner();
        return !external.getShuffleId().isBlank()
                && external.hasSourcePartitionStart()
                && external.getSourcePartitionStart() >= 0
                && external.getPartitionCount() > 0
                && external.getPublicationsCount() >= 1
                && external.getPublicationsCount() <= MAX_EXTERNAL_PARTITIONS_PER_INPUT
                && external.getPublicationsCount() <= external.getPartitionCount() - external.getSourcePartitionStart()
                && external.getPlanEpoch() > 0
                && external.hasExecutionOwner()
                && !owner.getOwnerId().isBlank()
                && !owner.getOwnerIncarnation().isBlank()
                && owner.getFencingToken() > 0
                && external.getDeadlineEpochMs() > 0;
    }

    private static boolean validExternalReadCapability(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        return external.hasReadCapability()
                && !external.getReadCapability().getToken().isEmpty()
                && external.getReadCapability().getExpiresAtEpochMs() >= external.getDeadlineEpochMs();
    }

    private static boolean validExternalAddress(
            org.pangrova.protocol.v2.ExternalShuffleInput external) {
        return external.hasCommitAddress()
                && !external.getCommitAddress().getHost().isBlank()
                && validPort(external.getCommitAddress().getPort());
    }

    private static boolean exactPartitionCoverage(
            java.util.List<org.pangrova.protocol.v2.ShuffleServiceEndpoint> endpoints,
            int partitionCount) {
        int cursor = 0;
        for (var endpoint : endpoints) {
            if (endpoint.getHost().isBlank() || !validPort(endpoint.getPort())
                    || endpoint.getPartitionStart() != cursor
                    || endpoint.getPartitionEnd() <= cursor
                    || endpoint.getPartitionEnd() > partitionCount) {
                return false;
            }
            cursor = endpoint.getPartitionEnd();
        }
        return cursor == partitionCount;
    }

    private static boolean validPort(int port) {
        return port > 0 && port <= MAX_PORT;
    }

    private static boolean isValidExchangeSource(
            org.pangrova.protocol.v2.ExchangePartitionSource source,
            String producer,
            HashSet<String> producers) {
        return !source.getSourceTaskId().isBlank()
                && producers.add(producer)
                && source.getSourceStageId() >= 0
                && source.getSourcePartitionId() >= 0
                && source.getOutputPartitionId() >= 0
                && source.getAttemptId() >= 0
                && source.getPlanEpoch() > 0
                && source.getFencingToken() > 0
                && !source.getHost().isBlank()
                && validPort(source.getPort())
                && !source.getSignedTicket().isEmpty()
                && source.getTicketExpiresAtEpochMs() > 0
                && source.getExpectedRows() >= 0
                && source.getExpectedBytes() >= 0;
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
