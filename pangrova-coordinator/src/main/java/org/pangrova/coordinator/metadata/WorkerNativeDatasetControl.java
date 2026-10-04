package org.pangrova.coordinator.metadata;

import org.pangrova.coordinator.metadata.nativecontrol.NativeDatasetControlRpcException;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.DatasetScanPartitionsRequest;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintResult;
import org.pangrova.protocol.v2.DistributedWriteFragment;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.FinalizeDistributedWriteRequest;
import org.pangrova.protocol.v2.FinalizeDistributedWriteResult;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteFragment;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;

import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Catalog-owner metadata control implemented by authenticated Native Worker RPC. */
public final class WorkerNativeDatasetControl implements NativeDatasetControlPlane {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNativeDatasetControl.class);

    /** Late-bound process services, unavailable until query ownership and discovery have started. */
    public record Dependencies(Supplier<List<WorkerInfo>> workers, Supplier<WriteCommitJournal> journal,
                               Supplier<WriteCommitFencer> fencer, LongSupplier controlEpoch) {
        public Dependencies {
            Objects.requireNonNull(workers);
            Objects.requireNonNull(journal);
            Objects.requireNonNull(fencer);
            Objects.requireNonNull(controlEpoch);
        }
    }

    private static final long REQUEST_TIMEOUT_MS = 60_000;
    private final CatalogDescriptor catalog;
    private final Dependencies dependencies;
    private final NativeDatasetControlClient client;

    /** Pins the unresolved catalog deployment; the Worker resolves credentials locally. */
    public WorkerNativeDatasetControl(CatalogDescriptor catalog, Dependencies dependencies,
                                     NativeDatasetControlClient client) {
        this.catalog = Objects.requireNonNull(catalog);
        this.dependencies = Objects.requireNonNull(dependencies);
        this.client = Objects.requireNonNull(client);
    }

    @Override
    public byte[] schemaFingerprint(ConnectorSession session, List<ConnectorTableMetadata.Column> columns) {
        var schema = NativeDatasetSchemaCompiler.compile(columns);
        long deadline = deadline();
        var request = DatasetSchemaFingerprintRequest.newBuilder().setCatalog(catalogBinding())
                .setSecurity(security(session)).setSchema(schema.schema()).addAllColumnTypes(schema.columnTypes())
                .setDeadlineEpochMs(deadline).build();
        var response = client.call(endpoint(),
                command(RuntimeCommandType.RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT, request), deadline);
        try {
            var digest = response.getPayload().unpack(DatasetSchemaFingerprintResult.class).getPhysicalSchemaSha256();
            requireSha256(digest);
            return digest.getDigest().toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Native storage schema response is malformed", failure);
        }
    }

    @Override
    public org.pangrova.spi.connector.metadata.NativeDatasetScanPage scanPartitions(
            ConnectorSession session, String datasetUri, long version, byte[] schemaFingerprint,
            java.util.OptionalLong afterPartitionId, int pageSize) {
        if (version <= 0 || schemaFingerprint.length != 32 || pageSize < 1 || pageSize > 256
                || (afterPartitionId.isPresent() && afterPartitionId.getAsLong() < 0)) {
            throw new IllegalArgumentException("Invalid dataset scan pagination");
        }
        long deadline = deadline();
        var binding = catalogBinding();
        var request = DatasetScanPartitionsRequest.newBuilder().setCatalog(binding).setSecurity(security(session))
                .setDatasetUri(datasetUri).setVersion(version).setPhysicalSchemaSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(schemaFingerprint)))
                .setDeadlineEpochMs(deadline).setPageSize(pageSize).setHasCursor(afterPartitionId.isPresent());
        afterPartitionId.ifPresent(request::setAfterPartitionId);
        var response = client.call(endpoint(), command(RuntimeCommandType.RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS,
                request.build()), deadline);
        return NativeDatasetScanPages.decode(response, binding, version, schemaFingerprint, afterPartitionId, pageSize);
    }

    @Override
    public NativeWriteFinalization finalizeWrite(ConnectorSession session, ConnectorOutputTableHandle output,
            List<byte[]> fragments, DistributedWriteCommitContext context) {
        WriteCommitJournal journal = dependencies.journal().get();
        WriteCommitEntry intent = journal.getEntry(context.executionId()).orElseThrow(
                () -> new IllegalStateException("Native publication requires a durable write intent"));
        validateIntent(intent, fragments, context);
        validateOutput(intent, output, catalog.name());
        WriteCommitFencer fencer = Objects.requireNonNull(dependencies.fencer().get(),
                "Native publication requires durable ownership");
        ExecutionAnchor anchor = fencer.currentAnchor(context.executionId());
        requireOwner(fencer, anchor, context.executionId());
        long deadline = Math.min(deadline(), anchor.expiresAt().toEpochMilli());
        var request = finalizeRequest(security(session), intent, anchor, deadline,
                dependencies.controlEpoch().getAsLong());
        if (!request.getSecurity().getTenantId().equals(intent.tenantId())
                || !request.getOutputBinding().hasLance()
                || !catalogBinding().equals(request.getOutputBinding().getLance().getCatalog())) {
            throw new SecurityException("Publication catalog deployment or tenant differs from durable intent");
        }
        NativeWriteFinalization publication;
        try {
            publication = publish(request, deadline, context.commitFingerprint());
        } catch (NativeDatasetControlRpcException uncertain) {
            LOG.warn("event=native_publication_unknown queryId={} fingerprint={}",
                    context.executionId(), context.commitFingerprint(), uncertain);
            return new NativeWriteFinalization(NativeWriteFinalization.Outcome.UNKNOWN, 0, new byte[0]);
        }
        requireOwner(fencer, anchor, context.executionId());
        if (publication.outcome() == NativeWriteFinalization.Outcome.COMMITTED) {
            journal.recordPublication(context.executionId(), context.commitFingerprint(), publication);
        }
        return publication;
    }

    private NativeWriteFinalization publish(
            FinalizeDistributedWriteRequest request, long deadline, String fingerprint) {
        var response = client.call(endpoint(),
                command(RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE, request), deadline);
        try {
            return decodePublication(response, fingerprint);
        } catch (IllegalArgumentException invalidEvidence) {
            throw new NativeDatasetControlRpcException("Native publication response cannot establish outcome",
                    invalidEvidence);
        }
    }

    static FinalizeDistributedWriteRequest finalizeRequest(SecurityContext security, WriteCommitEntry intent,
            ExecutionAnchor anchor, long deadline, long controlEpoch) {
        if (controlEpoch <= 0 || deadline <= System.currentTimeMillis()) {
            throw new IllegalArgumentException("Publication control term/deadline is invalid");
        }
        if (intent.ownerId() == null || intent.executionFencingToken() <= 0) {
            throw new IllegalArgumentException("Native publication requires durably owned winner provenance");
        }
        validateIntentOwner(intent, anchor, deadline);
        try {
            var binding = WriteOutputBinding.parseFrom(intent.writeOutputBinding());
            var request = FinalizeDistributedWriteRequest.newBuilder().setSecurity(security).setOutputBinding(binding)
                    .setExecutionId(intent.queryId()).setCommitFingerprint(intent.commitFingerprint())
                    .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId())
                            .setOwnerIncarnation(anchor.ownerIncarnation()).setFencingToken(anchor.fencingToken()))
                    .setIntentExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId(intent.ownerId())
                            .setOwnerIncarnation(intent.ownerIncarnation())
                            .setFencingToken(intent.executionFencingToken()))
                    .setControlEpoch(controlEpoch).setDeadlineEpochMs(deadline).setRecovering(true);
            for (byte[] bytes : intent.fragments()) {
                var fragment = LanceWriteFragment.parseFrom(bytes);
                request.addFragments(DistributedWriteFragment.newBuilder().setTaskAttempt(fragment.getTaskAttempt())
                        .setCommitMetadata(ByteString.copyFrom(bytes)).setSha256(CanonicalTaskRequests.sha256(bytes)));
            }
            if (intent.publication() != null) {
                request.setKnownPublishedVersion(intent.publication().publishedVersion())
                        .setKnownManifestSha256(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                                .setDigest(ByteString.copyFrom(intent.publication().manifestSha256())));
            }
            return request.build();
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Durable Native write metadata is malformed", malformed);
        }
    }

    private static void validateIntentOwner(WriteCommitEntry intent, ExecutionAnchor anchor, long deadline) {
        if (!anchor.executionId().equals(intent.queryId())
                || anchor.fencingToken() < intent.executionFencingToken()
                || deadline > anchor.expiresAt().toEpochMilli()) {
            throw new SecurityException("Publication authority differs from durable execution intent");
        }
        if (anchor.fencingToken() == intent.executionFencingToken()
                && (!anchor.ownerId().equals(intent.ownerId())
                    || !anchor.ownerIncarnation().equals(intent.ownerIncarnation()))) {
            throw new SecurityException("Publication owner differs at the same fencing token");
        }
    }

    static void validateIntent(WriteCommitEntry intent, List<byte[]> fragments, DistributedWriteCommitContext context) {
        if (!intent.queryId().equals(context.executionId())
                || !intent.commitFingerprint().equals(context.commitFingerprint())
                || !intent.tenantId().equals(context.tenantId())
                || intent.baseSnapshotId() != context.baseSnapshotId()
                || !Arrays.equals(intent.writeOutputBinding(), context.writeOutputBinding())
                || !sameFragments(intent.fragments(), fragments)) {
            throw new IllegalArgumentException("Publication differs from durable intent");
        }
    }

    static void validateOutput(WriteCommitEntry intent, ConnectorOutputTableHandle output, String catalogName) {
        String target = output.targetTable().orElseThrow(
                () -> new IllegalArgumentException("Native output requires a target table")).toString();
        long base = output.writeBaseSnapshotId().orElseThrow(
                () -> new IllegalArgumentException("Native output requires its original base snapshot"));
        if (!intent.catalogName().equals(catalogName) || !intent.tableName().equals(target)
                || !intent.writeMode().equals(output.writeMode()) || intent.baseSnapshotId() != base) {
            throw new IllegalArgumentException("Native output identity differs from durable intent");
        }
        NativeWriteAllocation allocation = output.nativeWriteAllocation().orElseThrow(
                () -> new IllegalArgumentException("Native output requires its original allocation"));
        try {
            WriteOutputBinding binding = WriteOutputBinding.parseFrom(intent.writeOutputBinding());
            if (!binding.hasLance()) {
                throw new IllegalArgumentException("Durable Native output binding is missing");
            }
            validateAllocation(allocation, binding.getLance());
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Durable Native output binding is malformed", malformed);
        }
    }

    private static void validateAllocation(NativeWriteAllocation allocation, LanceWriteBinding binding) {
        validateTransaction(allocation, binding);
        requireSha256(binding.getSchemaFenceSha256());
        if (!Arrays.equals(allocation.schemaFenceSha256(), binding.getSchemaFenceSha256().getDigest().toByteArray())
                || !allocation.stagingPrefix().equals(binding.getAllocationStagingPrefix())
                || !allocation.generation().equals(binding.getGeneration())
                || !allocation.writeColumnNames().equals(binding.getWriteColumnNamesList())) {
            throw new IllegalArgumentException("Native output allocation differs from durable intent");
        }
    }

    private static void validateTransaction(NativeWriteAllocation allocation, LanceWriteBinding binding) {
        if (!allocation.datasetUri().equals(binding.getDatasetUri())
                || operation(allocation.operation()) != binding.getOperation()
                || allocation.readVersion() != binding.getExpectedReadVersion()
                || !allocation.writeId().equals(binding.getWriteId())) {
            throw new IllegalArgumentException("Native output transaction differs from durable intent");
        }
    }

    private static LanceWriteOperation operation(NativeWriteAllocation.Operation operation) {
        return switch (operation) {
            case CREATE -> LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE;
            case APPEND -> LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND;
            case OVERWRITE -> LanceWriteOperation.LANCE_WRITE_OPERATION_OVERWRITE;
        };
    }

    private static boolean sameFragments(List<byte[]> left, List<byte[]> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int index = 0; index < left.size(); index++) {
            if (!Arrays.equals(left.get(index), right.get(index))) {
                return false;
            }
        }
        return true;
    }

    private static void requireOwner(WriteCommitFencer fencer, ExecutionAnchor anchor, String executionId) {
        if (!anchor.executionId().equals(executionId) || anchor.expiresAt().toEpochMilli() <= System.currentTimeMillis()
                || !fencer.isCurrent(executionId, anchor.fencingToken())) {
            throw new SecurityException("Native publication owner is stale");
        }
    }

    static NativeWriteFinalization decodePublication(RuntimeCommandResponse response, String fingerprint) {
        try {
            var result = response.getPayload().unpack(FinalizeDistributedWriteResult.class);
            if (!fingerprint.equals(result.getCommitFingerprint())) {
                throw new IllegalArgumentException("Publication fingerprint differs");
            }
            var outcome = switch (result.getOutcome()) {
                case WRITE_PUBLICATION_OUTCOME_COMMITTED -> NativeWriteFinalization.Outcome.COMMITTED;
                case WRITE_PUBLICATION_OUTCOME_NOT_COMMITTED -> NativeWriteFinalization.Outcome.NOT_COMMITTED;
                case WRITE_PUBLICATION_OUTCOME_UNKNOWN -> NativeWriteFinalization.Outcome.UNKNOWN;
                default -> throw new IllegalArgumentException("Native publication outcome is missing");
            };
            if (outcome == NativeWriteFinalization.Outcome.COMMITTED) {
                requireSha256(result.getManifestSha256());
            }
            return new NativeWriteFinalization(outcome, result.getPublishedVersion(),
                    result.getManifestSha256().getDigest().toByteArray());
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Native publication response is malformed", malformed);
        }
    }

    private SecurityContext security(ConnectorSession session) {
        String tenant = session.getProperties().get("tenant_id");
        if (tenant == null || tenant.isBlank() || session.getUser() == null || session.getUser().isBlank()
                || !session.getCatalog().equals(java.util.Optional.of(catalog.name()))
                || (!catalog.allowedTenants().isEmpty() && !catalog.allowedTenants().contains(tenant))) {
            throw new SecurityException("Native metadata catalog tenant is unauthorized");
        }
        return SecurityContext.newBuilder().setTenantId(tenant).setPrincipal(session.getUser())
                .setPolicyContextHash(session.getProperties().getOrDefault("pangrova.policy_context_hash", "")).build();
    }

    private CatalogExecutionBinding catalogBinding() {
        return CatalogExecutionBindings.from(catalog);
    }

    private URI endpoint() {
        WorkerInfo worker = dependencies.workers().get().stream()
                .min(Comparator.comparingInt(WorkerInfo::runningTaskCount)
                .thenComparing(WorkerInfo::workerId)).orElseThrow(
                        () -> new IllegalStateException("No active Native Worker for metadata control"));
        try {
            return new URI("http", null, worker.host(), worker.httpPort(), "/v1/native-dataset/control", null, null);
        } catch (java.net.URISyntaxException invalid) {
            throw new IllegalArgumentException("Invalid registered Worker endpoint", invalid);
        }
    }

    private static RuntimeCommand command(RuntimeCommandType type, com.google.protobuf.Message payload) {
        return RuntimeCommand.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion()).setCommandType(type)
                .setPayload(Any.pack(payload)).build();
    }
    private static long deadline() {
        return Math.addExact(System.currentTimeMillis(), REQUEST_TIMEOUT_MS);
    }

    private static void requireSha256(Checksum digest) {
        if (digest.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256 || digest.getDigest().size() != 32) {
            throw new IllegalArgumentException("Native metadata requires exact SHA-256 evidence");
        }
    }
}
