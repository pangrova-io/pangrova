package org.pangrova.coordinator.metadata;

import org.pangrova.connector.api.enums.SourceObjectPurpose;
import org.pangrova.connector.api.enums.NativeWriteOperation;
import org.pangrova.connector.api.enums.NativeWritePreparationOutcome;
import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.pangrova.coordinator.exception.NativeDatasetControlRpcException;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.DatasetScanPartitionsRequest;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintResult;
import org.pangrova.protocol.v2.DistributedWriteFragment;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.PrepareDistributedWriteResult;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteFragment;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.NativeDatasetControlPlane;
import org.pangrova.connector.api.metadata.NativeWritePreparation;
import org.pangrova.connector.api.metadata.PreparedSourcePublication;
import org.pangrova.connector.api.metadata.SourceObjectPage;
import org.pangrova.connector.api.metadata.SourcePublicationReservation;
import org.pangrova.connector.api.metadata.ManagedSourceCatalog;
import org.pangrova.connector.api.metadata.AcceptedSourceSnapshot;
import org.pangrova.connector.api.metadata.NativeWriteAllocation;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;

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
    private final ManagedSourceCatalog sourceCatalog;

    /** Pins the unresolved catalog deployment; the Worker resolves credentials locally. */
    public WorkerNativeDatasetControl(CatalogDescriptor catalog, Dependencies dependencies,
                                     NativeDatasetControlClient client, ManagedSourceCatalog sourceCatalog) {
        this.catalog = Objects.requireNonNull(catalog);
        this.dependencies = Objects.requireNonNull(dependencies);
        this.sourceCatalog = Objects.requireNonNull(sourceCatalog);
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
    public org.pangrova.connector.api.metadata.NativeDatasetScanPage scanPartitions(
            ConnectorSession session, AcceptedSourceSnapshot snapshot,
            java.util.OptionalLong afterPartitionId, int pageSize) {
        long version = snapshot.version();
        byte[] schemaFingerprint = snapshot.schemaSha256();
        if (!security(session).getTenantId().equals(snapshot.tenant())) {
            throw new SecurityException("Scan membership tenant differs");
        }
        if (version <= 0 || schemaFingerprint.length != 32 || pageSize < 1 || pageSize > 256
                || (afterPartitionId.isPresent() && afterPartitionId.getAsLong() < 0)) {
            throw new IllegalArgumentException("Invalid dataset scan pagination");
        }
        long deadline = deadline();
        var binding = catalogBinding();
        var request = DatasetScanPartitionsRequest.newBuilder().setCatalog(binding).setSecurity(security(session))
                .setDatasetUri(snapshot.datasetUri()).setVersion(version).setPhysicalSchemaSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(schemaFingerprint)))
                .setAcceptedSnapshot(org.pangrova.protocol.source.SourceSnapshotBindings.encode(snapshot, binding))
                .setDeadlineEpochMs(deadline).setPageSize(pageSize).setHasCursor(afterPartitionId.isPresent());
        afterPartitionId.ifPresent(request::setAfterPartitionId);
        var response = client.call(endpoint(), command(RuntimeCommandType.RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS,
                request.build()), deadline);
        return NativeDatasetScanPages.decode(response, binding, snapshot, afterPartitionId, pageSize);
    }

    @Override public void cleanupSourceObjects(ConnectorSession session, SourcePublicationReservation reservation) {
        if (!security(session).getTenantId().equals(reservation.base().key().tenant())) {
            throw new SecurityException("Source cleanup tenant differs from its authority");
        }
        for (int page = 0; page < SourceObjectPage.MAX_PAGES; page++) {
            var selected = sourceCatalog.reserveCleanup(reservation);
            if (selected.isEmpty()) return;
            var batch = selected.orElseThrow();
            var anchor = dependencies.fencer().get().currentAnchor(reservation.executionId());
            requireOwner(dependencies.fencer().get(), anchor, reservation.executionId());
            long deadline = Math.min(deadline(), anchor.expiresAt().toEpochMilli());
            var request = org.pangrova.protocol.v2.CleanupSourceObjectsRequest.newBuilder()
                    .setCatalog(catalogBinding()).setSecurity(security(session)).setExecutionId(batch.executionId())
                    .setCommitFingerprint(batch.fingerprint()).setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                            .setOwnerId(anchor.ownerId()).setOwnerIncarnation(anchor.ownerIncarnation())
                            .setFencingToken(anchor.fencingToken()))
                    .setControlEpoch(dependencies.controlEpoch().getAsLong()).setDeadlineEpochMs(deadline)
                    .setCleanupId(batch.cleanupId()).setDatasetUri(batch.datasetUri())
                    .setObjectsSha256(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                            .setDigest(ByteString.copyFrom(batch.sha256())));
            for (var object : batch.objects()) {
                request.addObjects(org.pangrova.protocol.v2.PreparedSourceObject.newBuilder()
                        .setRelativePath(object.relativePath()).setSizeBytes(object.sizeBytes())
                        .setPurpose(object.purpose() == SourceObjectPurpose.SOURCE
                                ? org.pangrova.protocol.v2.SourceObjectPurpose.SOURCE_OBJECT_PURPOSE_SOURCE
                                : org.pangrova.protocol.v2.SourceObjectPurpose.SOURCE_OBJECT_PURPOSE_WINNER_SIDECAR));
            }
            try {
                var response = client.call(endpoint(),
                        command(RuntimeCommandType.RUNTIME_COMMAND_TYPE_CLEANUP_SOURCE_OBJECTS, request.build()),
                        deadline);
                var result = response.getPayload().unpack(org.pangrova.protocol.v2.CleanupSourceObjectsResult.class);
                if (!result.getCleanupId().equals(batch.cleanupId()) || !result.getSdkJoined()
                        || result.getDeletedCount() != batch.objects().size()
                        || !result.getObjectsSha256().equals(request.getObjectsSha256())) {
                    throw new IllegalArgumentException(
                            "Source cleanup response differs from its authoritative deletion set");
                }
                sourceCatalog.completeCleanup(reservation, batch);
            } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
                throw new IllegalArgumentException("Source cleanup response is malformed", malformed);
            }
        }
        throw new IllegalStateException("Source cleanup exceeded its admitted page bound");
    }

    @Override
    public NativeWritePreparation prepareWrite(ConnectorSession session, ConnectorOutputTableHandle output,
            List<byte[]> fragments, DistributedWriteCommitContext context, SourcePublicationReservation reservation) {
        WriteCommitEntry intent = dependencies.journal().get().getEntry(context.executionId()).orElseThrow(
                () -> new IllegalStateException("Native preparation requires a durable write intent"));
        validateIntent(intent, fragments, context);
        validateOutput(intent, output, catalog.name());
        WriteCommitFencer fencer = Objects.requireNonNull(
                dependencies.fencer().get(), "Preparation requires durable ownership");
        ExecutionAnchor anchor = fencer.currentAnchor(context.executionId());
        requireOwner(fencer, anchor, context.executionId());
        long deadline = Math.min(deadline(), anchor.expiresAt().toEpochMilli());
        var request = prepareRequest(security(session), intent, anchor, deadline,
                dependencies.controlEpoch().getAsLong(), reservation);
        if (!request.getSecurity().getTenantId().equals(intent.tenantId())
                || !request.getOutputBinding().hasLance()
                || !catalogBinding().equals(request.getOutputBinding().getLance().getCatalog())) {
            throw new SecurityException("Preparation deployment or tenant differs from durable intent");
        }
        PreparedSourcePublication first = null;
        String after = "";
        for (int pageIndex = 0; pageIndex < SourceObjectPage.MAX_PAGES; pageIndex++) {
            var pageRequest = request.toBuilder().setObjectPageIndex(pageIndex).setAfterObjectPath(after)
                    .setObjectPageSize(SourceObjectPage.MAX_PAGE_OBJECTS).build();
            URI worker = endpoint();
            var invocation = sourceCatalog.issuePreparation(reservation, pageIndex, after, worker.toString(),
                    ProtocolChecksums.sha256(pageRequest.toByteArray()).getDigest().toByteArray());
            pageRequest = pageRequest.toBuilder().setInvocationId(invocation.invocationId()).build();
            PreparationPage page;
            try {
                var response = client.call(worker,
                        command(RuntimeCommandType.RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE, pageRequest),
                        deadline);
                page = decodePreparation(response, context.commitFingerprint(), catalogBinding());
                var evidence = response.getPayload().unpack(PrepareDistributedWriteResult.class);
                if (!evidence.getInvocationId().equals(invocation.invocationId())) {
                    throw new IllegalArgumentException("Preparation response differs from its durable SDK invocation");
                }
                if (!evidence.getSdkJoined()) {
                    if (page != null) {
                        throw new IllegalArgumentException("Prepared candidate has no actual SDK join proof");
                    }
                    return new NativeWritePreparation(
                            NativeWritePreparationOutcome.UNKNOWN, java.util.Optional.empty());
                }
                if (page != null) {
                    requireIssuedPage(page.objects(), pageIndex, after);
                }
                sourceCatalog.acknowledgePreparation(reservation, invocation,
                        ProtocolChecksums.sha256(evidence.toByteArray()).getDigest().toByteArray());
            } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
                throw new IllegalArgumentException("Preparation join response is malformed", malformed);
            } catch (NativeDatasetControlRpcException uncertain) {
                LOG.warn("event=native_preparation_unknown queryId={} fingerprint={} page={}",
                        context.executionId(), context.commitFingerprint(), pageIndex, uncertain);
                return new NativeWritePreparation(NativeWritePreparationOutcome.UNKNOWN, java.util.Optional.empty());
            }
            if (page == null) {
                return new NativeWritePreparation(
                        NativeWritePreparationOutcome.UNKNOWN, java.util.Optional.empty());
            }
            requireOwner(fencer, anchor, context.executionId());
            if (first == null) {
                first = page.publication();
            } else {
                requireSamePreparation(first, page.publication());
            }
            sourceCatalog.registerObjects(reservation, page.publication(), page.objects());
            if (page.objects().last()) {
                return new NativeWritePreparation(NativeWritePreparationOutcome.PREPARED,
                        java.util.Optional.of(page.publication()));
            }
            after = page.objects().lastPath();
        }
        throw new IllegalStateException("SDK object registration exceeds its admitted page bound");
    }

    static void requireIssuedPage(SourceObjectPage page, int pageIndex, String after) {
        if (page.index() != pageIndex || !page.afterPath().equals(after)) {
            throw new IllegalArgumentException("SDK object page sequence or cursor differs");
        }
    }

    static PrepareDistributedWriteRequest prepareRequest(SecurityContext security, WriteCommitEntry intent,
            ExecutionAnchor anchor, long deadline, long controlEpoch, SourcePublicationReservation reservation) {
        if (controlEpoch <= 0 || deadline <= System.currentTimeMillis()) {
            throw new IllegalArgumentException("Invalid preparation term/deadline");
        }
        if (intent.ownerId() == null || intent.executionFencingToken() <= 0) {
            throw new IllegalArgumentException("Winners lack durable provenance");
        }
        validateIntentOwner(intent, anchor, deadline);
        try {
            var binding = WriteOutputBinding.parseFrom(intent.writeOutputBinding());
            var allocation = org.pangrova.protocol.v2.SourcePublicationReservation.newBuilder()
                    .setTableId(reservation.base().tableId()).setTableGeneration(reservation.base().generation())
                    .setTenantId(reservation.base().key().tenant())
                    .setManifestRelativePath(reservation.manifestRelativePath());
            reservation.baseSnapshot().ifPresent(base -> allocation.setBaseSnapshot(
                    org.pangrova.protocol.source.SourceSnapshotBindings.encode(base, binding.getLance().getCatalog())));
            var request = PrepareDistributedWriteRequest.newBuilder().setSecurity(security).setOutputBinding(binding)
                    .setExecutionId(intent.queryId()).setCommitFingerprint(intent.commitFingerprint())
                    .setReservation(allocation)
                    .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId())
                            .setOwnerIncarnation(anchor.ownerIncarnation()).setFencingToken(anchor.fencingToken()))
                    .setIntentExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId(intent.ownerId())
                            .setOwnerIncarnation(intent.ownerIncarnation())
                            .setFencingToken(intent.executionFencingToken()))
                    .setControlEpoch(controlEpoch).setDeadlineEpochMs(deadline);
            for (byte[] bytes : intent.fragments()) {
                var fragment = LanceWriteFragment.parseFrom(bytes);
                request.addFragments(DistributedWriteFragment.newBuilder().setTaskAttempt(fragment.getTaskAttempt())
                        .setCommitMetadata(ByteString.copyFrom(bytes)).setSha256(ProtocolChecksums.sha256(bytes)));
            }
            return request.build();
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Durable Native preparation metadata is malformed", malformed);
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
                || !intent.principal().equals(context.principal())
                || !intent.authorizationReceipt().equals(context.authorizationReceipt())
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
            var acceptedBase = output.acceptedWriteBaseSnapshot();
            if ((base > 0) != acceptedBase.isPresent()
                    || binding.getLance().hasBaseSnapshot() != acceptedBase.isPresent()
                    || (acceptedBase.isPresent() && !org.pangrova.protocol.source.SourceSnapshotBindings.encode(
                            acceptedBase.orElseThrow(), binding.getLance().getCatalog())
                            .equals(binding.getLance().getBaseSnapshot()))) {
                throw new IllegalArgumentException("Native output accepted base differs from durable intent");
            }
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

    private static LanceWriteOperation operation(NativeWriteOperation operation) {
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

    record PreparationPage(PreparedSourcePublication publication, SourceObjectPage objects) {}

    static PreparationPage decodePreparation(
            RuntimeCommandResponse response, String fingerprint, CatalogExecutionBinding catalog) {
        try {
            var result = response.getPayload().unpack(PrepareDistributedWriteResult.class);
            if (!result.getUnknownFields().asMap().isEmpty()
                    || !fingerprint.equals(result.getCommitFingerprint())) {
                throw new IllegalArgumentException("Preparation fingerprint differs");
            }
            if (!isPreparedOutcome(result)) {
                return null;
            }
            var header = result.getPublication();
            if (!header.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("Unknown source preparation header fields");
            }
            requireSha256(header.getObjectsSha256());
            var reference = org.pangrova.protocol.source.SourceSnapshotBindings.decode(header.getCandidate(), catalog);
            if (!reference.membershipId().equals(fingerprint)
                    || reference.acceptedOriginRevision() != catalog.getCatalogRevision()) {
                throw new IllegalArgumentException("Candidate membership differs");
            }
            var publication = new PreparedSourcePublication(reference, header.getRowCount(), header.getObjectsCount(),
                    header.getObjectsSha256().getDigest().toByteArray(), header.getInheritedMembership());
            var page = result.getObjectPage();
            if (!page.getUnknownFields().asMap().isEmpty() || page.getObjectsList().stream()
                    .anyMatch(object -> !object.getUnknownFields().asMap().isEmpty())) {
                throw new IllegalArgumentException("Unknown source object page fields");
            }
            requireSha256(page.getSha256());
            var objects = new SourceObjectPage(page.getPageIndex(), page.getAfterPath(),
                    page.getObjectsList().stream()
                            .map(object -> new SourceObjectPage.ObjectReference(
                                    object.getRelativePath(), object.getSizeBytes(),
                                    objectPurpose(object.getPurpose())))
                            .toList(),
                    page.getSha256().getDigest().toByteArray(), page.getIsLast());
            return new PreparationPage(publication, objects);
        } catch (com.google.protobuf.InvalidProtocolBufferException | IllegalArgumentException malformed) {
            throw new NativeDatasetControlRpcException("Native preparation response is unconfirmed", malformed);
        }
    }

    private static boolean isPreparedOutcome(PrepareDistributedWriteResult result) {
        if (result.getOutcome()
                == org.pangrova.protocol.v2.WritePreparationOutcome.WRITE_PREPARATION_OUTCOME_UNKNOWN) {
            if (result.hasPublication() || result.hasObjectPage()) {
                throw new IllegalArgumentException("UNKNOWN has no prepared evidence");
            }
            return false;
        }
        if (result.getOutcome()
                != org.pangrova.protocol.v2.WritePreparationOutcome.WRITE_PREPARATION_OUTCOME_PREPARED
                || !result.hasPublication() || !result.hasObjectPage()) {
            throw new IllegalArgumentException("Missing SDK preparation evidence");
        }
        return true;
    }

    private static void requireSamePreparation(PreparedSourcePublication first, PreparedSourcePublication next) {
        var left = first.candidate();
        var right = next.candidate();
        if (!sameCandidateAuthority(left, right) || !sameCandidateFiles(left, right)
                || first.rows() != next.rows() || first.objectsCount() != next.objectsCount()
                || !Arrays.equals(first.objectsSha256(), next.objectsSha256())
                || !first.inheritedMembership().equals(next.inheritedMembership())) {
            throw new IllegalArgumentException("SDK preparation header changed across pages");
        }
    }

    private static boolean sameCandidateAuthority(AcceptedSourceSnapshot left, AcceptedSourceSnapshot right) {
        return left.deployment().equals(right.deployment())
                && left.acceptedOriginRevision() == right.acceptedOriginRevision()
                && left.tenant().equals(right.tenant())
                && left.tableId().equals(right.tableId())
                && left.generation() == right.generation()
                && left.datasetUri().equals(right.datasetUri())
                && left.version() == right.version();
    }

    private static boolean sameCandidateFiles(AcceptedSourceSnapshot left, AcceptedSourceSnapshot right) {
        return left.manifestRelativePath().equals(right.manifestRelativePath())
                && Arrays.equals(left.manifestSha256(), right.manifestSha256())
                && Arrays.equals(left.schemaSha256(), right.schemaSha256())
                && left.manifestSize() == right.manifestSize()
                && left.namingScheme().equals(right.namingScheme())
                && left.membershipId().equals(right.membershipId());
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
        return RuntimeCommand.newBuilder().setProtocol(CanonicalProtocolIdentity.current()).setCommandType(type)
                .setPayload(Any.pack(payload)).build();
    }
    private static long deadline() {
        return Math.addExact(System.currentTimeMillis(), REQUEST_TIMEOUT_MS);
    }

    private static void requireSha256(Checksum digest) {
        if (!digest.getUnknownFields().asMap().isEmpty()
                || digest.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || digest.getDigest().size() != 32) {
            throw new IllegalArgumentException("Native metadata requires exact SHA-256 evidence");
        }
    }
    private static SourceObjectPurpose objectPurpose(org.pangrova.protocol.v2.SourceObjectPurpose value) {
        return switch (value) {
            case SOURCE_OBJECT_PURPOSE_SOURCE -> SourceObjectPurpose.SOURCE;
            case SOURCE_OBJECT_PURPOSE_WINNER_SIDECAR -> SourceObjectPurpose.WINNER_SIDECAR;
            default -> throw new IllegalArgumentException("SDK object purpose is absent or unknown");
        };
    }

}
