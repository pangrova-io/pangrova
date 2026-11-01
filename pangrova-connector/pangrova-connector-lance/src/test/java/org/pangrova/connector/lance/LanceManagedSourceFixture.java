package org.pangrova.connector.lance;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.ByteString;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.query.store.jdbc.anchor.JdbcExecutionAnchorStore;
import org.pangrova.query.store.jdbc.transaction.JdbcWriteCommitJournal;
import org.pangrova.metadata.source.jdbc.JdbcManagedSourceCatalog;
import org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore;
import org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.protocol.catalog.CatalogDeploymentFingerprint;
import org.pangrova.protocol.write.NativeWriteBindings;
import org.pangrova.protocol.source.SourceSnapshotBindings;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.AcceptedSourceSnapshotRef;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DistributedWriteFragment;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteFragment;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.PrepareDistributedWriteResult;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.WritePreparationOutcome;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;
import org.pangrova.persistence.jdbc.security.policy.JdbcRoleStore;
import org.pangrova.spi.security.tenant.Tenant;
import org.pangrova.spi.security.tenant.TenantRole;
import org.pangrova.spi.security.tenant.TenantStatus;
import org.pangrova.spi.connector.metadata.AcceptedSourceSnapshot;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.SourceObjectPage;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.query.coordination.ExecutionAnchor;
import org.pangrova.persistence.api.query.transaction.ExecutionTransactionFence;
import org.pangrova.security.spi.jdbc.CurrentModelAuthority;
import org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.connector.coordination.WriteCommitEntry;
import org.pangrova.spi.connector.coordination.WriteCommitFingerprint;
import org.pangrova.spi.connector.coordination.WriteDeclarationSnapshot;
import org.postgresql.ds.PGSimpleDataSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Actual production PostgreSQL authority used by Java and Native SDK fixtures.
 * Local maps retain allocated output handles and locally owned leases, never source visibility.
 * No JNI class is loaded here, so Java test compilation can precede the Native test build.
 */
public final class LanceManagedSourceFixture {
    static final String TENANT = "tenant-a";
    static final String PRINCIPAL = "lance-fixture-writer";
    private static final String OWNER = "lance-fixture-broker";
    private static final Duration LEASE = Duration.ofSeconds(60);
    private final String incarnation = UUID.randomUUID().toString();
    private final PGSimpleDataSource dataSource = new PGSimpleDataSource();
    private final JdbcExecutionAnchorStore anchors;
    private final JdbcWriteCommitJournal journal;
    private final JdbcManagedSourceCatalog authority;
    private final LanceCatalogStore store;
    private final CatalogExecutionBinding catalog;
    private final Map<String, ExecutionAnchor> owned = new ConcurrentHashMap<>();
    private final Map<String, Allocation> allocations = new ConcurrentHashMap<>();
    private final Map<String, org.pangrova.spi.connector.metadata.SourcePrepareInvocation> invocations = new ConcurrentHashMap<>();
    private final Map<String, org.pangrova.spi.connector.metadata.SourceCleanupBatch> cleanupBatches = new ConcurrentHashMap<>();
    private final Map<String, org.pangrova.spi.connector.metadata.SourcePublicationReservation> reservations
            = new ConcurrentHashMap<>();

    public LanceManagedSourceFixture() {
        this("test-lance", java.nio.file.Path.of(System.getProperty("java.io.tmpdir")));
    }

    LanceManagedSourceFixture(String catalogName, java.nio.file.Path warehouse) {
        this(catalogName, warehouse, GenerationAuthorityFixtures.modelAuthority(),
                new org.pangrova.query.store.jdbc.anchor.JdbcExecutionTransactionFence());
    }

    LanceManagedSourceFixture(String catalogName, java.nio.file.Path warehouse,
            CurrentModelAuthority models, ExecutionTransactionFence executionFence) {
        var postgres = LanceCatalogTestDatabase.instance();
        dataSource.setURL(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        SchemaMigrationRunner.migrate(dataSource);
        var metadata = new JdbcMetadataStore(dataSource);
        var descriptor = metadata.getCatalogDescriptor(catalogName).orElseGet(() -> {
            var created = new CatalogDescriptor(catalogName, CatalogType.CONNECTOR, "lance", CatalogState.ACTIVE,
                    Map.of("warehouse", warehouse.toAbsolutePath().normalize().toString()),
                    "Real PG authority for SDK fixtures", Set.of(), UUID.randomUUID().toString(), 1);
            metadata.saveCatalogDescriptor(created);
            return created;
        });
        catalog = CatalogExecutionBinding.newBuilder().setCatalogName(descriptor.name()).setConnectorProvider("lance")
                .setCatalogIncarnationId(descriptor.incarnationId()).setCatalogRevision(descriptor.revision())
                .setConfigurationFingerprint(CatalogDeploymentFingerprint.checksum(new CatalogDeploymentResponse(
                        descriptor.name(), descriptor.provider(), descriptor.incarnationId(), descriptor.revision(),
                        descriptor.properties(), descriptor.allowedTenants()))).build();
        anchors = new JdbcExecutionAnchorStore(dataSource);
        journal = new JdbcWriteCommitJournal(dataSource);
        authority = new JdbcManagedSourceCatalog(dataSource, new ManagedSourceCatalog.Deployment(descriptor.name(),
                descriptor.incarnationId(), descriptor.revision()), execution -> Optional.ofNullable(owned.get(execution))
                        .orElseThrow(() -> new IllegalStateException("Fixture does not own this real PG execution")),
                executionFence, models);
        store = new LanceCatalogStore(authority);
        provisionSubject();
    }

    public CatalogExecutionBinding catalog() { return catalog; }
    /** Returns the exact current PG catalog warehouse, never the caller JVM temporary directory. */
    public java.nio.file.Path warehouse() {
        var descriptor = new JdbcMetadataStore(dataSource).getCatalogDescriptor(catalog.getCatalogName())
                .orElseThrow(() -> new SecurityException("Fixture catalog no longer exists"));
        var fingerprint = CatalogDeploymentFingerprint.checksum(new CatalogDeploymentResponse(
                descriptor.name(), descriptor.provider(), descriptor.incarnationId(), descriptor.revision(),
                descriptor.properties(), descriptor.allowedTenants()));
        if (descriptor.state() != CatalogState.ACTIVE || !"lance".equals(descriptor.provider())
                || !descriptor.incarnationId().equals(catalog.getCatalogIncarnationId())
                || descriptor.revision() != catalog.getCatalogRevision()
                || !fingerprint.equals(catalog.getConfigurationFingerprint())
                || (!descriptor.allowedTenants().isEmpty() && !descriptor.allowedTenants().contains(TENANT))) {
            throw new SecurityException("Fixture warehouse requires its exact current catalog deployment");
        }
        String configured = descriptor.properties().get("warehouse");
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("Actual fixture catalog has no warehouse");
        }
        var result = java.nio.file.Path.of(configured);
        if (!result.isAbsolute() || !result.equals(result.normalize())) {
            throw new IllegalStateException("SDK fixture requires an absolute canonical local warehouse");
        }
        return result;
    }

    public javax.sql.DataSource dataSource() { return dataSource; }
    public ManagedSourceCatalog authority() { return authority; }
    LanceCatalogStore store() { return store; }
    public JdbcWriteCommitJournal journal() { return journal; }

    /** Renews only leases still owned by this actual broker process; expired leases never revive. */
    public void renewOwned() {
        var identities = new java.util.HashSet<LeaseIdentity>();
        owned.values().forEach(anchor -> identities.add(new LeaseIdentity(anchor.ownerId(), anchor.ownerIncarnation())));
        identities.forEach(identity -> anchors.renewOwned(identity.owner(), identity.incarnation(), LEASE));
    }

    /** Actual PG yield and reacquisition, preserving the original durable winners and intent identity. */
    public synchronized ExecutionAnchor handoff(String execution) {
        var previous = Optional.ofNullable(owned.get(execution)).orElseThrow();
        if (journal.getEntry(execution).isEmpty()) throw new IllegalStateException("Recovery requires a durable actual intent");
        if (!anchors.yield(previous)) throw new IllegalStateException("Actual original execution could not yield");
        var next = anchors.tryAcquire(execution, OWNER + "-recovery", UUID.randomUUID().toString(), LEASE).orElseThrow();
        if (next.fencingToken() <= previous.fencingToken()) throw new IllegalStateException("Actual recovery did not fence its predecessor");
        owned.put(execution, next);
        return next;
    }
    private record LeaseIdentity(String owner, String incarnation) {}

    public synchronized Allocation allocate(String uri, String writeId, LanceWriteOperation operation,
            long version, byte[] schemaSha256, byte[] arrowIpc, int partitionId) {
        String execution = "lance-sdk-" + digest(uri + "\n" + writeId);
        Allocation previous = allocations.get(execution);
        if (previous != null) {
            if (!previous.output().allocation().datasetUri().equals(uri)
                    || !previous.output().allocation().writeId().equals(writeId)
                    || previous.output().allocation().readVersion() != version
                    || operation(previous.output().allocation().operation()) != operation
                    || !Arrays.equals(previous.output().table().schemaFence(), schemaSha256)) {
                throw new IllegalStateException("Fixture allocation replay differs");
            }
            return attempt(previous, partitionId);
        }
        var tableName = new SchemaTableName("sdk", "t_" + digest(uri));
        var actualSchema = LanceFixtureSchema.metadata(tableName, arrowIpc);
        var current = store.load(catalog.getCatalogName(), TENANT, tableName);
        if (current == null) {
            if (version != 0 || operation != LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE) {
                throw new IllegalStateException("No accepted source exists for this write base");
            }
            store.createSchema(catalog.getCatalogName(), TENANT, tableName.schema());
            current = new LanceTableHandle(catalog.getCatalogName(), TENANT, tableName, UUID.randomUUID().toString(), uri,
                    actualSchema.typedColumns().stream().map(LanceColumn::from).toList(), Map.of(), schemaSha256, 0, 1, 0);
            store.create(current);
            grant(tableName);
        }
        var base = version == 0 ? current : store.loadSnapshot(
                (LanceTableHandle) current.withTimeTravelSnapshot(version)).table();
        if (!base.columns().equals(actualSchema.typedColumns().stream().map(LanceColumn::from).toList())
                || !Arrays.equals(base.schemaFence(), schemaSha256) || base.version() != version
                || !base.datasetUri().equals(uri)) throw new IllegalStateException("Actual fixture schema or base differs");
        AcceptedSourceSnapshot accepted = version == 0 ? null : store.loadSnapshot(base).reference();
        var allocation = new NativeWriteAllocation(uri, switch (operation) {
            case LANCE_WRITE_OPERATION_CREATE -> NativeWriteAllocation.Operation.CREATE;
            case LANCE_WRITE_OPERATION_APPEND -> NativeWriteAllocation.Operation.APPEND;
            case LANCE_WRITE_OPERATION_OVERWRITE -> NativeWriteAllocation.Operation.OVERWRITE;
            default -> throw new IllegalArgumentException("Unknown write operation");
        }, version, schemaSha256, writeId, "_pangrova/write-staging/" + writeId, "generation-1", actualSchema.columns());
        var output = new LanceOutputHandle(base, allocation,
                operation == LanceWriteOperation.LANCE_WRITE_OPERATION_OVERWRITE ? "OVERWRITE" : "INSERT", accepted);
        var anchor = anchors.tryAcquire(execution, OWNER, incarnation, LEASE).orElseThrow();
        owned.put(execution, anchor);
        var result = new Allocation(execution, output, binding(output), anchor, null, null);
        allocations.put(execution, result);
        return attempt(result, partitionId);
    }

    /** Records the exact production redo record before SDK preparation is issued. */
    public WriteCommitEntry recordIntent(String execution, List<byte[]> fragments) {
        var allocation = allocation(execution);
        var output = allocation.output();
        if (journal.getEntry(execution).filter(value -> value.state() == WriteCommitEntry.State.COMMITTED).isEmpty()) {
            validateActualWinners(allocation, fragments);
        }
        var receipt = receipt(output.table());
        var intent = new WriteCommitFingerprint.Intent(execution, TENANT, catalog.getCatalogName(),
                output.table().table().toString(), output.mode(), output.table().version(),
                allocation.binding().toByteArray(), PRINCIPAL, receipt, null);
        String fingerprint = WriteCommitFingerprint.compute(intent, fragments);
        var anchor = allocation.anchor();
        var entry = new WriteCommitEntry(execution, catalog.getCatalogName(), output.table().table().toString(),
                WriteCommitEntry.State.PENDING, Instant.now().toEpochMilli(), fragments, output.mode(), fingerprint,
                TENANT, PRINCIPAL, receipt, output.table().version(), allocation.binding().toByteArray(), fingerprint,
                anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken(), null, null);
        journal.recordIntent(entry);
        return journal.getEntry(execution).orElseThrow();
    }

    public Optional<AcceptedSourceSnapshotRef> accepted(String execution) {
        var entry = journal.getEntry(execution).orElseThrow();
        return store.accepted(allocation(execution).output(), entry.fragments(), entry.commitContext())
                .map(value -> SourceSnapshotBindings.encode(value.reference(), catalog));
    }

    public PrepareDistributedWriteRequest reserve(String execution, List<byte[]> fragments) throws Exception {
        var entry = recordIntent(execution, fragments);
        var allocation = allocation(execution);
        var output = allocation.output();
        var anchor = owned.get(execution);
        String fingerprint = entry.commitFingerprint();
        var reservation = store.reserve(output, fragments, entry.commitContext());
        reservations.put(execution, reservation);
        var owner = owner(anchor);
        var request = PrepareDistributedWriteRequest.newBuilder().setSecurity(SecurityContext.newBuilder()
                        .setTenantId(TENANT).setPrincipal(PRINCIPAL))
                .setOutputBinding(allocation.binding()).setExecutionId(execution).setCommitFingerprint(fingerprint)
                .setExecutionOwner(owner).setIntentExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                        .setOwnerId(entry.ownerId()).setOwnerIncarnation(entry.ownerIncarnation())
                        .setFencingToken(entry.executionFencingToken())).setControlEpoch(1)
                .setDeadlineEpochMs(Instant.now().plusSeconds(30).toEpochMilli())
                .setReservation(org.pangrova.protocol.v2.SourcePublicationReservation.newBuilder()
                        .setTableId(output.table().tableId()).setTableGeneration(1).setTenantId(TENANT)
                        .setManifestRelativePath(reservation.manifestRelativePath()))
                .setObjectPageSize(SourceObjectPage.MAX_PAGE_OBJECTS);
        if (output.acceptedBase() != null) request.getReservationBuilder()
                .setBaseSnapshot(SourceSnapshotBindings.encode(output.acceptedBase(), catalog));
        for (byte[] bytes : fragments) {
            var fragment = LanceWriteFragment.parseFrom(bytes);
            request.addFragments(DistributedWriteFragment.newBuilder().setTaskAttempt(fragment.getTaskAttempt())
                    .setCommitMetadata(ByteString.copyFrom(bytes)).setSha256(ProtocolChecksums.sha256(bytes)));
        }
        return request.build();
    }

    public PrepareDistributedWriteRequest issuePreparation(PrepareDistributedWriteRequest request,
            java.nio.file.Path actorSocket, String actorNonce) throws java.io.IOException {
        var bare = request.toBuilder().clearInvocationId().build();
        var reservation = Optional.ofNullable(reservations.get(request.getExecutionId())).orElseThrow();
        LanceSdkPreparationActor.requireListeningActor(actorSocket, actorNonce, bare);
        String endpoint = "unix://" + actorSocket.toAbsolutePath().toUri().getRawPath();
        var invocation = authority.issuePreparation(reservation, request.getObjectPageIndex(), request.getAfterObjectPath(),
                endpoint, ProtocolChecksums.sha256(bare.toByteArray()).getDigest().toByteArray());
        invocations.put(invocation.invocationId(), invocation);
        return bare.toBuilder().setInvocationId(invocation.invocationId()).build();
    }

    public AcceptedSourceSnapshotRef register(String execution, PrepareDistributedWriteResult result) {
        var publication = registerObjects(execution, result);
        if (!result.getObjectPage().getIsLast()) return null;
        return acceptRegistered(execution, publication);
    }

    org.pangrova.spi.connector.metadata.PreparedSourcePublication registerObjects(
            String execution, PrepareDistributedWriteResult result) {
        var reservation = Optional.ofNullable(reservations.get(execution)).orElseThrow();
        if (result.getOutcome() != WritePreparationOutcome.WRITE_PREPARATION_OUTCOME_PREPARED
                || !result.getCommitFingerprint().equals(reservation.fingerprint())
                || !result.hasPublication() || !result.hasObjectPage()) {
            throw new IllegalArgumentException("Fixture cannot accept an unconfirmed SDK preparation");
        }
        var invocation = Optional.ofNullable(invocations.get(result.getInvocationId())).orElseThrow(
                () -> new IllegalArgumentException("SDK fixture has no matching durable issuance"));
        if (!result.getSdkJoined()) throw new IllegalArgumentException("SDK future did not actually join");
        var page = result.getObjectPage();
        if (page.getPageIndex() != invocation.pageIndex()
                || !page.getAfterPath().equals(invocation.afterPath())) {
            throw new IllegalArgumentException("SDK object page sequence or cursor differs");
        }
        authority.acknowledgePreparation(reservation, invocation,
                ProtocolChecksums.sha256(result.toByteArray()).getDigest().toByteArray());
        var header = result.getPublication();
        var publication = new org.pangrova.spi.connector.metadata.PreparedSourcePublication(
                SourceSnapshotBindings.decode(header.getCandidate(), catalog), header.getRowCount(), header.getObjectsCount(),
                sha(header.getObjectsSha256()), header.getInheritedMembership());
        authority.registerObjects(reservation, publication, new SourceObjectPage(page.getPageIndex(), page.getAfterPath(),
                page.getObjectsList().stream().map(object -> new SourceObjectPage.ObjectReference(
                        object.getRelativePath(), object.getSizeBytes(), objectPurpose(object.getPurpose()))).toList(), sha(page.getSha256()), page.getIsLast()));
        return publication;
    }

    AcceptedSourceSnapshotRef acceptRegistered(String execution,
            org.pangrova.spi.connector.metadata.PreparedSourcePublication publication) {
        var reservation = Optional.ofNullable(reservations.get(execution)).orElseThrow();
        var allocation = allocation(execution);
        long rows = journal.getEntry(execution).orElseThrow().fragments().stream().mapToLong(bytes -> {
            try { return LanceWriteFragment.parseFrom(bytes).getRowsWritten(); }
            catch (com.google.protobuf.InvalidProtocolBufferException error) { throw new IllegalArgumentException(error); }
        }).reduce(0, Math::addExact);
        var accepted = store.accept(allocation.output(), reservation, publication, rows);
        var committed = journal.getEntry(execution).orElseThrow();
        if (committed.state() != WriteCommitEntry.State.COMMITTED || committed.publication() == null
                || !Arrays.equals(committed.publication().manifestSha256(), accepted.manifestDigest())) {
            throw new IllegalStateException("PG acceptance and durable journal are not the same publication");
        }
        return SourceSnapshotBindings.encode(accepted.reference(), catalog);
    }

    /** Returns only formal references loaded from accepted current/historical PostgreSQL rows. */
    public AcceptedSourceSnapshotRef reference(String uri, long version) {
        var current = store.load(catalog.getCatalogName(), TENANT, new SchemaTableName("sdk", "t_" + digest(uri)));
        if (current == null || current.version() == 0) throw new IllegalStateException("No accepted source");
        var selector = version == 0 ? current : (LanceTableHandle) current.withTimeTravelSnapshot(version);
        return SourceSnapshotBindings.encode(store.loadSnapshot(selector).reference(), catalog);
    }

    public org.pangrova.spi.connector.metadata.DistributedWriteAbortReceipt abort(String execution) {
        var entry = journal.getEntry(execution).orElseThrow();
        return store.abort(allocation(execution).output(), entry.commitContext());
    }

    public void requireCleanupWinners(String execution, List<byte[]> fragments) {
        var entry = journal.getEntry(execution).orElseThrow();
        var expected = entry.fragments().stream().map(Base64.getEncoder()::encodeToString).collect(java.util.stream.Collectors.toSet());
        var actual = fragments.stream().map(Base64.getEncoder()::encodeToString).collect(java.util.stream.Collectors.toSet());
        if (fragments.size() != entry.fragments().size() || actual.size() != fragments.size() || !actual.equals(expected)) {
            throw new IllegalArgumentException("Cleanup winners differ from the actual durable source intent");
        }
    }

    org.pangrova.spi.connector.plugin.Connector connector() {
        var descriptor = new JdbcMetadataStore(dataSource).getCatalogDescriptor(catalog.getCatalogName()).orElseThrow();
        return new LanceConnector(descriptor.name(), descriptor.properties().get("warehouse"), descriptor.properties(),
                Optional.empty(), Optional.of(authority));
    }

    public Optional<org.pangrova.protocol.v2.CleanupSourceObjectsRequest> cleanupRequest(String execution) {
        var reservation = Optional.ofNullable(reservations.get(execution)).orElseThrow();
        return authority.reserveCleanup(reservation).map(batch -> {
            cleanupBatches.put(batch.cleanupId(), batch);
            var request = org.pangrova.protocol.v2.CleanupSourceObjectsRequest.newBuilder().setCatalog(catalog)
                    .setSecurity(SecurityContext.newBuilder().setTenantId(TENANT).setPrincipal(PRINCIPAL))
                    .setExecutionId(execution).setCommitFingerprint(batch.fingerprint()).setExecutionOwner(owner(owned.get(execution)))
                    .setControlEpoch(1).setDeadlineEpochMs(Instant.now().plusSeconds(30).toEpochMilli())
                    .setCleanupId(batch.cleanupId()).setDatasetUri(batch.datasetUri())
                    .setObjectsSha256(org.pangrova.protocol.v2.Checksum.newBuilder()
                            .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                            .setDigest(ByteString.copyFrom(batch.sha256())));
            for (var object : batch.objects()) request.addObjects(org.pangrova.protocol.v2.PreparedSourceObject.newBuilder()
                    .setRelativePath(object.relativePath()).setSizeBytes(object.sizeBytes())
                    .setPurpose(object.purpose() == SourceObjectPage.Purpose.SOURCE
                            ? org.pangrova.protocol.v2.SourceObjectPurpose.SOURCE_OBJECT_PURPOSE_SOURCE
                            : org.pangrova.protocol.v2.SourceObjectPurpose.SOURCE_OBJECT_PURPOSE_WINNER_SIDECAR));
            return request.build();
        });
    }

    public void completeCleanup(String execution, org.pangrova.protocol.v2.CleanupSourceObjectsResult result) {
        var batch = Optional.ofNullable(cleanupBatches.get(result.getCleanupId())).orElseThrow();
        if (!result.getSdkJoined() || result.getDeletedCount() != batch.objects().size()
                || !Arrays.equals(result.getObjectsSha256().getDigest().toByteArray(), batch.sha256())
                || result.getObjectsSha256().getAlgorithm() != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256) {
            throw new IllegalArgumentException("SDK deletion receipt does not match its exact PG cleanup batch");
        }
        authority.completeCleanup(Optional.ofNullable(reservations.get(execution)).orElseThrow(), batch);
    }

    private void validateActualWinners(Allocation allocation, List<byte[]> fragments) {
        var nativeOutput = allocation.output().allocation();
        var partitions = new java.util.HashSet<String>();
        if (fragments.isEmpty() || fragments.size() > 4096) throw new IllegalArgumentException("Invalid fixture winners");
        for (byte[] bytes : fragments) {
            try {
                var fragment = LanceWriteFragment.parseFrom(bytes);
                var attempt = fragment.getTaskAttempt();
                var reference = fragment.getTransaction();
                String prefix = NativeWriteBindings.attemptPrefix(nativeOutput.stagingPrefix(), attempt);
                if (fragment.getContractVersion() != 2 || !fragment.getDatasetUri().equals(nativeOutput.datasetUri())
                        || !fragment.getWriteId().equals(nativeOutput.writeId())
                        || fragment.getOperation() != operation(nativeOutput.operation())
                        || fragment.getReadVersion() != nativeOutput.readVersion()
                        || !Arrays.equals(fragment.getSchemaFenceSha256().toByteArray(), nativeOutput.schemaFenceSha256())
                        || !attempt.getQueryOrJobId().equals(allocation.executionId())
                        || !attempt.getExecutionOwner().equals(owner(allocation.anchor()))
                        || !partitions.add(attempt.getStageId() + "/" + attempt.getPartitionId())
                        || !reference.getTaskAttempt().equals(attempt) || !reference.getTenantId().equals(TENANT)
                        || !reference.getWriteId().equals(nativeOutput.writeId())
                        || !reference.getGeneration().equals(nativeOutput.generation())
                        || !reference.getRelativeObjectKey().startsWith(prefix + "/transaction-")) {
                    throw new IllegalArgumentException("Fixture receipt differs from the actual issued allocation");
                }
                // These are genuine Native-finish receipts. Validate their actual immutable
                // sidecar before admitting them as winners; malformed fixture input must not
                // masquerade as a second immutable redo intent for the same query.
                new SourceObjectPage.ObjectReference(reference.getRelativeObjectKey(), reference.getSizeBytes(),
                        SourceObjectPage.Purpose.WINNER_SIDECAR);
                if (reference.getSizeBytes() <= 0 || reference.getSizeBytes() > 64L * 1024 * 1024) {
                    throw new IllegalArgumentException("Fixture sidecar is unbounded");
                }
                var path = java.nio.file.Path.of(nativeOutput.datasetUri()).resolve(reference.getRelativeObjectKey());
                if (java.nio.file.Files.size(path) != reference.getSizeBytes()) {
                    throw new IllegalArgumentException("Actual fixture sidecar size differs from its receipt");
                }
                byte[] actual = java.nio.file.Files.readAllBytes(path);
                if (!ProtocolChecksums.sha256(actual).equals(reference.getSha256())) {
                    throw new IllegalArgumentException("Actual fixture sidecar SHA-256 differs from its receipt");
                }
            } catch (java.io.IOException failure) {
                throw new IllegalArgumentException("Fixture winner does not name an actual Native sidecar", failure);
            }
        }
    }

    public WriteOutputBinding allocatedBinding(String execution) { return allocation(execution).binding(); }
    org.pangrova.spi.connector.metadata.SourcePublicationReservation reservation(String execution) {
        return Optional.ofNullable(reservations.get(execution)).orElseThrow();
    }

    private Allocation allocation(String execution) {
        return Optional.ofNullable(allocations.get(execution)).orElseThrow(() -> new IllegalArgumentException("Unknown allocation"));
    }

    private Allocation attempt(Allocation allocation, int partitionId) {
        var identity = TaskAttemptId.newBuilder().setQueryOrJobId(allocation.executionId()).setStageId(1)
                .setPartitionId(partitionId).setPlanEpoch(1).setAttemptId(0).setFencingToken(allocation.anchor().fencingToken())
                .setTaskId("task-" + partitionId).setControlEpoch(1).setExecutionOwner(owner(allocation.anchor())).build();
        return new Allocation(allocation.executionId(), allocation.output(), allocation.binding(), allocation.anchor(),
                identity, NativeWriteBindings.attemptPrefix(allocation.output().allocation().stagingPrefix(), identity));
    }

    private WriteOutputBinding binding(LanceOutputHandle output) {
        var codec = new LanceOutputHandleCodec();
        byte[] payload = codec.serialize(output);
        var nativeOutput = output.allocation();
        var lance = LanceWriteBinding.newBuilder().setCatalog(catalog)
                .setOutputHandle(SerializedConnectorOutputHandle.newBuilder().setTypeUrl(codec.typeUrl())
                        .setCodecVersion(codec.codecVersion()).setPayload(ByteString.copyFrom(payload))
                        .setPayloadChecksum(ProtocolChecksums.sha256(payload)).addAllWriteColumnNames(nativeOutput.writeColumnNames()))
                .setDatasetUri(nativeOutput.datasetUri()).setOperation(operation(nativeOutput.operation()))
                .setExpectedReadVersion(nativeOutput.readVersion()).setSchemaFenceSha256(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(nativeOutput.schemaFenceSha256())))
                .setWriteId(nativeOutput.writeId()).setGeneration(nativeOutput.generation())
                .setTransactionStagingPrefix(nativeOutput.stagingPrefix()).setAllocationStagingPrefix(nativeOutput.stagingPrefix())
                .addAllWriteColumnNames(nativeOutput.writeColumnNames());
        if (output.acceptedBase() != null) lance.setBaseSnapshot(SourceSnapshotBindings.encode(output.acceptedBase(), catalog));
        return WriteOutputBinding.newBuilder().setLance(lance).build();
    }

    private static ExecutionOwnerIdentity owner(ExecutionAnchor anchor) {
        return ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId()).setOwnerIncarnation(anchor.ownerIncarnation())
                .setFencingToken(anchor.fencingToken()).build();
    }

    private static LanceWriteOperation operation(NativeWriteAllocation.Operation operation) {
        return switch (operation) {
            case CREATE -> LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE;
            case APPEND -> LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND;
            case OVERWRITE -> LanceWriteOperation.LANCE_WRITE_OPERATION_OVERWRITE;
        };
    }

    private static WriteAuthorizationReceipt receipt(LanceTableHandle table) {
        var name = new QualifiedTableName(table.catalog(), table.table().schema(), table.table().table());
        var metadata = new org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata(table.table(),
                table.columns().stream().map(LanceColumn::name).toList(), table.columns().stream().map(LanceColumn::metadata).toList(), Map.of());
        var declaration = WriteDeclarationSnapshot.capture(name, metadata, GeneratedColumnSet.newBuilder().setSchemaVersion(1).build().toByteArray(), false);
        return DeferredAuthorizationScopeCodec.encodeWriteReceipt(new DeferredAuthorizationScope(List.of(
                new DeferredAuthorizationScope.TablePermission(name, Set.of(), Privilege.INSERT,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())),
                Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, List.of()))));
    }

    private void provisionSubject() {
        var roles = new JdbcRoleStore(dataSource);
        roles.initSchema();
        if (roles.findRole("lance_sdk_writer").isEmpty()) roles.createRole(Role.of("lance_sdk_writer"));
        roles.grantRoleToUser(UserRoleAssignment.of(PRINCIPAL, "lance_sdk_writer"));
        var tenants = new JdbcTenantStore(dataSource);
        tenants.initSchema();
        if (tenants.getTenant(TENANT).isEmpty()) tenants.createTenant(new Tenant(TENANT, TENANT, TenantStatus.ACTIVE,
                null, List.of(), List.of(), null, Map.of(), Instant.EPOCH, PRINCIPAL));
        tenants.addUserToTenant(TENANT, PRINCIPAL, TenantRole.MEMBER);
    }

    private void grant(SchemaTableName table) {
        new JdbcGrantStore(dataSource).addGrant(PrivilegeGrant.of("lance_sdk_writer", Privilege.INSERT, "TABLE",
                catalog.getCatalogName(), table.schema(), table.table(), null));
    }

    private static byte[] sha(Checksum checksum) {
        if (checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256 || checksum.getDigest().size() != 32) {
            throw new IllegalArgumentException("Actual SHA-256 receipt required");
        }
        return checksum.getDigest().toByteArray();
    }

    private static String digest(String value) {
        return HexFormat.of().formatHex(ProtocolChecksums.sha256(value.getBytes(StandardCharsets.UTF_8))
                .getDigest().toByteArray());
    }

    public record Allocation(String executionId, LanceOutputHandle output, WriteOutputBinding binding,
                             ExecutionAnchor anchor, TaskAttemptId attempt, String attemptPrefix) {}
    private static SourceObjectPage.Purpose objectPurpose(org.pangrova.protocol.v2.SourceObjectPurpose value) {
        return switch (value) {
            case SOURCE_OBJECT_PURPOSE_SOURCE -> SourceObjectPage.Purpose.SOURCE;
            case SOURCE_OBJECT_PURPOSE_WINNER_SIDECAR -> SourceObjectPage.Purpose.WINNER_SIDECAR;
            default -> throw new IllegalArgumentException("SDK object purpose is absent or unknown");
        };
    }

}
