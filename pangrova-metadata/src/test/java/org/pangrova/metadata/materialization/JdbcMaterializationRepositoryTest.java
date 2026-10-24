package org.pangrova.metadata.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.Binding;
import org.pangrova.materialization.domain.ColumnDefinition;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.GcLedgerEntry;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.IncarnationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.OutputSchema;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.PolicySnapshot;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.SourcePin;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.MaterializationRepository;
import org.pangrova.materialization.port.PublicationResult;
import org.pangrova.materialization.port.PublishCommand;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.materialization.CommitEvidence;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;
import org.pangrova.testing.ContainerTestBase;

/**
 * Exercises the JDBC authority against the real V68 schema.
 *
 * <p>Each test migrates an isolated schema, so durable auxiliary tables
 * (anchors, leases, commit journal) are private while the shared
 * {@code materialization} schema is partitioned by a random tenant.
 */
class JdbcMaterializationRepositoryTest extends ContainerTestBase {

    private static final long CONTROL_EPOCH = 7L;
    private static final byte[] MANIFEST = manifestBytes();
    private static final String MANIFEST_HASH = "01".repeat(32);

    /**
     * The kernels tables are fully qualified by the migrations, so they are
     * shared by every test in this container. Tenant-scoped reads are isolated
     * by construction; the scheduler claim is global by design and needs a
     * clean due set per test.
     */
    @BeforeEach
    void clearSharedKernelState() throws SQLException {
        try (Connection connection = SharedPostgresContainer.dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            try (var probe = statement.executeQuery(
                    "SELECT to_regclass('materialization.objects')")) {
                if (!probe.next() || probe.getString(1) == null) {
                    return;
                }
            }
            statement.execute("TRUNCATE materialization.events, materialization.commands, "
                    + "materialization.gc_ledger, materialization.reader_pins, "
                    + "materialization.source_pins, materialization.commit_evidence, "
                    + "materialization.generations, materialization.attempts, "
                    + "materialization.dependencies, materialization.schedules, "
                    + "materialization.definition_revisions, materialization.objects, "
                    + "materialization.registry_epochs CASCADE");
        }
    }

    @Test
    void reservesCommitsAndPublishesExactlyOnceWithIdempotentReplay() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("mat_repo");
        MaterializationRepository repository = new JdbcMaterializationRepository(dataSource);
        MaterializationCatalog catalog = new JdbcMaterializationCatalog(dataSource);

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String tenant = uniqueTenant();
        MaterializationId object = objectId(tenant);
        Principal requester = new Principal("user:alice");
        Principal definer = new Principal("role:analytics");
        IncarnationId incarnation = IncarnationId.random();
        SourceRef source = new SourceRef("iceberg", "lake", "raw", "orders");
        SourceSnapshotRef snapshot = new SourceSnapshotRef(
                "iceberg", "snap-1", "b".repeat(64), "c".repeat(64), now, true);
        SourceVector vector = new SourceVector(Map.of(source, snapshot), 1);

        MaterializationObject header = MaterializationObject.creating(object,
                MaterializationKind.DYNAMIC_TABLE, incarnation, definer, SecurityMode.DEFINER,
                RefreshMode.FULL, Optional.empty(), now);
        DefinitionRevision definition = definition(object, source, definer, now);
        Schedule schedule = Schedule.scheduled(object, new TargetLag(Duration.ofMinutes(5)),
                now.minusSeconds(1));
        CommandId creationCommand = CommandId.random();
        repository.createObject(header, definition, schedule, creationCommand,
                event(MaterializationEventType.OBJECT_CREATED, object, requester, definer, now));

        assertThat(repository.findObject(object)).get()
                .satisfies(found -> {
                    assertThat(found.state()).isEqualTo(ObjectState.Creating.INSTANCE);
                    assertThat(found.revision()).isZero();
                    assertThat(found.incarnation()).isEqualTo(incarnation);
                });
        assertThat(catalog.findPublished(object)).isEmpty();
        long epochAfterCreate = catalog.registryEpoch(new TenantId(tenant));
        assertThat(epochAfterCreate).isPositive();
        assertThat(repository.findDefinition(object, 1)).get()
                .satisfies(found -> assertThat(found.fingerprint())
                        .isEqualTo(definition.fingerprint()));

        Authority authority = seedCoordinatorAuthority(dataSource);
        ExecutionFence fence = new ExecutionFence(
                authority.anchor(), CONTROL_EPOCH, now.plus(Duration.ofMinutes(10)));
        PhysicalTableRef candidateTable = candidateTable();
        AttemptId attemptId = AttemptId.random();
        GenerationId generationId = GenerationId.random();
        RefreshAttempt attempt = RefreshAttempt.reserve(attemptId, header, definition, requester,
                new RefreshAttempt.AttemptScope(Optional.empty(), RefreshKind.Full.INSTANCE,
                        vector, TriggerKind.CREATION, fence, candidateTable), now);
        Generation candidate = Generation.candidate(generationId, object, incarnation, 1,
                attemptId, Optional.empty(), candidateTable, RefreshKind.Full.INSTANCE, vector, now);
        MaterializationWriteCapability capability = new MaterializationWriteCapability(object,
                incarnation, 1, attemptId, candidateTable, generationId, vector.canonicalHash(),
                definer, CONTROL_EPOCH, now, now.plus(Duration.ofMinutes(10)));
        repository.reserveAttempt(attempt, candidate,
                List.of(SourcePin.acquired(attemptId, source, snapshot,
                        now.plus(Duration.ofMinutes(30)))),
                capability, 0,
                event(MaterializationEventType.ATTEMPT_RESERVED, object, requester, definer, now));

        assertThat(repository.findWriteCapability(attemptId)).get()
                .satisfies(found -> {
                    assertThat(found.candidateTable().qualifiedName())
                            .isEqualTo(candidateTable.qualifiedName());
                    assertThat(found.sourceVectorHash()).isEqualTo(vector.canonicalHash());
                });
        assertThat(repository.findActiveAttempt(object)).get()
                .satisfies(found -> assertThat(found.id()).isEqualTo(attemptId));
        assertThat(repository.findGeneration(generationId)).get()
                .satisfies(found -> assertThat(found.state())
                        .isEqualTo(GenerationState.Candidate.INSTANCE));

        CommitEvidence evidence = new CommitEvidence("iceberg", "d".repeat(64), MANIFEST_HASH,
                "iceberg-v3", now);
        seedCommittedJournal(dataSource, authority, tenant, requester, candidateTable,
                evidence, now);
        // The coordinator purges the redo journal after retiring the write; evidence must
        // already be captured durably at that point.
        purgeJournal(dataSource, authority.anchor());
        repository.recordCommitEvidence(attemptId, evidence, now);

        assertThat(repository.findAttempt(attemptId)).get()
                .satisfies(found -> {
                    assertThat(found.evidence()).contains(evidence);
                    assertThat(found.state()).isEqualTo(
                            org.pangrova.materialization.domain.AttemptState.Committed.INSTANCE);
                });
        assertThat(repository.findGeneration(generationId)).get()
                .satisfies(found -> assertThat(found.state())
                        .isEqualTo(GenerationState.Committed.INSTANCE));

        // The creation command owns both admission and publication: publishing must
        // upgrade its admission receipt instead of replaying it.
        CommandId publishCommand = creationCommand;
        PublishCommand command = new PublishCommand(object, 0, candidate, attempt, evidence,
                fence, publishCommand);
        Instant publishedAt = now.plusSeconds(1);
        PublicationResult published = repository.publish(command, event(
                MaterializationEventType.GENERATION_PUBLISHED, object, requester, definer,
                publishedAt));

        assertThat(published.newlyPublished()).isTrue();
        assertThat(published.object().state()).isEqualTo(ObjectState.Active.INSTANCE);
        assertThat(published.object().publishedGeneration()).contains(generationId);
        assertThat(published.generation().state()).isEqualTo(GenerationState.Published.INSTANCE);

        MaterializationObject head = repository.findObject(object).orElseThrow();
        assertThat(head.revision()).isEqualTo(1);
        assertThat(head.publishedGeneration()).contains(generationId);
        assertThat(repository.findPublishedGeneration(object)).get()
                .satisfies(found -> {
                    assertThat(found.id()).isEqualTo(generationId);
                    assertThat(found.evidence()).contains(evidence);
                });
        assertThat(repository.findAttempt(attemptId)).get()
                .satisfies(found -> assertThat(found.state())
                        .isEqualTo(org.pangrova.materialization.domain.AttemptState.Published.INSTANCE));

        PublishedMaterialization projection = catalog.findPublished(object).orElseThrow();
        assertThat(projection.generation()).isEqualTo(generationId);
        assertThat(projection.physicalTable().qualifiedName())
                .isEqualTo(candidateTable.qualifiedName());
        assertThat(projection.refreshKind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(projection.sources()).containsExactly(source);
        assertThat(catalog.listPublished(new TenantId(tenant))).containsExactly(projection);
        assertThat(catalog.registryEpoch(new TenantId(tenant))).isGreaterThan(epochAfterCreate);

        CommandRecord receipt = repository.findCommand(new TenantId(tenant), publishCommand)
                .orElseThrow();
        assertThat(receipt.resultReference()).contains(generationId.value().toString());

        PublicationResult replayed = repository.publish(command, event(
                MaterializationEventType.GENERATION_PUBLISHED, object, requester, definer,
                publishedAt.plusSeconds(1)));
        assertThat(replayed.newlyPublished()).isFalse();
        assertThat(replayed.object().revision()).isEqualTo(1);
        assertThat(replayed.generation().id()).isEqualTo(generationId);
        assertThat(repository.findObject(object).orElseThrow().revision()).isEqualTo(1);

        PublishCommand stale = new PublishCommand(object, 0, candidate, attempt, evidence, fence,
                CommandId.random());
        assertThatThrownBy(() -> repository.publish(stale, event(
                MaterializationEventType.GENERATION_PUBLISHED, object, requester, definer,
                publishedAt.plusSeconds(2))))
                .isInstanceOfSatisfying(MaterializationException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CONCURRENT_PUBLICATION));

        Schedule claimed = repository.claimDueSchedule("scheduler-1",
                now.plusSeconds(2), Duration.ofMinutes(1)).orElseThrow();
        assertThat(claimed.object()).isEqualTo(object);
        assertThat(claimed.lease()).get()
                .satisfies(lease -> assertThat(lease.owner()).isEqualTo("scheduler-1"));
        assertThat(repository.findSchedule(object)).get()
                .satisfies(found -> assertThat(found.version()).isEqualTo(1));
    }

    @Test
    void renamesEveryDurableSurfaceAndKeepsHistoricalReceipts() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("mat_repo_rename");
        MaterializationRepository repository = new JdbcMaterializationRepository(dataSource);

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String tenant = uniqueTenant();
        TenantId tenantId = new TenantId(tenant);
        MaterializationId upstream = new MaterializationId(tenantId,
                QualifiedName.parse("analytics.upstream_dt"));
        MaterializationId downstream = new MaterializationId(tenantId,
                QualifiedName.parse("analytics.downstream_mv"));
        MaterializationId renamed = new MaterializationId(tenantId,
                QualifiedName.parse("analytics.upstream_dt_v2"));
        Principal requester = new Principal("user:alice");
        Principal definer = new Principal("role:analytics");
        IncarnationId incarnation = IncarnationId.random();
        SourceRef source = new SourceRef("iceberg", "lake", "raw", "orders");

        CommandId creationCommand = CommandId.random();
        repository.createObject(MaterializationObject.creating(upstream,
                        MaterializationKind.DYNAMIC_TABLE, incarnation, definer,
                        SecurityMode.DEFINER, RefreshMode.FULL, Optional.empty(), now),
                definition(upstream, source, definer, now),
                Schedule.manual(upstream), creationCommand,
                event(MaterializationEventType.OBJECT_CREATED, upstream, requester, definer, now));
        DefinitionRevision downstreamDefinition = new DefinitionRevision(downstream, 1,
                "SELECT * FROM " + upstream.name(),
                new Binding("SELECT * FROM " + upstream.name(), "e".repeat(64)),
                new OutputSchema(List.of(
                        new ColumnDefinition("order_id", "BIGINT", false, 0))),
                Set.of(source),
                Set.of(new Dependency(upstream, downstream, 1,
                        org.pangrova.materialization.domain.DependencyKind.DIRECT)),
                new PolicySnapshot(Map.of(source, "f".repeat(64))),
                SecurityMode.DEFINER, definer, Map.of(), now);
        repository.createObject(MaterializationObject.creating(downstream,
                        MaterializationKind.MATERIALIZED_VIEW, IncarnationId.random(), definer,
                        SecurityMode.DEFINER, RefreshMode.FULL, Optional.empty(), now),
                downstreamDefinition, Schedule.manual(downstream), CommandId.random(),
                event(MaterializationEventType.OBJECT_CREATED, downstream, requester, definer,
                        now));

        MaterializationObject header = repository.findObject(upstream).orElseThrow();
        MaterializationObject target = header.renamed(renamed, header.revision(),
                now.plusSeconds(1));
        CommandId renameCommand = CommandId.random();
        repository.renameObject(target, upstream, header.revision(), renameCommand,
                event(MaterializationEventType.OBJECT_ALTERED, renamed, requester, definer,
                        now.plusSeconds(1)));

        assertThat(repository.findObject(upstream)).isEmpty();
        assertThat(repository.findObject(renamed)).get()
                .satisfies(found -> {
                    assertThat(found.revision()).isEqualTo(1);
                    assertThat(found.incarnation()).isEqualTo(incarnation);
                    assertThat(found.definitionRevision()).isEqualTo(1);
                });
        assertThat(repository.findDefinition(renamed, 1)).isPresent();
        assertThat(repository.findSchedule(upstream)).isEmpty();
        assertThat(repository.findSchedule(renamed)).isPresent();
        assertThat(repository.findLiveDependents(renamed)).containsExactly(downstream);
        assertThat(repository.findCommand(tenantId, creationCommand)).get()
                .satisfies(receipt -> assertThat(receipt.resultReference())
                        .contains(upstream.name().toString()));
        assertThat(repository.findCommand(tenantId, renameCommand)).get()
                .satisfies(receipt -> assertThat(receipt.resultReference())
                        .contains(renamed.name().toString()));

        assertThatThrownBy(() -> repository.renameObject(header.renamed(renamed,
                        header.revision(), now.plusSeconds(2)), upstream, header.revision(),
                CommandId.random(), event(MaterializationEventType.OBJECT_ALTERED, renamed,
                        requester, definer, now.plusSeconds(2))))
                .isInstanceOf(MaterializationException.class);
    }

    @Test
    void rejectsExpiredFenceAndConflictingEvidence() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("mat_repo_guard");
        MaterializationRepository repository = new JdbcMaterializationRepository(dataSource);

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String tenant = uniqueTenant();
        MaterializationId object = objectId(tenant);
        Principal requester = new Principal("user:bob");
        Principal definer = new Principal("role:analytics");
        IncarnationId incarnation = IncarnationId.random();
        SourceRef source = new SourceRef("paimon", "lake", "raw", "events");
        SourceSnapshotRef snapshot = new SourceSnapshotRef(
                "paimon", "snap-9", "b".repeat(64), "c".repeat(64), now, true);
        SourceVector vector = new SourceVector(Map.of(source, snapshot), 1);

        MaterializationObject header = MaterializationObject.creating(object,
                MaterializationKind.MATERIALIZED_VIEW, incarnation, definer, SecurityMode.DEFINER,
                RefreshMode.FULL, Optional.empty(), now);
        repository.createObject(header, definition(object, source, definer, now),
                Schedule.manual(object), CommandId.random(),
                event(MaterializationEventType.OBJECT_CREATED, object, requester, definer, now));

        Authority authority = seedCoordinatorAuthority(dataSource);
        ExecutionFence fence = new ExecutionFence(
                authority.anchor(), CONTROL_EPOCH, now.plus(Duration.ofMinutes(10)));
        PhysicalTableRef candidateTable = candidateTable();
        AttemptId attemptId = AttemptId.random();
        GenerationId generationId = GenerationId.random();
        RefreshAttempt attempt = RefreshAttempt.reserve(attemptId, header,
                definition(object, source, definer, now), requester,
                new RefreshAttempt.AttemptScope(Optional.empty(), RefreshKind.Full.INSTANCE,
                        vector, TriggerKind.MANUAL, fence, candidateTable), now);
        Generation candidate = Generation.candidate(generationId, object, incarnation, 1,
                attemptId, Optional.empty(), candidateTable, RefreshKind.Full.INSTANCE, vector, now);
        MaterializationWriteCapability capability = new MaterializationWriteCapability(object,
                incarnation, 1, attemptId, candidateTable, generationId, vector.canonicalHash(),
                definer, CONTROL_EPOCH, now, now.plus(Duration.ofMinutes(10)));
        repository.reserveAttempt(attempt, candidate,
                List.of(SourcePin.acquired(attemptId, source, snapshot,
                        now.plus(Duration.ofMinutes(30)))),
                capability, 0,
                event(MaterializationEventType.ATTEMPT_RESERVED, object, requester, definer, now));

        CommitEvidence evidence = new CommitEvidence("paimon", "e".repeat(64), MANIFEST_HASH,
                "paimon-v1", now);
        seedCommittedJournal(dataSource, authority, tenant, requester, candidateTable,
                evidence, now);
        repository.recordCommitEvidence(attemptId, evidence, now);

        assertThat(repository.findReconcilableAttempts(now.plusSeconds(1)))
                .extracting(RefreshAttempt::id)
                .doesNotContain(attemptId);

        CommitEvidence conflicting = new CommitEvidence("paimon", "f".repeat(64), MANIFEST_HASH,
                "paimon-v1", now);
        assertThatThrownBy(() -> repository.recordCommitEvidence(attemptId, conflicting, now))
                .isInstanceOfSatisfying(MaterializationException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_EVIDENCE_MISMATCH));

        expireAnchor(dataSource, authority.anchor());
        assertThat(repository.findReconcilableAttempts(now.plusSeconds(1)))
                .extracting(RefreshAttempt::id)
                .contains(attemptId);
        PublishCommand command = new PublishCommand(object, 0, candidate, attempt, evidence,
                fence, CommandId.random());
        assertThatThrownBy(() -> repository.publish(command, event(
                MaterializationEventType.GENERATION_PUBLISHED, object, requester, definer,
                now.plusSeconds(1))))
                .isInstanceOfSatisfying(MaterializationException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_FENCE_REJECTED));

        MaterializationObject head = repository.findObject(object).orElseThrow();
        assertThat(head.revision()).isZero();
        assertThat(head.state()).isEqualTo(ObjectState.Creating.INSTANCE);
        assertThat(repository.findGeneration(generationId)).get()
                .satisfies(found -> assertThat(found.state())
                        .isEqualTo(GenerationState.Committed.INSTANCE));
    }

    @Test
    void projectsSystemSurfacesAndTriggersDependentsOnPublication() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("mat_repo_projection");
        MaterializationRepository repository = new JdbcMaterializationRepository(dataSource);

        Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);
        String tenant = uniqueTenant();
        TenantId tenantId = new TenantId(tenant);
        MaterializationId upstream = new MaterializationId(tenantId,
                QualifiedName.parse("analytics.upstream_dt"));
        MaterializationId downstream = new MaterializationId(tenantId,
                QualifiedName.parse("analytics.downstream_mv"));
        Principal requester = new Principal("user:alice");
        Principal definer = new Principal("role:analytics");
        IncarnationId incarnation = IncarnationId.random();
        SourceRef source = new SourceRef("iceberg", "lake", "raw", "orders");
        SourceSnapshotRef snapshot = new SourceSnapshotRef(
                "iceberg", "snap-1", "b".repeat(64), "c".repeat(64), now, true);
        SourceVector vector = new SourceVector(Map.of(source, snapshot), 1);

        MaterializationObject upstreamHeader = MaterializationObject.creating(upstream,
                MaterializationKind.DYNAMIC_TABLE, incarnation, definer,
                SecurityMode.DEFINER, RefreshMode.FULL, Optional.empty(), now);
        repository.createObject(upstreamHeader,
                definition(upstream, source, definer, now),
                Schedule.scheduled(upstream, new TargetLag(Duration.ofMinutes(5)),
                        now.minusSeconds(1)),
                CommandId.random(),
                event(MaterializationEventType.OBJECT_CREATED, upstream, requester, definer, now));
        DefinitionRevision downstreamDefinition = new DefinitionRevision(downstream, 1,
                "SELECT * FROM " + upstream.name(),
                new Binding("SELECT * FROM " + upstream.name(), "d".repeat(64)),
                new OutputSchema(List.of(
                        new ColumnDefinition("order_id", "BIGINT", false, 0))),
                Set.of(source),
                Set.of(new Dependency(upstream, downstream, 1,
                        org.pangrova.materialization.domain.DependencyKind.DIRECT)),
                new PolicySnapshot(Map.of(source, "c".repeat(64))),
                SecurityMode.DEFINER, definer, Map.of(), now);
        Instant downstreamDue = now.plus(Duration.ofHours(1));
        repository.createObject(MaterializationObject.creating(downstream,
                        MaterializationKind.MATERIALIZED_VIEW, IncarnationId.random(), definer,
                        SecurityMode.DEFINER, RefreshMode.FULL, Optional.empty(), now),
                downstreamDefinition,
                Schedule.scheduled(downstream, new TargetLag(Duration.ofMinutes(1)),
                        downstreamDue),
                CommandId.random(),
                event(MaterializationEventType.OBJECT_CREATED, downstream, requester, definer,
                        now));

        assertThat(repository.listObjects(tenantId))
                .extracting(object -> object.id().name().toString())
                .containsExactly("analytics.downstream_mv", "analytics.upstream_dt");
        assertThat(repository.findDependencies(tenantId, Optional.of(downstream)))
                .singleElement()
                .satisfies(edge -> {
                    assertThat(edge.upstream()).isEqualTo(upstream);
                    assertThat(edge.downstream()).isEqualTo(downstream);
                    assertThat(edge.downstreamDefinitionRevision()).isEqualTo(1);
                });

        Authority authority = seedCoordinatorAuthority(dataSource);
        ExecutionFence fence = new ExecutionFence(
                authority.anchor(), CONTROL_EPOCH, now.plus(Duration.ofMinutes(10)));
        PhysicalTableRef candidateTable = candidateTable();
        AttemptId attemptId = AttemptId.random();
        GenerationId generationId = GenerationId.random();
        RefreshAttempt attempt = RefreshAttempt.reserve(attemptId, upstreamHeader,
                definition(upstream, source, definer, now), requester,
                new RefreshAttempt.AttemptScope(Optional.empty(), RefreshKind.Full.INSTANCE,
                        vector, TriggerKind.MANUAL, fence, candidateTable), now);
        Generation candidate = Generation.candidate(generationId, upstream, incarnation, 1,
                attemptId, Optional.empty(), candidateTable, RefreshKind.Full.INSTANCE, vector,
                now);
        MaterializationWriteCapability capability = new MaterializationWriteCapability(upstream,
                incarnation, 1, attemptId, candidateTable, generationId, vector.canonicalHash(),
                definer, CONTROL_EPOCH, now, now.plus(Duration.ofMinutes(10)));
        repository.reserveAttempt(attempt, candidate,
                List.of(SourcePin.acquired(attemptId, source, snapshot,
                        now.plus(Duration.ofMinutes(30)))),
                capability, 0,
                event(MaterializationEventType.ATTEMPT_RESERVED, upstream, requester, definer,
                        now));
        CommitEvidence evidence = new CommitEvidence("iceberg", "a".repeat(64), MANIFEST_HASH,
                "iceberg-v3", now);
        seedCommittedJournal(dataSource, authority, tenant, requester, candidateTable, evidence,
                now);
        repository.recordCommitEvidence(attemptId, evidence, now);
        Instant publishedAt = now.plusSeconds(1);
        repository.publish(new PublishCommand(upstream, 0, candidate, attempt, evidence, fence,
                        CommandId.random()),
                event(MaterializationEventType.GENERATION_PUBLISHED, upstream, requester, definer,
                        publishedAt));

        assertThat(repository.findAttempts(tenantId, Optional.of(upstream), 10))
                .extracting(RefreshAttempt::id)
                .containsExactly(attemptId);
        assertThat(repository.findAttempts(tenantId, Optional.of(downstream), 10)).isEmpty();
        assertThat(repository.findSchedule(downstream)).get()
                .satisfies(schedule -> {
                    assertThat(schedule.nextDueAt()).contains(publishedAt);
                    assertThat(schedule.version()).isEqualTo(1);
                });

        repository.recordGcDecision(GcLedgerEntry.pending(
                generationId, "retired generation", publishedAt));
        assertThat(repository.findGcHistory(tenantId, Optional.of(upstream), 10))
                .singleElement()
                .satisfies(entry -> {
                    assertThat(entry.object()).isEqualTo(upstream);
                    assertThat(entry.generation()).isEqualTo(generationId);
                    assertThat(entry.state()).isEqualTo(GcLedgerEntry.GcState.PENDING);
                });
    }

    private static DefinitionRevision definition(MaterializationId object, SourceRef source,
            Principal definer, Instant now) {
        return new DefinitionRevision(object, 1,
                "SELECT order_id, total FROM " + source.catalog() + "." + source.schema() + "."
                        + source.table(),
                new Binding(
                        "SELECT order_id, total FROM " + source.catalog() + "." + source.schema()
                                + "." + source.table(),
                        "a".repeat(64)),
                new OutputSchema(List.of(
                        new ColumnDefinition("order_id", "BIGINT", false, 0),
                        new ColumnDefinition("total", "DECIMAL(18,2)", true, 1))),
                Set.of(source), Set.<Dependency>of(),
                new PolicySnapshot(Map.of(source, "c".repeat(64))),
                SecurityMode.DEFINER, definer, Map.of(), now);
    }

    private static MaterializationEvent event(MaterializationEventType type,
            MaterializationId object, Principal requester, Principal effective, Instant now) {
        return new MaterializationEvent(UUID.randomUUID().toString(), type, object,
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                requester, effective, "", now);
    }

    private static MaterializationId objectId(String tenant) {
        return new MaterializationId(new TenantId(tenant),
                QualifiedName.parse("analytics.daily_orders"));
    }

    private static String uniqueTenant() {
        return "t_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static PhysicalTableRef candidateTable() {
        return new PhysicalTableRef("lake", "__pangrova_materializations",
                "__pg_mat_" + UUID.randomUUID().toString().replace("-", ""));
    }

    private static Authority seedCoordinatorAuthority(DataSource dataSource) throws SQLException {
        String anchor = "anchor-" + UUID.randomUUID();
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO distributed_lease (lease_key, holder_id, epoch, acquired_at, "
                            + "expires_at, version) VALUES ('coordinator_leader', 'coordinator-1', "
                            + "?, clock_timestamp(), clock_timestamp() + interval '1 hour', 1) "
                            + "ON CONFLICT (lease_key) DO UPDATE SET epoch=EXCLUDED.epoch, "
                            + "holder_id=EXCLUDED.holder_id, "
                            + "expires_at=EXCLUDED.expires_at")) {
                statement.setLong(1, CONTROL_EPOCH);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO execution_anchor (execution_id, owner_id, owner_incarnation, "
                            + "fencing_token, acquired_at, expires_at, version) VALUES "
                            + "(?, 'coordinator-1', 'incarnation-1', 11, clock_timestamp(), "
                            + "clock_timestamp() + interval '1 hour', 1)")) {
                statement.setString(1, anchor);
                statement.executeUpdate();
            }
        }
        return new Authority(anchor);
    }

    private static void seedCommittedJournal(DataSource dataSource, Authority authority,
            String tenant, Principal principal, PhysicalTableRef candidate,
            CommitEvidence evidence, Instant committedAt) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO write_commit_journal (query_id, catalog_name, table_name, state, "
                             + "timestamp_ms, idempotency_key, owner_id, owner_incarnation, "
                             + "execution_fencing_token, tenant_id, base_snapshot_id, "
                             + "write_output_binding, commit_fingerprint, published_version, "
                             + "manifest_sha256, principal, deferred_authorization_scope, "
                             + "write_mode) VALUES (?, ?, ?, 'COMMITTED', ?, ?, 'coordinator-1', "
                             + "'incarnation-1', 11, ?, -1, ?, ?, 1, ?, ?, ?, 'INSERT')")) {
            statement.setString(1, authority.anchor());
            statement.setString(2, candidate.catalog());
            statement.setString(3, candidate.schema() + "." + candidate.table());
            statement.setLong(4, committedAt.toEpochMilli());
            statement.setString(5, authority.anchor());
            statement.setString(6, tenant);
            statement.setString(7, "binding:" + authority.anchor());
            statement.setString(8, evidence.commitFingerprint());
            statement.setBytes(9, MANIFEST);
            statement.setString(10, principal.name());
            statement.setBytes(11, new byte[] {1});
            statement.executeUpdate();
        }
    }

    private static void purgeJournal(DataSource dataSource, String queryId) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM write_commit_journal WHERE query_id=?")) {
            statement.setString(1, queryId);
            statement.executeUpdate();
        }
    }

    private static void expireAnchor(DataSource dataSource, String anchor) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE execution_anchor SET expires_at=clock_timestamp() - interval '1 minute' "
                             + "WHERE execution_id=?")) {
            statement.setString(1, anchor);
            statement.executeUpdate();
        }
    }

    private static byte[] manifestBytes() {
        byte[] bytes = new byte[32];
        java.util.Arrays.fill(bytes, (byte) 1);
        return bytes;
    }

    private record Authority(String anchor) {}
}
