package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.types.enums.VectorElementType;
import org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationSubmissionAuthority;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.job.SnapshotPinReservation;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.planner.generation.GenerationPlan;
import org.pangrova.planner.generation.Bm25GenerationPlanBuilder;
import org.pangrova.planner.generation.VectorGenerationPlanBuilder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.BooleanSupplier;

/** Maintenance compiler for snapshot-pinned Native index work; persists canonical control metadata, never row bytes. */
public final class DurableIndexBuildService {

    private static final Logger LOG = LoggerFactory.getLogger(DurableIndexBuildService.class);
    private static final String LEGACY_SUBMISSION_ATTEMPT = "submission";
    private static final int MAX_REDUCTION_FAN_IN = 64;
    // Leave headroom for the Worker system pool and concurrent service leases.
    // The grant is a task limit, not the Worker's total process memory budget.
    private static final long DEFAULT_MEMORY_BYTES = 1L * 1024 * 1024 * 1024;
    private static final long DEFAULT_SPILL_BYTES = 2L * 1024 * 1024 * 1024;
    private static final int TASK_TIMEOUT_MINUTES = 30;
    private static final long DEFAULT_TASK_TIMEOUT_MILLIS = Duration.ofMinutes(TASK_TIMEOUT_MINUTES).toMillis();
    private static final int MAX_SEARCH_LIST = 100_000;
    private static final double DEFAULT_DISKANN_ALPHA = 1.2;
    private static final int DEFAULT_HNSW_EF_CONSTRUCTION = 200;
    private static final int MAX_COARSE_CLUSTERS = 4_096;
    private static final int DEFAULT_TRAINING_ITERATIONS = 20;
    private static final int DEFAULT_TRAINING_SAMPLES = 100_000;
    private static final int MAX_TRAINING_SAMPLES = 1_000_000;
    private static final int MAX_VECTOR_DIMENSION = 65_535;

    private final GenerationJobStore store;
    private final IndexMetadataStore indexMetadataStore;
    private final CatalogRegistry catalogRegistry;
    private final SnapshotGenerationSplitPlanner splitPlanner;
    private final String baseArtifactUri;
    private final Function<ConnectorSession, SnapshotPinProvider> pinProviders;
    private final ArtifactSnapshotPinValidator snapshotValidator;

    /** Creates the sole durable Native index compiler over catalog and metadata authority. */
    public DurableIndexBuildService(
            GenerationJobStore store, IndexMetadataStore indexMetadataStore,
            CatalogRegistry catalogRegistry, String baseArtifactUri,
            Function<ConnectorSession, SnapshotPinProvider> pinProviders) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.indexMetadataStore = Objects.requireNonNull(indexMetadataStore, "indexMetadataStore must not be null");
        this.catalogRegistry = Objects.requireNonNull(
                catalogRegistry, "catalogRegistry must not be null");
        this.splitPlanner = new SnapshotGenerationSplitPlanner(catalogRegistry);
        if (baseArtifactUri == null || baseArtifactUri.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be blank");
        }
        this.baseArtifactUri = stripTrailingSlash(baseArtifactUri);
        this.pinProviders = Objects.requireNonNull(pinProviders, "pinProviders");
        this.snapshotValidator = new ArtifactSnapshotPinValidator(store, pinProviders);
    }

    /** Returns whether this descriptor belongs to the Native artifact build plane. */
    public static boolean supports(IndexDescriptor descriptor) {
        String algorithm = normalizedAlgorithm(descriptor);
        return (descriptor.indexType() == IndexType.VECTOR
                && (algorithm.equals("DISK_ANN") || algorithm.equals("HNSW")
                || algorithm.equals("IVF_PQ")))
                || (descriptor.indexType() == IndexType.INVERTED && algorithm.equals("BM25"));
    }

    /** Submits only the captured Native admission; stale callers cannot adopt a later reservation. */
    public String submit(IndexDescriptor descriptor, QualifiedTableName table, QuerySession context,
                         String expectedAttemptId) {
        Objects.requireNonNull(expectedAttemptId, "captured Native submission attempt");
        if (!supports(descriptor)) {
            throw new IllegalArgumentException("descriptor is not a supported Native index");
        }
        String tenantId = requireTenantId(descriptor.tenantId());
        if (!tenantId.equals(context.tenantId())
                || !table.catalog().equals(descriptor.catalogName())
                || !table.schema().equals(descriptor.schemaName())
                || !table.table().equals(descriptor.tableName())) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Native index build scope does not match its durable descriptor");
        }
        ConnectorSession session = SessionContextConverter.toConnectorSession(
                context, table.catalog(), table.schema());
        return submitScoped(descriptor, table, tenantId, session, expectedAttemptId);
    }

    /** Reauthorizes and submits the exact captured replacement reservation. */
    public String submitReplacement(IndexDescriptor previous, IndexDescriptor replacement, String expectedAttemptId) {
        Objects.requireNonNull(expectedAttemptId, "captured Native submission attempt");
        var resolved = DeferredIndexAuthorityResolver.resolve(store, indexMetadataStore, previous, replacement);
        return submitScoped(replacement, resolved.table(), resolved.tenantId(), resolved.session(), expectedAttemptId);
    }

    /** Reauthorizes the durable Native submitter for connector metadata maintenance. */
    public ConnectorSession authorizedSession(IndexDescriptor descriptor) {
        return DeferredIndexAuthorityResolver.resolveExisting(store, indexMetadataStore, descriptor).session();
    }

    private String submitScoped(IndexDescriptor descriptor, QualifiedTableName table,
                                String tenantId, ConnectorSession session, String expectedAttemptId) {
        AtomicReference<String> jobId = new AtomicReference<>();
        indexMetadataStore.withIndexLifecycleLock(descriptor.indexId(), () -> {
            IndexDescriptor current = indexMetadataStore.findIndex(descriptor.indexId()).orElseThrow(() ->
                    new IllegalStateException("Native index descriptor disappeared before submission"));
            requireBuildableReservation(current, descriptor);
            jobId.set(submitScopedLocked(descriptor, table, tenantId, session, expectedAttemptId));
        });
        return jobId.get();
    }

    private void requireBuildableReservation(IndexDescriptor current, IndexDescriptor requested) {
        if (!current.tenantId().equals(requested.tenantId())
                || !current.catalogName().equals(requested.catalogName())
                || !current.schemaName().equals(requested.schemaName())
                || !current.tableName().equals(requested.tableName())
                || !current.columnNames().equals(requested.columnNames())
                || !current.properties().equals(requested.properties())
                || current.indexType() != requested.indexType()
                || !indexMetadataStore.isIndexGenerationSnapshotBuildable(
                        requested.indexId(), requested.generation(),
                        requested.buildSnapshotId(), requested.updatedAt())) {
            throw new IllegalStateException("Native index descriptor is no longer buildable");
        }
    }

    private String submitScopedLocked(IndexDescriptor descriptor, QualifiedTableName table,
                                      String tenantId, ConnectorSession session, String expectedAttemptId) {
        String attempt = indexMetadataStore.findIndexGenerationSubmissionAttempt(
                descriptor.indexId(), descriptor.generation(), descriptor.buildSnapshotId())
                .orElseThrow(() -> new IllegalStateException("Native index submission identity is absent"));
        if (!expectedAttemptId.equals(attempt)) {
            throw new IllegalStateException("Native index submission attempt is no longer current");
        }
        String jobId = jobId(tenantId, descriptor, attempt);
        NativeBuildSpec build = prepareBuild(descriptor, table, session);
        String sourceId = String.join(".", table.catalog(), table.schema(), table.table());
        var authority = new GenerationSubmissionAuthority(tenantId, session.getUser(),
                DeferredAuthorizationScope.raw(table, Set.copyOf(descriptor.columnNames()), table));
        SnapshotPinIntent intent = store.reserveSnapshotPin(new SnapshotPinReservation(
                tenantId, descriptor.indexId(), descriptor.generation(), attempt,
                sourceId, descriptor.buildSnapshotId(), authority, Instant.now()));
        if (intent.jobId() != null) {
            // An ambiguous prior submission already committed the exact job. Do not re-enumerate
            // or allocate a second artifact namespace for the same durable reservation.
            GenerationJob existing = store.findJob(tenantId, intent.jobId()).orElseThrow(() ->
                    new IllegalStateException("Bound Native submission job is absent"));
            Instant reservationUpdatedAt = indexMetadataStore.findIndexGenerationSnapshotReservation(
                    descriptor.indexId(), descriptor.generation(), descriptor.buildSnapshotId())
                    .orElseThrow(() -> new IllegalStateException("Native index reservation is absent"));
            if (!existing.jobId().equals(jobId)
                    || !existing.publicationExpectedUpdatedAt().equals(reservationUpdatedAt)
                    || !existing.artifactId().equals(descriptor.indexId())
                    || !existing.stagingGeneration().equals(Integer.toString(descriptor.generation()))
                    || !existing.sourceSnapshot().equals(intent.sourceSnapshot())
                    || existing.state() == GenerationJobState.FAILED
                    || existing.state() == GenerationJobState.CANCELLED) {
                throw new IllegalStateException("Native submission is terminal or differs from its durable identity");
            }
            return existing.jobId();
        }
        SnapshotPinOwner owner = new SnapshotPinOwner(
                intent.indexId(), intent.generation(), intent.attemptId());
        boolean emptySnapshot = SnapshotGenerationSplitPlanner.EMPTY_SNAPSHOT.equals(intent.sourceSnapshot());
        if (!emptySnapshot) {
            SnapshotPinProvider pinProvider = requirePinProvider(session);
            pinProvider.pinSnapshot(table.catalog(), table.schema(), table.table(),
                    intent.sourceSnapshot(), owner);
        }
        if (!emptySnapshot) {
            requirePinProvider(session).validateSnapshotPin(table.catalog(), table.schema(), table.table(),
                    intent.sourceSnapshot(), owner);
        }
        if (!store.markSnapshotPinPinned(tenantId, intent.pinOwner(), Instant.now())) {
            throw new IllegalStateException("Snapshot pin could not transition to PINNED");
        }
        List<GenerationSplit> splits = splitPlanner.enumerate(
                table, intent.sourceSnapshot(), session);
        GenerationPlan plan = build.plan();
        String generation = Integer.toString(descriptor.generation());
        String stagingUri = stagingUri(tenantId, descriptor, attempt);
        String idempotencyKey = jobId + ":snapshot:" + (LEGACY_SUBMISSION_ATTEMPT.equals(attempt)
                ? descriptor.buildSnapshotId() : org.pangrova.common.util.DigestUtils.sha256Hex(
                        descriptor.buildSnapshotId()));
        Map<String, String> properties = descriptor.properties();
        int maxAttempts = boundedInt(properties, "max_attempts", 5, 1, 100);
        List<GenerationPage> pages = buildPages(
                jobId, splits, build.artifactType(), maxAttempts);
        Instant now = Instant.now();
        var grant = new GenerationDispatchSpec.ResourceGrant(
                        boundedLong(properties, "max_memory_bytes", DEFAULT_MEMORY_BYTES,
                                64L * 1024 * 1024, 64L * 1024 * 1024 * 1024),
                        boundedInt(properties, "max_threads", 1, 1, 16),
                        boundedLong(properties, "max_spill_bytes", DEFAULT_SPILL_BYTES,
                                64L * 1024 * 1024, 256L * 1024 * 1024 * 1024),
                        boundedLong(properties, "task_timeout_millis", DEFAULT_TASK_TIMEOUT_MILLIS,
                                1_000, Duration.ofDays(1).toMillis()));
        var submission = DurableIndexBuildSubmission.create(
                new DurableIndexBuildSubmission.Definition(
                        tenantId, session.getUser(), jobId, descriptor, table, plan,
                        build.artifactType(), build.properties(), pages, generation,
                        stagingUri, idempotencyKey, grant, now));
        store.createSnapshotPinnedJob(
                submission.job(), submission.dispatch(), pages, intent.pinOwner());
        return jobId;
    }

    static List<GenerationPage> buildPages(
            String jobId, List<GenerationSplit> splits,
            String artifactType, int maxAttempts) {
        List<GenerationPage> pages = new ArrayList<>();
        List<String> level = new ArrayList<>(splits.size());
        for (int index = 0; index < splits.size(); index++) {
            String pageId = jobId + ":map:" + index;
            pages.add(new GenerationPage(
                    jobId, pageId, 0, index, pages.size(),
                    GenerationPageKind.SOURCE_MAP,
                    List.of(splits.get(index)), List.of(), maxAttempts));
            level.add(pageId);
        }
        if (!artifactType.equals(
                org.pangrova.generation.api.constants.GenerationOutputTypeIds.TEXT_INDEX)
                || level.isEmpty()) {
            return List.copyOf(pages);
        }
        int stage = 1;
        while (level.size() > MAX_REDUCTION_FAN_IN) {
            List<String> next = new ArrayList<>();
            for (int offset = 0; offset < level.size(); offset += MAX_REDUCTION_FAN_IN) {
                int partition = next.size();
                String pageId = jobId + ":reduce:" + stage + ":" + partition;
                pages.add(new GenerationPage(
                        jobId, pageId, stage, partition, pages.size(),
                        GenerationPageKind.DEPENDENCY_REDUCE, List.of(),
                        level.subList(offset, Math.min(
                                level.size(), offset + MAX_REDUCTION_FAN_IN)),
                        maxAttempts));
                next.add(pageId);
            }
            level = next;
            stage++;
        }
        pages.add(new GenerationPage(
                jobId, jobId + ":finalize", stage, 0, pages.size(),
                GenerationPageKind.GENERATION_FINALIZE,
                List.of(), level, maxAttempts));
        return List.copyOf(pages);
    }

    private NativeBuildSpec prepareBuild(
            IndexDescriptor descriptor, QualifiedTableName table, ConnectorSession session) {
        String column = requireSingleColumn(descriptor);
        List<String> tableName = List.of(table.catalog(), table.schema(), table.table());
        if (descriptor.indexType() == IndexType.INVERTED) {
            requireTextColumn(table, column, session);
            String analyzer = descriptor.properties().getOrDefault(
                    "analyzer", "standard-v1").trim().toLowerCase(Locale.ROOT);
            if (!analyzer.equals("standard-v1")) {
                throw validation("Native BM25 requires analyzer=standard-v1");
            }
            return new NativeBuildSpec(
                    Bm25GenerationPlanBuilder.build(tableName, column),
                    org.pangrova.generation.api.constants.GenerationOutputTypeIds.TEXT_INDEX,
                    List.of(stringProperty("analyzer", analyzer)));
        }
        String algorithm = normalizedAlgorithm(descriptor);
        CapabilityRef buildCapability =
                BuiltInCapabilityCatalog.vectorArtifactBuildPlanRef(algorithm);
        String metric = requireMetric(descriptor.properties(), algorithm);
        VectorShape vector = resolveVectorShape(table, column, session);
        if (vector.elementType() != VectorElementType.FLOAT) {
            throw validation("Native ANN build requires VECTOR(..., FLOAT)");
        }
        validateDeclaredDimension(descriptor.properties(), vector.dimension());
        return new NativeBuildSpec(
                VectorGenerationPlanBuilder.build(
                        tableName, column, buildCapability,
                        BuiltInCapabilityCatalog.vectorArtifactFormat(algorithm)),
                org.pangrova.generation.api.constants.GenerationOutputTypeIds.VECTOR_INDEX,
                vectorProperties(
                        buildCapability, descriptor.properties(), vector.dimension(), metric));
    }

    public int cancel(String artifactId, String tenantId, Instant now) {
        return store.cancelArtifactBuilds(requireTenantId(tenantId), artifactId, now);
    }

    /** Revalidates the durable and connector pin before publication can expose a generation. */
    public void validateSnapshotPin(GenerationJob job) {
        snapshotValidator.validate(job);
    }

    /** Completes a durable cleanup claim; uncertain connector outcomes remain RELEASING for retry. */
    private void finishSnapshotPinRelease(SnapshotPinIntent intent) {
        if (!SnapshotGenerationSplitPlanner.EMPTY_SNAPSHOT.equals(intent.sourceSnapshot())) {
            var resolved = DeferredIndexAuthorityResolver.resolveSubmission(intent.authority());
            requirePinProvider(resolved.session()).unpinSnapshot(
                    resolved.table().catalog(), resolved.table().schema(), resolved.table().table(),
                    intent.sourceSnapshot(), owner(intent));
        }
        if (!store.markSnapshotPinReleased(intent.tenantId(), intent.pinOwner(), Instant.now())) {
            throw new IllegalStateException("Released snapshot pin is absent from the durable ledger");
        }
        LOG.info("event=index_snapshot_pin_released indexId={} generation={} attemptId={} snapshotId={}",
                intent.indexId(), intent.generation(), intent.attemptId(), intent.sourceSnapshot());
    }

    /** Acquires real connector pins for published jobs adopted by the V121 ledger backfill. */
    public int adoptLegacySnapshotPins() {
        return adoptLegacySnapshotPins(() -> true);
    }

    /** Stops admitting new maintenance work when the current leadership epoch ends. */
    public int adoptLegacySnapshotPins(BooleanSupplier stillLeader) {
        Objects.requireNonNull(stillLeader, "stillLeader");
        int adopted = 0;
        for (SnapshotPinIntent candidate : store.listSnapshotPinsForAdoption(100)) {
            if (!stillLeader.getAsBoolean()) {
                break;
            }
            try {
                boolean[] completed = {false};
                indexMetadataStore.withIndexLifecycleLock(candidate.indexId(), () -> {
                    if (stillLeader.getAsBoolean()) {
                        completed[0] = adoptLegacyPin(candidate);
                    }
                });
                if (completed[0]) {
                    adopted++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("event=index_snapshot_pin_adoption_retry indexId={} generation={} attemptId={} error={}",
                        candidate.indexId(), candidate.generation(), candidate.attemptId(), failure.getMessage());
            }
        }
        return adopted;
    }

    private boolean adoptLegacyPin(SnapshotPinIntent candidate) {
        // The list is only a hint. The lifecycle lock serializes this connector acquisition
        // with submission and cleanup; the ledger state is checked again under that lock.
        var current = store.findSnapshotPin(candidate.tenantId(), candidate.pinOwner());
        if (current.isEmpty()) {
            return false;
        }
        SnapshotPinIntent intent = current.orElseThrow();
        if (intent.state() != SnapshotPinState.INTENDED || !intent.attemptId().startsWith("legacy-")
                || intent.jobId() == null || store.findJob(intent.tenantId(), intent.jobId())
                        .map(job -> job.state() != GenerationJobState.PUBLISHED).orElse(true)) {
            return false;
        }
        if (!SnapshotGenerationSplitPlanner.EMPTY_SNAPSHOT.equals(intent.sourceSnapshot())) {
            var resolved = DeferredIndexAuthorityResolver.resolveSubmission(intent.authority());
            SnapshotPinProvider provider = requirePinProvider(resolved.session());
            provider.pinSnapshot(resolved.table().catalog(), resolved.table().schema(), resolved.table().table(),
                    intent.sourceSnapshot(), owner(intent));
            provider.validateSnapshotPin(resolved.table().catalog(), resolved.table().schema(),
                    resolved.table().table(), intent.sourceSnapshot(), owner(intent));
        }
        if (!store.markSnapshotPinPinned(intent.tenantId(), intent.pinOwner(), Instant.now())) {
            throw new IllegalStateException("Legacy snapshot pin could not transition to PINNED");
        }
        return true;
    }

    /** Fences reservations interrupted before an original subject was durably recorded; no replay or IO. */
    public int fenceAbandonedNativeReservations(BooleanSupplier stillLeader) {
        Objects.requireNonNull(stillLeader, "stillLeader");
        Instant cutoff = Instant.now().minus(Duration.ofHours(1));
        int fenced = 0;
        for (var candidate : indexMetadataStore.listAbandonedNativeReservations(100, cutoff)) {
            if (!stillLeader.getAsBoolean()) {
                break;
            }
            try {
                boolean[] completed = {false};
                indexMetadataStore.withIndexLifecycleLock(candidate.descriptor().indexId(), () -> {
                    if (!stillLeader.getAsBoolean()) {
                        return;
                    }
                    var current = indexMetadataStore.findAbandonedNativeReservation(
                            candidate.descriptor().indexId(), candidate.generation(), cutoff);
                    if (current.isPresent()) {
                        var reservation = current.orElseThrow();
                        if (reservation.updatedAt().equals(candidate.updatedAt())
                                && reservation.attemptId().equals(candidate.attemptId())
                                && reservation.descriptor().tenantId().equals(candidate.descriptor().tenantId())) {
                            completed[0] = indexMetadataStore.failNativeSubmissionAttempt(
                                    candidate.descriptor().indexId(), candidate.generation(),
                                    candidate.attemptId(), candidate.updatedAt(),
                                    Instant.now(), "native_submission_missing_durable_intent");
                        }
                    }
                });
                if (completed[0]) {
                    fenced++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("event=index_native_abandoned_reservation_retry indexId={} generation={} error={}",
                        candidate.descriptor().indexId(), candidate.generation(), failure.getMessage());
            }
        }
        return fenced;
    }

    /** Replays unbound Native submissions only when their exact durable intent and reservation still exist. */
    public int recoverUnboundSnapshotPins(BooleanSupplier stillLeader) {
        Objects.requireNonNull(stillLeader, "stillLeader");
        Instant cutoff = Instant.now().minus(Duration.ofHours(1));
        var candidates = store.listUnboundSnapshotPinsForRecovery(100, cutoff);
        int recovered = 0;
        for (var candidate : candidates) {
            if (!stillLeader.getAsBoolean()) {
                break;
            }
            try {
                boolean[] completed = {false};
                indexMetadataStore.withIndexLifecycleLock(candidate.indexId(), () -> {
                    if (stillLeader.getAsBoolean()) {
                        var reservation = indexMetadataStore.findIndexGenerationSnapshotReservation(
                                candidate.indexId(), candidate.generation(), candidate.sourceSnapshot());
                        if (reservation.isPresent()) {
                            completed[0] = recoverUnboundSnapshotPin(candidate, reservation.orElseThrow(), cutoff);
                        }
                    }
                });
                if (completed[0]) {
                    recovered++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("event=index_snapshot_pin_submission_retry indexId={} generation={} error={}",
                        candidate.indexId(), candidate.generation(), failure.getMessage());
            }
        }
        return recovered;
    }

    private boolean recoverUnboundSnapshotPin(
            SnapshotPinIntent candidate, Instant reservationUpdatedAt, Instant cutoff) {
        var claimed = store.claimSnapshotPinRecovery(
                candidate.tenantId(), candidate.pinOwner(), cutoff, Instant.now());
        if (claimed.isEmpty()) {
            return false;
        }
        SnapshotPinIntent intent = claimed.orElseThrow();
        int generation = intent.generation();
        IndexDescriptor current = indexMetadataStore.findIndex(intent.indexId()).orElse(null);
        if (current == null || !supports(current)) {
            return false;
        }
        IndexDescriptor requested = current.withGeneration(generation, reservationUpdatedAt)
                .withBuildSnapshotId(intent.sourceSnapshot(), reservationUpdatedAt);
        requireBuildableReservation(current, requested);
        if (!indexMetadataStore.findIndexGenerationSubmissionAttempt(
                intent.indexId(), intent.generation(), intent.sourceSnapshot()).filter(intent.attemptId()::equals)
                .isPresent()) {
            return false;
        }
        var table = new QualifiedTableName(current.catalogName(), current.schemaName(), current.tableName());
        var expected = DeferredAuthorizationScope.raw(table, Set.copyOf(current.columnNames()), table);
        if (!intent.authority().authorizationScope().equals(expected)
                || !intent.sourceId().equals(String.join(".", table.catalog(), table.schema(), table.table()))) {
            throw new IllegalStateException("Snapshot intent differs from its exact recovery source scope");
        }
        // Reauthorize the original durable subject before metadata or connector effects.
        store.reserveSnapshotPin(new SnapshotPinReservation(
                intent.tenantId(), intent.indexId(), intent.generation(), intent.attemptId(),
                intent.sourceId(), intent.sourceSnapshot(), intent.authority(), intent.createdAt()));
        var resolved = DeferredIndexAuthorityResolver.resolveSubmission(intent.authority());
        submitScopedLocked(requested, resolved.table(), intent.tenantId(), resolved.session(), intent.attemptId());
        return true;
    }

    /** Runs bounded idempotent cleanup for ambiguous pin/job and terminal build crash windows. */
    public int cleanupSnapshotPins() {
        return cleanupSnapshotPins(() -> true);
    }

    /** Claims only currently eligible pins; listing does not grant release authority. */
    public int cleanupSnapshotPins(BooleanSupplier stillLeader) {
        Objects.requireNonNull(stillLeader, "stillLeader");
        Instant cutoff = Instant.now().minus(Duration.ofHours(1));
        List<SnapshotPinIntent> candidates = store.listSnapshotPinCleanupCandidates(100, cutoff, cutoff);
        int released = 0;
        for (SnapshotPinIntent candidate : candidates) {
            if (!stillLeader.getAsBoolean()) {
                break;
            }
            try {
                var claim = new AtomicReference<Optional<SnapshotPinIntent>>(Optional.empty());
                indexMetadataStore.withIndexLifecycleLock(candidate.indexId(), () -> {
                    if (stillLeader.getAsBoolean()) {
                        claim.set(store.claimSnapshotPinCleanup(candidate.tenantId(), candidate.pinOwner(),
                                cutoff, cutoff, Instant.now()));
                    }
                });
                if (claim.get().isPresent()) {
                    // RELEASING prevents new acquisition, job binding and retention. Do not
                    // hold the lifecycle DB connection during the connector release.
                    finishSnapshotPinRelease(claim.get().orElseThrow());
                    released++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("event=index_snapshot_pin_cleanup_retry indexId={} generation={} attemptId={} error={}",
                        candidate.indexId(), candidate.generation(), candidate.attemptId(), failure.getMessage());
            }
        }
        return released;
    }

    private SnapshotPinProvider requirePinProvider(ConnectorSession session) {
        SnapshotPinProvider provider = pinProviders.apply(session);
        if (provider == null) {
            throw new IllegalStateException("Authorized catalog does not support durable snapshot pinning");
        }
        return provider;
    }

    private static SnapshotPinOwner owner(SnapshotPinIntent intent) {
        return new SnapshotPinOwner(intent.indexId(), intent.generation(), intent.attemptId());
    }

    private VectorShape resolveVectorShape(
            QualifiedTableName table, String columnName, ConnectorSession session) {
        ConnectorTableMetadata.Column column = resolveColumn(table, columnName, session);
        if (!(column.parameterizedType() instanceof VectorType vector)) {
            throw validation("Native ANN requires a fixed-dimension VECTOR column");
        }
        return new VectorShape(vector.dimension(), vector.elementType());
    }

    private void requireTextColumn(QualifiedTableName table, String columnName, ConnectorSession session) {
        if (resolveColumn(table, columnName, session).type() != PangrovaType.VARCHAR) {
            throw validation("Native BM25 requires a VARCHAR column");
        }
    }

    private ConnectorTableMetadata.Column resolveColumn(
            QualifiedTableName table, String columnName, ConnectorSession session) {
        CatalogHandle catalog = catalogRegistry.getCatalog(table.catalog());
        if (catalog == null) {
            throw validation("Catalog disappeared while resolving index column: "
                    + table.catalog());
        }
        ConnectorMetadata metadata = catalog.getMetadata(session);
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(table.schema(), table.table()));
        if (handle == null) {
            throw validation("Table disappeared while resolving index column: " + table);
        }
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, handle);
        try {
            return org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint.resolveExactColumn(
                    columnName, tableMetadata.typedColumns());
        } catch (IllegalArgumentException failure) {
            throw validation("Index column binding is unavailable: " + columnName, failure);
        }
    }

    private static List<NamedValue> diskAnnProperties(
            Map<String, String> properties, int dimension) {
        int maxDegree = boundedInt(properties, "max_degree", 64, 1, 512);
        int searchListSize = properties.containsKey("search_list_size")
                ? boundedInt(properties, "search_list_size", 100, maxDegree, MAX_SEARCH_LIST)
                : boundedInt(properties, "build_list_size", 100, maxDegree, MAX_SEARCH_LIST);
        double alpha = boundedDouble(properties, "alpha", DEFAULT_DISKANN_ALPHA, 1.0, 2.0);
        int pqSubvectors = properties.containsKey("pq_subvectors")
                ? boundedInt(properties, "pq_subvectors", 1, 1, dimension)
                : greatestDivisorAtMost(dimension, 32);
        if (dimension % pqSubvectors != 0) {
            throw validation("pq_subvectors must divide vector dimension exactly");
        }
        return List.of(
                intProperty("dimension", dimension),
                intProperty("max_degree", maxDegree),
                intProperty("search_list_size", searchListSize),
                doubleProperty("alpha", alpha),
                intProperty("pq_subvectors", pqSubvectors),
                intProperty("pq_bits_per_code",
                        boundedInt(properties, "pq_bits_per_code", 8, 1, 8)),
                intProperty("beam_width",
                        boundedInt(properties, "beam_width", 4, 1, 1_024)));
    }

    private static List<NamedValue> hnswProperties(
            Map<String, String> properties, int dimension) {
        int connections = boundedInt(properties, "max_connections", 16, 2, 512);
        return List.of(
                intProperty("dimension", dimension),
                intProperty("max_connections", connections),
                intProperty("ef_construction", boundedInt(
                        properties, "ef_construction", DEFAULT_HNSW_EF_CONSTRUCTION, connections, MAX_SEARCH_LIST)));
    }

    private static List<NamedValue> ivfPqProperties(
            Map<String, String> properties, int dimension) {
        int subvectors = properties.containsKey("pq_subvectors")
                ? boundedInt(properties, "pq_subvectors", 1, 1, dimension)
                : greatestDivisorAtMost(dimension, 32);
        if (dimension % subvectors != 0) {
            throw validation("pq_subvectors must divide vector dimension exactly");
        }
        return List.of(
                intProperty("dimension", dimension),
                intProperty("coarse_clusters", boundedInt(
                        properties, "coarse_clusters", 64, 1, MAX_COARSE_CLUSTERS)),
                intProperty("pq_subvectors", subvectors),
                intProperty("pq_bits_per_code", boundedInt(
                        properties, "pq_bits_per_code", 8, 1, 8)),
                intProperty("training_iterations", boundedInt(
                        properties, "training_iterations", DEFAULT_TRAINING_ITERATIONS, 1, 100)),
                intProperty("training_samples", boundedInt(
                        properties, "training_samples", DEFAULT_TRAINING_SAMPLES, 256, MAX_TRAINING_SAMPLES)));
    }

    static List<NamedValue> vectorProperties(
            CapabilityRef buildCapability,
            Map<String, String> properties, int dimension, String metric) {
        String capabilityId = buildCapability.getCapabilityId();
        List<NamedValue> result;
        if (BuiltInCapabilityIdentifiers.HNSW_ARTIFACT_BUILD_ID.equals(capabilityId)) {
            result = new ArrayList<>(hnswProperties(properties, dimension));
        } else if (BuiltInCapabilityIdentifiers.IVF_PQ_ARTIFACT_BUILD_ID.equals(capabilityId)) {
            result = new ArrayList<>(ivfPqProperties(properties, dimension));
        } else if (BuiltInCapabilityIdentifiers.DISKANN_ARTIFACT_BUILD_ID.equals(capabilityId)) {
            result = new ArrayList<>(diskAnnProperties(properties, dimension));
        } else {
            throw validation("Unknown Native artifact build capability");
        }
        result.add(stringProperty("metric", metric));
        return List.copyOf(result);
    }

    private static NamedValue intProperty(String name, long value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("BIGINT"))
                        .setInt64Value(value)).build();
    }

    private static NamedValue doubleProperty(String name, double value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("DOUBLE"))
                        .setDoubleValue(value)).build();
    }

    private static NamedValue stringProperty(String name, String value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                        .setStringValue(value)).build();
    }

    private static int greatestDivisorAtMost(int dimension, int ceiling) {
        for (int candidate = Math.min(dimension, ceiling); candidate >= 1; candidate--) {
            if (dimension % candidate == 0) {
                return candidate;
            }
        }
        return 1;
    }

    static String requireMetric(Map<String, String> properties, String algorithm) {
        String metric = properties.getOrDefault("metric", "L2")
                .trim().toUpperCase(Locale.ROOT);
        if (metric.equals("EUCLIDEAN")) {
            metric = "L2";
        }
        if (!metric.equals("L2") && !metric.equals("COSINE")
                && !metric.equals("INNER_PRODUCT")) {
            throw validation("Unsupported Native ANN metric: " + metric);
        }
        if (algorithm.equals("DISK_ANN") && !metric.equals("L2")) {
            throw validation("Native DiskANN requires metric=L2");
        }
        return metric;
    }

    private static void validateDeclaredDimension(Map<String, String> properties, int actual) {
        String declared = properties.get("dimension");
        if (declared != null && boundedInt(properties, "dimension", actual, 1, MAX_VECTOR_DIMENSION) != actual) {
            throw validation("Index dimension differs from the VECTOR column dimension");
        }
    }

    private static String normalizedAlgorithm(IndexDescriptor descriptor) {
        String defaultAlgorithm = descriptor.indexType() == IndexType.INVERTED
                ? "BM25" : "HNSW";
        return descriptor.properties().getOrDefault("algorithm", defaultAlgorithm)
                .trim().toUpperCase(Locale.ROOT);
    }

    private static String requireSingleColumn(IndexDescriptor descriptor) {
        if (descriptor.columnNames().size() != 1) {
            throw validation("Native index artifacts require exactly one source column");
        }
        return descriptor.columnNames().get(0);
    }

    private String stagingUri(String tenantId, IndexDescriptor descriptor, String attempt) {
        String base = baseArtifactUri + "/v2/tenants/" + DeferredIndexAuthorityResolver.tenantScope(tenantId)
                + "/artifacts/" + safeComponent(descriptor.indexId());
        String generation = "/generation-" + descriptor.generation();
        // Keep new attempts outside the legacy generation prefix, including recursive object cleanup.
        return LEGACY_SUBMISSION_ATTEMPT.equals(attempt) ? base + generation
                : base + "/attempt-" + safeComponent(attempt) + generation;
    }

    /** Historical deterministic job key, retained only for legacy reconciliation. */
    static String jobId(String tenantId, IndexDescriptor descriptor) {
        return jobId(tenantId, descriptor, LEGACY_SUBMISSION_ATTEMPT);
    }

    static String jobId(String tenantId, IndexDescriptor descriptor, String attempt) {
        String base = "index-build:tenant-" + DeferredIndexAuthorityResolver.tenantScope(tenantId) + ":"
                + safeComponent(descriptor.indexId()) + ":generation:" + descriptor.generation();
        return LEGACY_SUBMISSION_ATTEMPT.equals(attempt) ? base : base + ":attempt:" + safeComponent(attempt);
    }

    private static String safeComponent(String value) {
        if (value == null || value.isBlank() || value.contains("..")
                || value.contains("/") || value.contains("\\") || value.indexOf('\0') >= 0) {
            throw validation("Artifact path identity contains unsafe characters");
        }
        return value;
    }

    private static String stripTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        if (result.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be a filesystem root");
        }
        return result;
    }

    private static String requireTenantId(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw validation("tenant identity is required for durable artifact work");
        }
        return tenantId;
    }

    private static int boundedInt(
            Map<String, String> properties, String key,
            int defaultValue, int minimum, int maximum) {
        long value = boundedLong(properties, key, defaultValue, minimum, maximum);
        return Math.toIntExact(value);
    }

    private static long boundedLong(
            Map<String, String> properties, String key,
            long defaultValue, long minimum, long maximum) {
        String raw = properties.get(key);
        final long value;
        try {
            value = raw == null ? defaultValue : Long.parseLong(raw);
        } catch (NumberFormatException failure) {
            throw validation(key + " must be an integer", failure);
        }
        if (value < minimum || value > maximum) {
            throw validation(key + " must be in [" + minimum + ", " + maximum + "]");
        }
        return value;
    }

    private static double boundedDouble(
            Map<String, String> properties, String key,
            double defaultValue, double minimum, double maximum) {
        String raw = properties.get(key);
        final double value;
        try {
            value = raw == null ? defaultValue : Double.parseDouble(raw);
        } catch (NumberFormatException failure) {
            throw validation(key + " must be numeric", failure);
        }
        if (!Double.isFinite(value) || value < minimum || value > maximum) {
            throw validation(key + " must be finite and in [" + minimum + ", " + maximum + "]");
        }
        return value;
    }

    private static PangrovaException validation(String message) {
        return new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR, message);
    }

    private static PangrovaException validation(String message, Throwable cause) {
        return new PangrovaException(
                PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR, message, cause);
    }

    private record VectorShape(int dimension, VectorElementType elementType) {}

    private record NativeBuildSpec(
            GenerationPlan plan,
            String artifactType,
            List<NamedValue> properties) {}
}
