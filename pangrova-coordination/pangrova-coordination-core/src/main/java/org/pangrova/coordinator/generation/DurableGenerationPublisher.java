package org.pangrova.coordinator.generation;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobStore;
import org.pangrova.generation.api.manifest.GenerationManifest;
import org.pangrova.generation.api.publication.GenerationPublicationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Horizontally scalable control loop that atomically publishes completed artifacts.
 *
 * <p>This component handles metadata only. Artifact bytes remain in immutable
 * object storage and query data never enters the Coordinator JVM. Publication
 * is partitioned by durable job lease rather than serialized behind the global
 * maintenance lease; every maintenance node may run this loop concurrently.</p>
 */
public final class DurableGenerationPublisher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DurableGenerationPublisher.class);
    private static final Duration DEFAULT_CLAIM_TTL = Duration.ofSeconds(30);
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(15);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(1);
    private static final int DEFAULT_BATCH_SIZE = 64;

    private final GenerationJobStore buildStore;
    private final Map<String, GenerationPublicationStore> publicationStores;
    private final String owner;
    private final GenerationExecutionOwnership executionOwnership;
    private final Duration claimTtl;
    private final Duration retryDelay;
    private final Duration pollInterval;
    private final int batchSize;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean();

    /** Creates a production publication loop using bounded default claim and retry intervals. */
    public DurableGenerationPublisher(
            GenerationJobStore buildStore,
            Map<String, GenerationPublicationStore> publicationStores,
            GenerationExecutionOwnership executionOwnership) {
        this(buildStore, publicationStores, executionOwnership,
                new PollSettings(DEFAULT_CLAIM_TTL, DEFAULT_RETRY_DELAY, DEFAULT_POLL_INTERVAL,
                        DEFAULT_BATCH_SIZE, Clock.systemUTC()),
                ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                        "artifact-publication"));
    }

    DurableGenerationPublisher(
            GenerationJobStore buildStore,
            Map<String, GenerationPublicationStore> publicationStores,
            GenerationExecutionOwnership executionOwnership,
            PollSettings settings,
            ScheduledExecutorService scheduler) {
        this.buildStore = Objects.requireNonNull(buildStore, "buildStore must not be null");
        this.publicationStores = Map.copyOf(publicationStores);
        this.executionOwnership = Objects.requireNonNull(executionOwnership, "executionOwnership");
        this.owner = executionOwnership.coordinatorId();
        Objects.requireNonNull(settings, "settings must not be null");
        this.claimTtl = requirePositive(settings.claimTtl(), "claimTtl");
        this.retryDelay = requirePositive(settings.retryDelay(), "retryDelay");
        this.pollInterval = requirePositive(settings.pollInterval(), "pollInterval");
        if (settings.batchSize() <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = settings.batchSize();
        this.clock = Objects.requireNonNull(settings.clock(), "clock must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    /** Starts the idempotent maintenance loop. */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        scheduler.scheduleWithFixedDelay(
                this::publishAvailableSafely,
                0,
                pollInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        LOG.info("event=artifact_publisher_started owner={} batchSize={}", owner, batchSize);
    }

    record PollSettings(Duration claimTtl, Duration retryDelay, Duration pollInterval, int batchSize, Clock clock) {}

    void publishAvailable() {
        Instant now = clock.instant();
        for (var jobKey : buildStore.listReadyForPublication(batchSize, now)) {
            publishOne(jobKey.tenantId(), jobKey.jobId(), now);
        }
    }

    private void publishOne(String tenantId, String jobId, Instant now) {
        var key = new org.pangrova.generation.api.job.GenerationJobKey(tenantId, jobId);
        var anchor = executionOwnership.acquire(key);
        if (anchor.isEmpty()) {
            return;
        }
        long ownerEpoch = anchor.orElseThrow().fencingToken();
        GenerationJob claim = null;
        try {
            claim = buildStore.claimReadyForPublication(
                    tenantId, jobId, anchor.orElseThrow(), ownerEpoch, claimTtl, now).orElse(null);
            if (claim == null) return;
            GenerationPublicationStore publisher = publicationStores.get(claim.artifactType());
            if (publisher == null) {
                throw new IllegalStateException(
                        "No publication adapter for artifact type " + claim.artifactType());
            }
            GenerationManifest manifest = publisher.publishClaimed(
                    claim, owner, ownerEpoch, clock.instant());
            LOG.info("event=artifact_published jobId={} artifactId={} generation={} "
                            + "manifestSha256={} objectCount={} rowCount={} totalBytes={}",
                    manifest.jobId(), manifest.artifactId(), manifest.generation(),
                    manifest.manifestSha256(), manifest.objectCount(), manifest.rowCount(),
                    manifest.totalBytes());
            executionOwnership.release(key);
        } catch (org.pangrova.security.api.AuthorizationDeniedException revoked) {
            buildStore.cancelJob(tenantId, jobId, revoked.getMessage(), clock.instant());
            executionOwnership.release(key);
            LOG.warn("event=artifact_publication_revoked jobId={} tenantId={} reason={}",
                    jobId, tenantId, revoked.getMessage());
        } catch (org.pangrova.generation.api.execution.GenerationExecutionOwnerLostException staleOwner) {
            LOG.warn("event=artifact_publication_owner_lost jobId={} tenantId={}", jobId, tenantId);
        } catch (RuntimeException failure) {
            if (claim == null) throw failure;
            Instant failedAt = clock.instant();
            boolean deferred = buildStore.deferPublication(
                    claim, owner, ownerEpoch,
                    failedAt.plus(retryDelay), failure.getMessage(), failedAt);
            LOG.error("event=artifact_publication_failed jobId={} artifactId={} "
                            + "generation={} deferred={} error={}",
                    claim.jobId(), claim.artifactId(), claim.stagingGeneration(), deferred,
                    failure.getMessage(), failure);
        }
    }

    private void publishAvailableSafely() {
        try {
            publishAvailable();
        } catch (RuntimeException failure) {
            LOG.error("event=artifact_publication_poll_failed error={}",
                    failure.getMessage(), failure);
        }
    }

    @Override
    public void close() {
        if (!started.compareAndSet(true, false)) {
            scheduler.shutdownNow();
            return;
        }
        scheduler.shutdownNow();
        LOG.info("event=artifact_publisher_stopped owner={}", owner);
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
