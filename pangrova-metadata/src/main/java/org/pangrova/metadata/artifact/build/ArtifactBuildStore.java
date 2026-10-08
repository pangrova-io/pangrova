package org.pangrova.metadata.artifact.build;

import org.pangrova.metadata.artifact.ArtifactGenerationManifest;

import org.pangrova.metadata.feature.FeatureCatalogStore.ExportRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Durable, lease-fenced control plane for generic immutable artifact builds. */
public interface ArtifactBuildStore {

    void createJob(
            ArtifactBuildJob job, ArtifactBuildDispatchSpec dispatchSpec,
            List<ArtifactBuildPage> pages);

    /** Atomically creates one catalog-export link and its durable artifact job. */
    void createCatalogExportJob(
            ExportRequest request, ArtifactBuildJob job,
            ArtifactBuildDispatchSpec dispatchSpec, List<ArtifactBuildPage> pages);

    /** Returns runnable jobs in stable age order; page claiming remains fenced and atomic. */
    List<ArtifactBuildJobKey> listRunnableJobs(int limit, Instant now);

    /** Returns complete jobs with an available/expired publication lease in stable order. */
    List<ArtifactBuildJobKey> listReadyForPublication(int limit, Instant now);

    Optional<ArtifactBuildLease> claimNextPage(
            String tenantId, String jobId,
            org.pangrova.spi.coordination.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now);

    boolean renewPage(ArtifactBuildLease lease, Duration leaseDuration, Instant now);

    /** Commits one validated Native report and atomically advances the all-pages barrier. */
    boolean completePage(ArtifactPageCompletion completion);

    boolean retryPage(
            ArtifactBuildLease lease, Instant nextAvailableAt,
            String failureReason, Instant now);

    boolean failPage(ArtifactBuildLease lease, String failureReason, Instant now);

    Optional<ArtifactBuildJob> findJob(String tenantId, String jobId);

    /** Finds one exact durable routing association, without granting execution authority. */
    Optional<ArtifactBuildJobKey> findExecutionJob(String executionId);

    Optional<ArtifactBuildDispatchSpec> findDispatchSpec(String tenantId, String jobId);

    /** Loads replayable work only after current subject, tenant, grants and observed policies are validated. */
    ArtifactBuildDispatchSpec loadAuthorizedDispatchSpec(String tenantId, String jobId);

    /** Looks up the exact immutable publication; unpublished work is never returned. */
    Optional<ArtifactGenerationManifest> findPublishedGeneration(
            String tenantId, String artifactId, String generation, String sourceSnapshot);

    List<ArtifactBuildOutput> loadCompleteOutputs(String tenantId, String jobId);

    /**
     * Loads immutable intermediate objects from an exact set of succeeded
     * dependency pages. Artifact bytes remain in object storage.
     */
    List<ArtifactBuildOutput> loadDependencyOutputs(
            String tenantId, String jobId, List<String> dependencyPageIds);

    /**
     * Loads only immutable objects belonging to the requested source objects
     * of a published generation. Implementations must fail closed when the
     * generation is not published; query dispatch must never observe staging
     * output or scan an entire large manifest to bind one task partition.
     */
    List<ArtifactBuildOutput> loadPublishedOutputs(
            String tenantId, String jobId, java.util.Set<String> sourceObjectIds);

    /**
     * Compares query splits with the exact canonical splits used to build a
     * published generation without returning their payloads. This prevents an
     * artifact built for a different file range/layout from being attached to
     * a query merely because the source object name is equal.
     */
    boolean matchesPublishedSplits(
            String tenantId, String jobId,
            java.util.Map<String, String> sourceSplitSha256);

    /** Claims the metadata-only publication lease for a job whose every page succeeded. */
    Optional<ArtifactBuildJob> claimReadyForPublication(
            String tenantId, String jobId,
            org.pangrova.spi.coordination.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now);

    /**
     * Releases one exact publication claim into a bounded retry delay.
     * The publication fencing token prevents an obsolete publisher from
     * delaying a newer claim after failover.
     */
    boolean deferPublication(
            ArtifactBuildJob claim, String owner, long ownerEpoch,
            Instant nextAvailableAt, String failureReason, Instant now);

    boolean markPublished(
            String tenantId, String jobId, String owner, long ownerEpoch,
            long publicationFencingToken, Instant now);

    /** Cancels every non-terminal generation for an artifact and fences its work pages. */
    int cancelArtifactBuilds(String tenantId, String artifactId, Instant now);

    /** Cancels exactly one uncompleted job after its subject or observed policy loses authority. */
    boolean cancelJob(String tenantId, String jobId, String reason, Instant now);
}
