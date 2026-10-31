package org.pangrova.databuild.api.build;

import org.pangrova.databuild.api.manifest.DataBuildGenerationManifest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Durable, lease-fenced control plane for generic immutable artifact builds. */
public interface DataBuildStore {

    void createJob(
            DataBuildJob job, DataBuildDispatchSpec dispatchSpec,
            List<DataBuildPage> pages);

    /** Returns runnable jobs in stable age order; page claiming remains fenced and atomic. */
    List<DataBuildJobKey> listRunnableJobs(int limit, Instant now);

    /** Returns complete jobs with an available/expired publication lease in stable order. */
    List<DataBuildJobKey> listReadyForPublication(int limit, Instant now);

    Optional<DataBuildLease> claimNextPage(
            String tenantId, String jobId,
            org.pangrova.query.api.coordination.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now);

    boolean renewPage(DataBuildLease lease, Duration leaseDuration, Instant now);

    /** Commits one validated Native report and atomically advances the all-pages barrier. */
    boolean completePage(DataBuildPageCompletion completion);

    boolean retryPage(
            DataBuildLease lease, Instant nextAvailableAt,
            String failureReason, Instant now);

    boolean failPage(DataBuildLease lease, String failureReason, Instant now);

    Optional<DataBuildJob> findJob(String tenantId, String jobId);

    /** Finds one exact durable routing association, without granting execution authority. */
    Optional<DataBuildJobKey> findExecutionJob(String executionId);

    Optional<DataBuildDispatchSpec> findDispatchSpec(String tenantId, String jobId);

    /** Loads replayable work only after current subject, tenant, grants and observed policies are validated. */
    DataBuildDispatchSpec loadAuthorizedDispatchSpec(String tenantId, String jobId);

    /** Looks up the exact immutable publication; unpublished work is never returned. */
    Optional<DataBuildGenerationManifest> findPublishedGeneration(
            String tenantId, String artifactId, String generation, String sourceSnapshot);

    List<DataBuildOutput> loadCompleteOutputs(String tenantId, String jobId);

    /**
     * Loads immutable intermediate objects from an exact set of succeeded
     * dependency pages. Artifact bytes remain in object storage.
     */
    List<DataBuildOutput> loadDependencyOutputs(
            String tenantId, String jobId, List<String> dependencyPageIds);

    /**
     * Loads only immutable objects belonging to the requested source objects
     * of a published generation. Implementations must fail closed when the
     * generation is not published; query dispatch must never observe staging
     * output or scan an entire large manifest to bind one task partition.
     */
    List<DataBuildOutput> loadPublishedOutputs(
            String tenantId, String jobId, java.util.Set<String> sourceObjectIds);

    /**
     * Compares exact canonical split bytes for durable stage dependencies.
     * Query scans may add predicates or projection detail and instead compare
     * immutable source ranges using {@link #loadPublishedSourceSplits}.
     */
    boolean matchesPublishedSplits(
            String tenantId, String jobId,
            java.util.Map<String, String> sourceSplitSha256);

    /** Loads checksummed build splits for immutable source-range comparison by the Coordinator. */
    java.util.Map<String, DataBuildSplit> loadPublishedSourceSplits(
            String tenantId, String jobId, java.util.Set<String> sourceObjectIds);

    /** Claims the metadata-only publication lease for a job whose every page succeeded. */
    Optional<DataBuildJob> claimReadyForPublication(
            String tenantId, String jobId,
            org.pangrova.query.api.coordination.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now);

    /**
     * Releases one exact publication claim into a bounded retry delay.
     * The publication fencing token prevents an obsolete publisher from
     * delaying a newer claim after failover.
     */
    boolean deferPublication(
            DataBuildJob claim, String owner, long ownerEpoch,
            Instant nextAvailableAt, String failureReason, Instant now);

    boolean markPublished(
            String tenantId, String jobId, String owner, long ownerEpoch,
            long publicationFencingToken, Instant now);

    /** Cancels every non-terminal generation for an artifact and fences its work pages. */
    int cancelArtifactBuilds(String tenantId, String artifactId, Instant now);

    /** Cancels exactly one uncompleted job after its subject or observed policy loses authority. */
    boolean cancelJob(String tenantId, String jobId, String reason, Instant now);
}
