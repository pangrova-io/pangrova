package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.control.ArtifactBuildJob;
import org.pangrova.indexing.control.ArtifactBuildLease;
import org.pangrova.indexing.control.ArtifactBuildJobKey;
import org.pangrova.indexing.control.ArtifactBuildOutput;
import org.pangrova.indexing.control.ArtifactPageCompletion;
import org.pangrova.indexing.control.ArtifactBuildPage;
import org.pangrova.indexing.control.ArtifactBuildDispatchSpec;
import org.pangrova.indexing.control.ArtifactBuildStore;
import org.pangrova.ai.api.FeatureCatalogStore.ExportRequest;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.indexing.store.jdbc.JdbcIndexSupport.setInstant;
import static org.pangrova.indexing.store.jdbc.JdbcArtifactBuildPageSelection.findPage;
import static org.pangrova.indexing.store.jdbc.JdbcArtifactBuildPageSelection.selectClaimablePage;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
@org.pangrova.spi.annotation.Internal
public final class JdbcArtifactBuildStore extends JdbcArtifactBuildStoreSupport implements ArtifactBuildStore {

    private final JdbcArtifactBuildReader reader;
    private final ArtifactCurrentAuthority authority;

    public JdbcArtifactBuildStore(DataSource dataSource, ArtifactCurrentAuthority authority) {
        super(dataSource);
        this.reader = new JdbcArtifactBuildReader(dataSource);
        this.authority = Objects.requireNonNull(authority, "authority");
    }

    @Override
    public void createJob(
            ArtifactBuildJob job, ArtifactBuildDispatchSpec dispatchSpec,
            List<ArtifactBuildPage> pages) {
        List<ArtifactBuildPage> immutablePages =
                JdbcArtifactBuildPageLoader.validateNewJob(job, dispatchSpec, pages);
        inTransaction("create artifact build job", connection -> {
            createJob(authority, connection, job, dispatchSpec, immutablePages);
            return null;
        });
    }

    @Override
    public void createCatalogExportJob(
            ExportRequest request, ArtifactBuildJob job,
            ArtifactBuildDispatchSpec dispatchSpec, List<ArtifactBuildPage> pages) {
        Objects.requireNonNull(request, "request must not be null");
        if (!request.tenantId().equals(job.tenantId())
                || !request.jobId().equals(job.jobId())
                || !request.artifactId().equals(job.artifactId())
                || !request.generation().equals(job.stagingGeneration())
                || !request.dependencySha256().equalsIgnoreCase(
                        job.dependencySet().sha256())) {
            throw new IllegalArgumentException(
                    "catalog export link differs from its artifact job identity");
        }
        List<ArtifactBuildPage> immutablePages =
                JdbcArtifactBuildPageLoader.validateNewJob(job, dispatchSpec, pages);
        inTransaction("create catalog export artifact job", connection -> {
            createJob(authority, connection, job, dispatchSpec, immutablePages);
            JdbcCatalogExportJobInsertion.insert(connection, request);
            return null;
        });
    }

    private static void createJob(
            ArtifactCurrentAuthority authority, Connection connection, ArtifactBuildJob job,
            ArtifactBuildDispatchSpec dispatchSpec,
            List<ArtifactBuildPage> immutablePages) throws SQLException {
        authority.requireCurrent(connection, dispatchSpec);
        Optional<ArtifactBuildJob> existing = findJob(
                connection, job.tenantId(), job.jobId(), true);
        if (existing.isPresent()) {
            validateIdempotentCreate(
                    connection, existing.orElseThrow(), job, dispatchSpec, immutablePages);
            return;
        }
        JdbcArtifactBuildJobInsertion.insertJob(connection, job);
        ArtifactExecutionFence.insertRoute(connection, job.tenantId(), job.jobId());
        insertDispatchSpec(connection, dispatchSpec);
        JdbcArtifactStageExchangeStore.insertGraphEdges(connection, dispatchSpec);
        for (ArtifactBuildPage page : immutablePages) {
            insertPage(connection, job.tenantId(), page, job.createdAt());
        }
        for (ArtifactBuildPage page : immutablePages) {
            insertSplits(connection, job.tenantId(), page);
            insertDependencies(connection, job.tenantId(), page);
        }
    }

    /**
     * Inserts the canonical job, graph and page allocations in a caller-owned PostgreSQL transaction.
     * Current authorization and idempotency use this same connection; the caller owns commit/rollback.
     */
    public static void createJobWithinTransaction(ArtifactCurrentAuthority authority,
            Connection connection, ArtifactBuildJob job,
            ArtifactBuildDispatchSpec dispatchSpec, List<ArtifactBuildPage> pages) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(authority, "authority");
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("artifact creation requires an explicit caller-owned transaction");
        }
        createJob(authority, connection, job, dispatchSpec,
                JdbcArtifactBuildPageLoader.validateNewJob(job, dispatchSpec, pages));
    }

    @Override
    public List<ArtifactBuildJobKey> listRunnableJobs(int limit, Instant now) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Objects.requireNonNull(now, "now must not be null");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT j.tenant_id,j.job_id FROM artifact_build_jobs j "
                             + "WHERE j.state IN ('QUEUED','RUNNING') AND EXISTS ("
                             + "SELECT 1 FROM artifact_build_pages p "
                             + "WHERE p.tenant_id=j.tenant_id AND p.job_id=j.job_id "
                             + "AND ((p.state='PENDING' AND p.next_available_at<=?) "
                             + "OR (p.state='LEASED' AND p.lease_expires_at<?)) "
                             + "AND NOT EXISTS (SELECT 1 FROM artifact_build_page_dependencies d "
                             + "JOIN artifact_build_pages u ON u.tenant_id=d.tenant_id "
                             + "AND u.job_id=d.job_id "
                             + "AND u.page_id=d.dependency_page_id WHERE d.job_id=p.job_id "
                             + "AND d.tenant_id=p.tenant_id AND d.page_id=p.page_id "
                             + "AND u.state<>'SUCCEEDED') "
                             + "AND NOT EXISTS (SELECT 1 FROM artifact_stage_exchanges e WHERE "
                             + "e.tenant_id=p.tenant_id AND e.job_id=p.job_id AND e.consumer_stage_id=p.stage_id "
                             + "AND NOT EXISTS (SELECT 1 FROM artifact_exchange_finalizations f WHERE "
                             + "f.tenant_id=e.tenant_id AND f.job_id=e.job_id AND f.exchange_id=e.exchange_id))) "
                             + "ORDER BY j.updated_at,j.tenant_id,j.job_id LIMIT ?")) {
            setInstant(statement, 1, now);
            setInstant(statement, 2, now);
            statement.setInt(3, limit);
            List<ArtifactBuildJobKey> jobs = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    jobs.add(new ArtifactBuildJobKey(
                            result.getString(1), result.getString(2)));
                }
            }
            return List.copyOf(jobs);
        } catch (SQLException e) {
            throw failure("list runnable artifact jobs", e);
        }
    }

    @Override
    public List<ArtifactBuildJobKey> listReadyForPublication(int limit, Instant now) {
        return JdbcArtifactPublicationClaims.listReadyForPublication(dataSource, limit, now);
    }

    @Override
    public Optional<ArtifactBuildLease> claimNextPage(
            String tenantId, String jobId,
            org.pangrova.spi.coordination.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now) {
        String leaseOwner = Objects.requireNonNull(executionAnchor, "executionAnchor").ownerId();
        if (!new ArtifactBuildJobKey(tenantId, jobId).executionId().equals(executionAnchor.executionId())) {
            throw new IllegalArgumentException("execution anchor belongs to another artifact job");
        }
        requireLeaseRequest(tenantId, jobId, leaseOwner, ownerEpoch, leaseDuration, now);
        return inTransaction("claim artifact build page", connection -> {
            var context = CurrentDeferredExecutionContext.resolve(authority, connection, tenantId, jobId,
                    org.pangrova.indexing.control.ArtifactExecutionOwner.from(executionAnchor));
            JdbcArtifactStageExchangeStore.lockValidatedBarriers(connection, context);
            ArtifactBuildJob job = findJob(connection, tenantId, jobId, true).orElse(null);
            if (job == null || terminal(job.state())) {
                return Optional.empty();
            }
            PageRow page = selectClaimablePage(connection, new JdbcArtifactBuildPageSelection.Selection(
                    tenantId, jobId, now, Optional.empty())).orElse(null);
            if (page == null) {
                return Optional.empty();
            }
            return JdbcArtifactBuildPageClaim.claim(connection, page, executionAnchor, ownerEpoch, leaseDuration, now);
        });
    }

    @Override
    public boolean renewPage(
            ArtifactBuildLease lease, Duration leaseDuration, Instant now) {
        Objects.requireNonNull(lease, "lease must not be null");
        requirePositiveDuration(leaseDuration);
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("renew authorized artifact build page", connection -> {
            authority.requireCurrent(connection, lease.tenantId(), lease.jobId());
            ArtifactExecutionFence.requireAnchor(connection, lease.tenantId(), lease.jobId(), lease.executionAnchor());
            try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_pages SET lease_expires_at=?, updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND page_id=? AND state='LEASED' "
                        + "AND fencing_token=? AND lease_owner=? AND owner_epoch=? "
                        + "AND lease_expires_at>=?")) {
                setInstant(statement, 1, now.plus(leaseDuration));
                setInstant(statement, 2, now);
                bindLeaseIdentity(statement, 3, lease);
                setInstant(statement, 9, now);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public boolean completePage(ArtifactPageCompletion completion) {
        requireCompletion(completion);
        return inTransaction("complete artifact build page", connection ->
                JdbcArtifactBuildCompletion.complete(connection, completion, authority));
    }

    @Override
    public boolean retryPage(
            ArtifactBuildLease lease, Instant nextAvailableAt,
            String failureReason, Instant now) {
        Objects.requireNonNull(lease, "lease must not be null");
        Objects.requireNonNull(nextAvailableAt, "nextAvailableAt must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("retry artifact build page", connection -> {
            authority.requireCurrent(connection, lease.tenantId(), lease.jobId());
            ArtifactExecutionFence.requireAnchor(connection, lease.tenantId(), lease.jobId(), lease.executionAnchor());
            PageRow page = findPage(connection, lease.tenantId(), lease.jobId(),
                    lease.pageId(), true).orElse(null);
            if (!matchesLiveLease(page, lease, now)) {
                return false;
            }
            if (page.attemptId() + 1 >= page.maxAttempts()) {
                failPageInTransaction(connection, lease, failureReason, now);
                return true;
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE artifact_build_pages SET state='PENDING', next_available_at=?, "
                            + "lease_owner=NULL, lease_expires_at=NULL, failure_reason=?, "
                            + "updated_at=? WHERE tenant_id=? AND job_id=? AND page_id=? "
                            + "AND state='LEASED' "
                            + "AND fencing_token=? AND owner_epoch=?")) {
                setInstant(statement, 1, nextAvailableAt);
                statement.setString(2, normalizeFailure(failureReason));
                setInstant(statement, 3, now);
                statement.setString(4, lease.tenantId());
                statement.setString(5, lease.jobId());
                statement.setString(6, lease.pageId());
                statement.setLong(7, lease.fencingToken());
                statement.setLong(8, lease.ownerEpoch());
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public boolean failPage(
            ArtifactBuildLease lease, String failureReason, Instant now) {
        Objects.requireNonNull(lease, "lease must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("fail artifact build page", connection -> {
            authority.requireCurrent(connection, lease.tenantId(), lease.jobId());
            ArtifactExecutionFence.requireAnchor(connection, lease.tenantId(), lease.jobId(), lease.executionAnchor());
            return failPageInTransaction(connection, lease, failureReason, now);
        });
    }

    @Override
    public Optional<ArtifactBuildJob> findJob(String tenantId, String jobId) {
        return reader.findJob(tenantId, jobId);
    }

    @Override
    public Optional<ArtifactBuildJobKey> findExecutionJob(String executionId) {
        return reader.findExecutionJob(executionId);
    }

    @Override
    public Optional<ArtifactBuildDispatchSpec> findDispatchSpec(
            String tenantId, String jobId) {
        return reader.findDispatchSpec(tenantId, jobId);
    }

    @Override
    public ArtifactBuildDispatchSpec loadAuthorizedDispatchSpec(String tenantId, String jobId) {
        return inTransaction("load authorized artifact work", connection ->
                authority.requireCurrent(connection, tenantId, jobId));
    }

    @Override
    public Optional<org.pangrova.indexing.api.ArtifactGenerationManifest>
            findPublishedGeneration(
                    String tenantId, String artifactId,
                    String generation, String sourceSnapshot) {
        return reader.findPublishedGeneration(
                tenantId, artifactId, generation, sourceSnapshot);
    }

    @Override
    public List<ArtifactBuildOutput> loadCompleteOutputs(String tenantId, String jobId) {
        return reader.loadCompleteOutputs(tenantId, jobId);
    }

    @Override
    public List<ArtifactBuildOutput> loadDependencyOutputs(
            String tenantId, String jobId, List<String> dependencyPageIds) {
        return reader.loadDependencyOutputs(tenantId, jobId, dependencyPageIds);
    }

    @Override
    public List<ArtifactBuildOutput> loadPublishedOutputs(
            String tenantId, String jobId, Set<String> sourceObjectIds) {
        return reader.loadPublishedOutputs(tenantId, jobId, sourceObjectIds);
    }

    @Override
    public boolean matchesPublishedSplits(
            String tenantId, String jobId, Map<String, String> sourceSplitSha256) {
        return reader.matchesPublishedSplits(tenantId, jobId, sourceSplitSha256);
    }

    @Override
    public Map<String, org.pangrova.indexing.control.ArtifactBuildSplit> loadPublishedSourceSplits(
            String tenantId, String jobId, Set<String> sourceObjectIds) {
        return reader.loadPublishedSourceSplits(tenantId, jobId, sourceObjectIds);
    }

    @Override
    public Optional<ArtifactBuildJob> claimReadyForPublication(
            String tenantId, String jobId,
            org.pangrova.spi.coordination.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now) {
        String owner = Objects.requireNonNull(executionAnchor, "executionAnchor").ownerId();
        requireLeaseRequest(tenantId, jobId, owner, ownerEpoch, leaseDuration, now);
        return inTransaction("claim artifact publication", connection ->
                JdbcArtifactPublicationClaims.claimReadyForPublication(
                        connection, authority, tenantId, jobId, executionAnchor,
                        ownerEpoch, leaseDuration, now));
    }

    @Override
    public boolean markPublished(
            String tenantId, String jobId, String owner, long ownerEpoch,
            long publicationFencingToken, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        requireText(owner, "owner");
        Objects.requireNonNull(now, "now must not be null");
        if (ownerEpoch <= 0 || publicationFencingToken <= 0) {
            throw new IllegalArgumentException("publication epoch and fence must be positive");
        }
        return inTransaction("mark authorized artifact published", connection ->
                JdbcArtifactPublicationClaims.markPublished(
                        connection, authority, tenantId, jobId, owner, ownerEpoch,
                        publicationFencingToken, now));
    }

    @Override
    public boolean deferPublication(
            ArtifactBuildJob claim, String owner, long ownerEpoch,
            Instant nextAvailableAt, String failureReason, Instant now) {
        Objects.requireNonNull(claim, "claim must not be null");
        String tenantId = claim.tenantId();
        String jobId = claim.jobId();
        long publicationFencingToken = claim.publicationFencingToken();
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        requireText(owner, "owner");
        Objects.requireNonNull(nextAvailableAt, "nextAvailableAt must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (ownerEpoch <= 0 || publicationFencingToken <= 0) {
            throw new IllegalArgumentException("publication epoch and fence must be positive");
        }
        if (!nextAvailableAt.isAfter(now)) {
            throw new IllegalArgumentException("nextAvailableAt must be after now");
        }
        return inTransaction("defer authorized artifact publication", connection ->
                JdbcArtifactPublicationClaims.deferPublication(
                        connection, authority, claim, owner, ownerEpoch,
                        nextAvailableAt, failureReason, now));
    }

    @Override
    public boolean cancelJob(String tenantId, String jobId, String reason, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(now, "now");
        return inTransaction("cancel revoked artifact work", connection ->
                ArtifactBuildRevocation.cancel(connection, tenantId, jobId, normalizeFailure(reason), now));
    }

    @Override
    public int cancelArtifactBuilds(String tenantId, String artifactId, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("cancel artifact builds", connection ->
                ArtifactBuildRevocation.cancelArtifact(connection, tenantId, artifactId, now));
    }

}
