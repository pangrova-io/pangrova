package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.jdbc.api.generation.GenerationJobSubmissionTransactions;
import org.pangrova.persistence.jdbc.api.generation.GenerationPublicationCandidates;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;
import org.pangrova.persistence.generation.postgresql.execution.PostgresGenerationExecutionRoutes;

import org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority;
import org.pangrova.persistence.generation.postgresql.exchange.PostgresGenerationStageExchangeStore;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.job.SnapshotPinReservation;

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

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;
import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationPageSelection.findPage;
import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationPageSelection.selectClaimablePage;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
@org.pangrova.common.api.annotation.Internal
public final class PostgresGenerationJobStore extends PostgresGenerationJobStoreSupport
        implements GenerationJobStore, GenerationJobSubmissionTransactions {

    private final PostgresGenerationJobReader reader;

    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationExecutionTransactionFence executionFence;
    private final GenerationPublicationCandidates publicationCandidates;

    /** Uses explicitly assembled current authority and execution fencing on each store transaction. */
    public PostgresGenerationJobStore(
            DataSource dataSource, GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence,
            GenerationPublicationCandidates publicationCandidates) {
        super(dataSource);
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
        this.publicationCandidates = Objects.requireNonNull(publicationCandidates, "publicationCandidates");
        this.reader = new PostgresGenerationJobReader(dataSource);
    }

    @Override
    public void createJob(
            GenerationJob job, GenerationDispatchSpec dispatchSpec,
            List<GenerationPage> pages) {
        List<GenerationPage> immutablePages =
                PostgresGenerationAdmission.validateNewJob(job, dispatchSpec, pages);
        inTransaction("create artifact build job", connection -> {
            createJob(transactionAuthority, connection, job, dispatchSpec, immutablePages);
            return null;
        });
    }

    @Override
    public SnapshotPinIntent reserveSnapshotPin(SnapshotPinReservation reservation) {
        return inTransaction("reserve source snapshot pin", connection ->
                PostgresSnapshotPinLedger.reserve(connection, transactionAuthority, reservation));
    }

    @Override
    public boolean markSnapshotPinPinned(String tenantId, String pinOwner, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(pinOwner, "pinOwner");
        Objects.requireNonNull(now, "now");
        return inTransaction("mark source snapshot pinned", connection ->
                PostgresSnapshotPinLedger.markPinned(connection, tenantId, pinOwner, now));
    }

    @Override
    public boolean beginSnapshotPinRelease(String tenantId, String pinOwner, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(pinOwner, "pinOwner");
        Objects.requireNonNull(now, "now");
        return inTransaction("begin source snapshot release", connection ->
                PostgresSnapshotPinLedger.beginRelease(connection, tenantId, pinOwner, now));
    }

    @Override
    public boolean markSnapshotPinReleased(String tenantId, String pinOwner, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(pinOwner, "pinOwner");
        Objects.requireNonNull(now, "now");
        return inTransaction("mark source snapshot released", connection ->
                PostgresSnapshotPinLedger.markReleased(connection, tenantId, pinOwner, now));
    }

    @Override
    public Optional<SnapshotPinIntent> findSnapshotPin(String tenantId, String pinOwner) {
        requireText(tenantId, "tenantId");
        requireText(pinOwner, "pinOwner");
        return inTransaction("find source snapshot pin", connection ->
                PostgresSnapshotPinLedger.find(connection, tenantId, pinOwner, false));
    }

    @Override
    public Optional<SnapshotPinIntent> findSnapshotPinForJob(String tenantId, String jobId) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        return inTransaction("find job source snapshot pin", connection ->
                PostgresSnapshotPinLedger.findForJob(connection, tenantId, jobId));
    }

    @Override
    public void createSnapshotPinnedJob(
            GenerationJob job, GenerationDispatchSpec dispatchSpec,
            List<GenerationPage> pages, String pinOwner) {
        requireText(pinOwner, "pinOwner");
        List<GenerationPage> immutablePages =
                PostgresGenerationAdmission.validateNewJob(job, dispatchSpec, pages);
        inTransaction("create snapshot-pinned artifact build job", connection -> {
            createJob(transactionAuthority, connection, job, dispatchSpec, immutablePages);
            PostgresSnapshotPinLedger.bindJob(connection, job, dispatchSpec, pinOwner);
            return null;
        });
    }

    @Override
    public List<SnapshotPinIntent> listSnapshotPinCleanupCandidates(
            int limit, Instant orphanGraceCutoff, Instant droppedGraceCutoff) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Objects.requireNonNull(orphanGraceCutoff, "orphanGraceCutoff");
        Objects.requireNonNull(droppedGraceCutoff, "droppedGraceCutoff");
        return inTransaction("list source snapshot pin cleanup", connection ->
                PostgresSnapshotPinLedger.cleanupCandidates(
                        connection, limit, orphanGraceCutoff, droppedGraceCutoff));
    }

    @Override
    public List<SnapshotPinIntent> listSnapshotPinsForAdoption(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return inTransaction("list legacy snapshot pin adoption", connection ->
                PostgresSnapshotPinLedger.listForAdoption(connection, limit));
    }

    @Override
    public void retainSnapshotPinForExecution(
            String tenantId, String artifactId, String generation,
            String sourceSnapshot, String executionId) {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        requireText(generation, "generation");
        requireText(sourceSnapshot, "sourceSnapshot");
        requireText(executionId, "executionId");
        inTransaction("retain artifact source snapshot for execution", connection -> {
            PostgresSnapshotPinLedger.retainForExecution(
                    connection, tenantId, artifactId, generation, sourceSnapshot, executionId);
            return null;
        });
    }

    private static void createJob(
            GenerationTransactionAuthority transactionAuthority, Connection connection, GenerationJob job,
            GenerationDispatchSpec dispatchSpec,
            List<GenerationPage> immutablePages) throws SQLException {
        transactionAuthority.requireCurrent(connection, dispatchSpec);
        Optional<GenerationJob> existing = findJob(
                connection, job.tenantId(), job.jobId(), true);
        if (existing.isPresent()) {
            validateIdempotentCreate(
                    connection, existing.orElseThrow(), job, dispatchSpec, immutablePages);
            return;
        }
        PostgresGenerationJobInsertion.insertJob(connection, job);
        PostgresGenerationExecutionRoutes.insertRoute(connection, job.tenantId(), job.jobId());
        insertDispatchSpec(connection, dispatchSpec);
        PostgresGenerationStageExchangeStore.insertGraphEdges(connection, dispatchSpec);
        for (GenerationPage page : immutablePages) {
            insertPage(connection, job.tenantId(), page, job.createdAt());
        }
        for (GenerationPage page : immutablePages) {
            insertSplits(connection, job.tenantId(), page);
            insertDependencies(connection, job.tenantId(), page);
        }
    }

    /**
     * Inserts the canonical job, graph and page allocations in a caller-owned PostgreSQL transaction.
     * Current authorization and idempotency use this same connection; the caller owns commit/rollback.
     */
    @Override
    public void createJob(Connection connection, GenerationJob job,
            GenerationDispatchSpec dispatchSpec, List<GenerationPage> pages) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        if (connection.getAutoCommit()) {
            throw new IllegalArgumentException("artifact creation requires an explicit caller-owned transaction");
        }
        createJob(transactionAuthority, connection, job, dispatchSpec,
                PostgresGenerationAdmission.validateNewJob(job, dispatchSpec, pages));
    }

    @Override
    public List<GenerationJobKey> listRunnableJobs(int limit, Instant now) {
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
            List<GenerationJobKey> jobs = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    jobs.add(new GenerationJobKey(
                            result.getString(1), result.getString(2)));
                }
            }
            return List.copyOf(jobs);
        } catch (SQLException e) {
            throw failure("list runnable artifact jobs", e);
        }
    }

    @Override
    public List<GenerationJobKey> listReadyForPublication(int limit, Instant now) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        Objects.requireNonNull(now, "now must not be null");
        try (Connection connection = dataSource.getConnection()) {
            return publicationCandidates.listReady(connection, limit, now);
        } catch (SQLException e) {
            throw failure("list publishable artifact jobs", e);
        }
    }

    @Override
    public Optional<GenerationLease> claimNextPage(
            String tenantId, String jobId,
            org.pangrova.execution.api.ownership.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now) {
        String leaseOwner = Objects.requireNonNull(executionAnchor, "executionAnchor").ownerId();
        if (!new GenerationJobKey(tenantId, jobId).executionId().equals(executionAnchor.executionId())) {
            throw new IllegalArgumentException("execution anchor belongs to another artifact job");
        }
        requireLeaseRequest(tenantId, jobId, leaseOwner, ownerEpoch, leaseDuration, now);
        return inTransaction("claim artifact build page", connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, tenantId, jobId,
                    org.pangrova.generation.api.execution.GenerationExecutionOwner.from(executionAnchor));
            PostgresGenerationStageExchangeStore.lockValidatedBarriers(connection, context);
            GenerationJob job = findJob(connection, tenantId, jobId, true).orElse(null);
            if (job == null || terminal(job.state())) {
                return Optional.empty();
            }
            PageRow page = selectClaimablePage(connection, new PostgresGenerationPageSelection.Selection(
                    tenantId, jobId, now, Optional.empty())).orElse(null);
            if (page == null) {
                return Optional.empty();
            }
            return PostgresGenerationPageClaim.claim(connection, page, executionAnchor, ownerEpoch, leaseDuration, now);
        });
    }

    @Override
    public boolean renewPage(
            GenerationLease lease, Duration leaseDuration, Instant now) {
        Objects.requireNonNull(lease, "lease must not be null");
        requirePositiveDuration(leaseDuration);
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("renew authorized artifact build page", connection -> {
            transactionAuthority.requireCurrent(connection, lease.tenantId(), lease.jobId());
            executionFence.requireAnchor(connection, lease.tenantId(), lease.jobId(), lease.executionAnchor());
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
    public boolean completePage(GenerationPageCompletion completion) {
        requireCompletion(completion);
        return inTransaction("complete artifact build page", connection ->
                PostgresGenerationCompletion.complete(transactionAuthority, executionFence, connection, completion));
    }

    @Override
    public boolean retryPage(
            GenerationLease lease, Instant nextAvailableAt,
            String failureReason, Instant now) {
        Objects.requireNonNull(lease, "lease must not be null");
        Objects.requireNonNull(nextAvailableAt, "nextAvailableAt must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("retry artifact build page", connection -> {
            transactionAuthority.requireCurrent(connection, lease.tenantId(), lease.jobId());
            executionFence.requireAnchor(connection, lease.tenantId(), lease.jobId(), lease.executionAnchor());
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
            GenerationLease lease, String failureReason, Instant now) {
        Objects.requireNonNull(lease, "lease must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("fail artifact build page", connection -> {
            transactionAuthority.requireCurrent(connection, lease.tenantId(), lease.jobId());
            executionFence.requireAnchor(connection, lease.tenantId(), lease.jobId(), lease.executionAnchor());
            return failPageInTransaction(connection, lease, failureReason, now);
        });
    }

    @Override
    public Optional<GenerationJob> findJob(String tenantId, String jobId) {
        return reader.findJob(tenantId, jobId);
    }

    @Override
    public Optional<GenerationJobKey> findExecutionJob(String executionId) {
        return reader.findExecutionJob(executionId);
    }

    @Override
    public Optional<GenerationDispatchSpec> findDispatchSpec(
            String tenantId, String jobId) {
        return reader.findDispatchSpec(tenantId, jobId);
    }

    @Override
    public GenerationDispatchSpec loadAuthorizedDispatchSpec(String tenantId, String jobId) {
        return inTransaction("load authorized artifact work", connection ->
                transactionAuthority.requireCurrent(connection, tenantId, jobId));
    }

    @Override
    public Optional<org.pangrova.generation.api.publication.GenerationManifest>
            findPublishedGeneration(
                    String tenantId, String artifactId,
                    String generation, String sourceSnapshot) {
        return reader.findPublishedGeneration(
                tenantId, artifactId, generation, sourceSnapshot);
    }

    @Override
    public List<GenerationOutput> loadCompleteOutputs(String tenantId, String jobId) {
        return reader.loadCompleteOutputs(tenantId, jobId);
    }

    @Override
    public List<GenerationOutput> loadDependencyOutputs(
            String tenantId, String jobId, List<String> dependencyPageIds) {
        return reader.loadDependencyOutputs(tenantId, jobId, dependencyPageIds);
    }

    @Override
    public List<GenerationOutput> loadPublishedOutputs(
            String tenantId, String jobId, Set<String> sourceObjectIds) {
        return reader.loadPublishedOutputs(tenantId, jobId, sourceObjectIds);
    }

    @Override
    public boolean matchesPublishedSplits(
            String tenantId, String jobId, Map<String, String> sourceSplitSha256) {
        return reader.matchesPublishedSplits(tenantId, jobId, sourceSplitSha256);
    }

    @Override
    public Map<String, org.pangrova.generation.api.page.GenerationSplit> loadPublishedSourceSplits(
            String tenantId, String jobId, Set<String> sourceObjectIds) {
        return reader.loadPublishedSourceSplits(tenantId, jobId, sourceObjectIds);
    }

    @Override
    public Optional<GenerationJob> claimReadyForPublication(
            String tenantId, String jobId,
            org.pangrova.execution.api.ownership.ExecutionAnchor executionAnchor, long ownerEpoch,
            Duration leaseDuration, Instant now) {
        String owner = Objects.requireNonNull(executionAnchor, "executionAnchor").ownerId();
        requireLeaseRequest(tenantId, jobId, owner, ownerEpoch, leaseDuration, now);
        return inTransaction("claim artifact publication", connection -> {
            transactionAuthority.requireCurrent(connection, tenantId, jobId);
            executionFence.requireAnchor(connection, tenantId, jobId, executionAnchor);
            GenerationJob job = findJob(connection, tenantId, jobId, true).orElse(null);
            if (job == null || job.state() != GenerationJobState.READY_TO_PUBLISH) {
                return Optional.empty();
            }
            PublicationLease current = loadPublicationLease(connection, tenantId, jobId);
            if (current.expiresAt() != null && current.expiresAt().isAfter(now)
                    && (!owner.equals(current.owner()) || ownerEpoch != current.ownerEpoch())
                    && publicationCandidates.hasLiveOwner(connection, tenantId, jobId)) {
                return Optional.empty();
            }
            long nextFence = Math.addExact(job.publicationFencingToken(), 1L);
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE artifact_build_jobs SET publication_owner=?, "
                            + "publication_owner_epoch=?, publication_fencing_token=?, "
                            + "publication_lease_expires_at=?, updated_at=?, "
                            + "publication_execution_owner_id=?, publication_execution_owner_incarnation=?, "
                            + "publication_execution_owner_token=? "
                            + "WHERE tenant_id=? AND job_id=? AND state='READY_TO_PUBLISH' "
                            + "AND publication_fencing_token=?")) {
                int parameter = 1;
                statement.setString(parameter++, owner);
                statement.setLong(parameter++, ownerEpoch);
                statement.setLong(parameter++, nextFence);
                setInstant(statement, parameter++, now.plus(leaseDuration));
                setInstant(statement, parameter++, now);
                statement.setString(parameter++, executionAnchor.ownerId());
                statement.setString(parameter++, executionAnchor.ownerIncarnation());
                statement.setLong(parameter++, executionAnchor.fencingToken());
                statement.setString(parameter++, tenantId);
                statement.setString(parameter++, jobId);
                statement.setLong(parameter, job.publicationFencingToken());
                if (statement.executeUpdate() != 1) {
                    throw new IllegalStateException("artifact publication fence was lost");
                }
            }
            return findJob(connection, tenantId, jobId, false);
        });
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
        return inTransaction("mark authorized artifact published", connection -> {
            transactionAuthority.requireCurrent(connection, tenantId, jobId);
            var candidate = findJob(connection, tenantId, jobId, false);
            if (candidate.isEmpty() || candidate.orElseThrow().state() != GenerationJobState.READY_TO_PUBLISH) {
                return false;
            }
            executionFence.requirePublication(connection, tenantId, jobId);
            try (var statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='PUBLISHED', "
                        + "publication_lease_expires_at=NULL, failure_reason=NULL, updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND state='READY_TO_PUBLISH' "
                        + "AND publication_owner=? AND publication_owner_epoch=? "
                        + "AND publication_fencing_token=? "
                        + "AND publication_lease_expires_at>=?")) {
                setInstant(statement, 1, now);
                statement.setString(2, tenantId);
                statement.setString(3, jobId);
                statement.setString(4, owner);
                statement.setLong(5, ownerEpoch);
                statement.setLong(6, publicationFencingToken);
                setInstant(statement, 7, now);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public boolean deferPublication(
            GenerationJob claim, String owner, long ownerEpoch,
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
        String normalizedFailure = normalizeFailure(failureReason);
        return inTransaction("defer authorized artifact publication", connection -> {
            transactionAuthority.requireCurrent(connection, tenantId, jobId);
            executionFence.requirePublication(connection, tenantId, jobId);
            try (var statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET publication_lease_expires_at=?, "
                        + "failure_reason=?, updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND state='READY_TO_PUBLISH' "
                        + "AND publication_owner=? AND publication_owner_epoch=? "
                        + "AND publication_fencing_token=?")) {
                setInstant(statement, 1, nextAvailableAt);
                statement.setString(2, normalizedFailure);
                setInstant(statement, 3, now);
                statement.setString(4, tenantId);
                statement.setString(5, jobId);
                statement.setString(6, owner);
                statement.setLong(7, ownerEpoch);
                statement.setLong(8, publicationFencingToken);
                return statement.executeUpdate() == 1;
            }
        });
    }

    @Override
    public boolean cancelJob(String tenantId, String jobId, String reason, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
        Objects.requireNonNull(now, "now");
        return inTransaction("cancel revoked artifact work", connection ->
                PostgresGenerationRevocation.cancel(connection, tenantId, jobId, normalizeFailure(reason), now));
    }

    @Override
    public boolean cancelPublicationClaim(
            GenerationJob claim, String owner, long ownerEpoch, String reason, Instant now) {
        Objects.requireNonNull(claim, "claim");
        requireText(owner, "owner");
        Objects.requireNonNull(now, "now");
        if (ownerEpoch <= 0 || claim.publicationFencingToken() <= 0) {
            throw new IllegalArgumentException("publication epoch and fence must be positive");
        }
        return inTransaction("cancel exact artifact publication claim", connection ->
                PostgresGenerationRevocation.cancelPublication(
                        connection, claim, owner, ownerEpoch, normalizeFailure(reason), now));
    }

    @Override
    public int cancelArtifactBuilds(String tenantId, String artifactId, Instant now) {
        requireText(tenantId, "tenantId");
        requireText(artifactId, "artifactId");
        Objects.requireNonNull(now, "now must not be null");
        return inTransaction("cancel artifact builds", connection ->
                PostgresGenerationRevocation.cancelArtifact(connection, tenantId, artifactId, now));
    }

}
