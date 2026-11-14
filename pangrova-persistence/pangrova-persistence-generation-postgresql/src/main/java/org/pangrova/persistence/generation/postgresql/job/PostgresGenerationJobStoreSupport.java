package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.generation.api.output.GenerationOutputFormat;

import org.pangrova.generation.api.output.GenerationOutputRole;

import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;

import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.codec.format.BuiltInGenerationOutputFormats;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;
import static org.pangrova.persistence.generation.postgresql.job.PostgresGenerationPageSelection.findPage;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
abstract class PostgresGenerationJobStoreSupport extends PostgresGenerationJobStoreMappings {

    PostgresGenerationJobStoreSupport(DataSource dataSource) {
        super(dataSource);
    }

    protected static void insertDispatchSpec(
            Connection connection, GenerationDispatchSpec spec) throws SQLException {
        byte[] graphBytes = GenerationStageGraphCodec.encode(spec.stageGraph());
        String sql = "INSERT INTO artifact_build_dispatch_specs "
                + "(tenant_id,job_id,stage_graph_bytes,stage_graph_sha256,principal,max_memory_bytes,"
                + "max_threads,max_spill_bytes,task_timeout_millis,authorization_scope_bytes) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, spec.tenantId());
            statement.setString(parameter++, spec.jobId());
            statement.setBytes(parameter++, graphBytes);
            statement.setString(parameter++, org.pangrova.common.util.DigestUtils.sha256Hex(graphBytes));
            statement.setString(parameter++, spec.principal());
            statement.setLong(parameter++, spec.maxMemoryBytes());
            statement.setInt(parameter++, spec.maxThreads());
            statement.setLong(parameter++, spec.maxSpillBytes());
            statement.setLong(parameter++, spec.taskTimeoutMillis());
            statement.setBytes(parameter, DeferredAuthorizationScopeCodec.encode(
                    spec.authorizationScope()));
            statement.executeUpdate();
        }
    }

    protected static void insertPage(
            Connection connection, String tenantId,
            GenerationPage page, Instant createdAt) throws SQLException {
        String sql = "INSERT INTO artifact_build_pages "
                + "(tenant_id,job_id,page_id,stage_id,partition_id,"
                + "page_ordinal,page_kind,state,next_available_at,max_attempts,created_at,updated_at) "
                + "VALUES (?,?,?,?,?,?,?,'PENDING',?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, tenantId);
            statement.setString(parameter++, page.jobId());
            statement.setString(parameter++, page.pageId());
            statement.setInt(parameter++, page.stageId());
            statement.setInt(parameter++, page.partitionId());
            statement.setInt(parameter++, page.pageOrdinal());
            statement.setString(parameter++, page.kind().name());
            setInstant(statement, parameter++, createdAt);
            statement.setInt(parameter++, page.maxAttempts());
            setInstant(statement, parameter++, createdAt);
            setInstant(statement, parameter, createdAt);
            statement.executeUpdate();
        }
    }

    protected static void insertSplits(
            Connection connection, String tenantId, GenerationPage page) throws SQLException {
        String sql = "INSERT INTO artifact_build_page_sources "
                + "(tenant_id,job_id,page_id,source_ordinal,source_object_id,"
                + "split_bytes,split_sha256) VALUES (?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < page.splits().size(); i++) {
                GenerationSplit split = page.splits().get(i);
                statement.setString(1, tenantId);
                statement.setString(2, page.jobId());
                statement.setString(3, page.pageId());
                statement.setInt(4, i);
                statement.setString(5, split.dataObjectId());
                statement.setBytes(6, split.canonicalSplitBytes());
                statement.setString(7, split.splitSha256()
                        .toLowerCase(java.util.Locale.ROOT));
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    protected static void insertDependencies(
            Connection connection, String tenantId, GenerationPage page) throws SQLException {
        if (page.dependencyPageIds().isEmpty()) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO artifact_build_page_dependencies "
                        + "(tenant_id,job_id,page_id,dependency_ordinal,dependency_page_id) "
                        + "VALUES (?,?,?,?,?)")) {
            for (int index = 0; index < page.dependencyPageIds().size(); index++) {
                statement.setString(1, tenantId);
                statement.setString(2, page.jobId());
                statement.setString(3, page.pageId());
                statement.setInt(4, index);
                statement.setString(5, page.dependencyPageIds().get(index));
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    protected static void markJobRunning(
            Connection connection, String tenantId, String jobId, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='RUNNING', updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND state='QUEUED'")) {
            setInstant(statement, 1, now);
            statement.setString(2, tenantId);
            statement.setString(3, jobId);
            statement.executeUpdate();
        }
    }

    protected static void advanceCompletionBarrier(
            Connection connection, String tenantId, String jobId, Instant now)
            throws SQLException {
        GenerationJob job = findJob(connection, tenantId, jobId, true).orElseThrow();
        if (job.state() != GenerationJobState.RUNNING
                && job.state() != GenerationJobState.QUEUED) {
            throw new IllegalStateException("artifact job cannot accept completed pages");
        }
        int completed = Math.incrementExact(job.completedPages());
        GenerationJobState next = completed == job.expectedPages()
                ? GenerationJobState.READY_TO_PUBLISH : GenerationJobState.RUNNING;
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET completed_pages=?, state=?, updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND completed_pages=? "
                        + "AND state IN ('QUEUED','RUNNING')")) {
            statement.setInt(1, completed);
            statement.setString(2, next.name());
            setInstant(statement, 3, now);
            statement.setString(4, tenantId);
            statement.setString(5, jobId);
            statement.setInt(6, job.completedPages());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("artifact all-pages barrier fence was lost");
            }
        }
    }

    protected static void insertOutput(
            Connection connection, String tenantId, String jobId, String pageId,
            GenerationOutput output) throws SQLException {
        String sql = "INSERT INTO artifact_build_outputs "
                + "(tenant_id,job_id,page_id,object_id,opaque_uri,"
                + "object_role,source_object_id,object_ordinal,object_scope,size_bytes,row_count,"
                + "checksum_sha256,encryption_key_ref) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, tenantId);
            statement.setString(parameter++, jobId);
            statement.setString(parameter++, pageId);
            statement.setString(parameter++, output.objectId());
            statement.setString(parameter++, output.opaqueUri());
            statement.setString(parameter++, output.objectRole());
            statement.setString(parameter++, output.sourceObjectId());
            statement.setInt(parameter++, output.ordinal());
            statement.setString(parameter++, output.scope().name());
            statement.setLong(parameter++, output.sizeBytes());
            statement.setLong(parameter++, output.rowCount());
            statement.setString(parameter++, output.checksumSha256()
                    .toLowerCase(java.util.Locale.ROOT));
            statement.setString(parameter, output.encryptionKeyRef());
            statement.executeUpdate();
        }
    }

    protected static void validateOutputCoverage(
            GenerationJob job, PageRow page,
            List<String> expectedSources, List<GenerationOutput> outputs) {
        if (page.kind() == GenerationPageKind.RELATIONAL_COMPUTE) {
            if (!outputs.isEmpty()) {
                throw new IllegalArgumentException("Shuffle producer completion cannot publish artifact objects");
            }
            return;
        }
        Map<String, GenerationOutputRole> roles =
                BuiltInGenerationOutputFormats.require(job.formatId(), job.artifactType())
                        .rolesFor(page.kind());
        var contract = BuiltInGenerationOutputFormats.require(job.formatId(), job.artifactType());
        if (roles.isEmpty() || outputs.size() != roles.size()) {
            throw new IllegalArgumentException(
                    "artifact outputs differ from the work-page role contract");
        }
        Set<String> expected = Set.copyOf(expectedSources);
        Set<String> ids = new HashSet<>();
        Set<String> uris = new HashSet<>();
        Set<String> actualRoles = new HashSet<>();
        for (GenerationOutput output : outputs) {
            GenerationOutputRole role = roles.get(output.objectRole());
            if (!registerOutput(output, role, ids, uris, actualRoles)) {
                throw new IllegalArgumentException("artifact outputs contain duplicate identity");
            }
            validateOutputScope(job, page, expected, output);
            validateEmptyOutput(job, contract, output);
        }
        if (!actualRoles.equals(roles.keySet())) {
            throw new IllegalArgumentException("artifact output roles are incomplete");
        }
    }

    private static void validateEmptyOutput(GenerationJob job,
            GenerationOutputFormat contract, GenerationOutput output) {
        if (output.rowCount() == 0
                && (job.emptyPartitionPolicy()
                        != org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                        || !contract.supportsEmptyPartitions())) {
            throw new IllegalArgumentException("zero-row artifact output is forbidden by job or format policy");
        }
    }

    private static boolean registerOutput(
            GenerationOutput output,
            GenerationOutputRole role,
            Set<String> ids,
            Set<String> uris,
            Set<String> actualRoles) {
        return ids.add(output.objectId()) && uris.add(output.opaqueUri())
                && actualRoles.add(output.objectRole()) && role != null
                && output.ordinal() == role.ordinal() && output.scope() == role.scope();
    }

    protected static void validateOutputScope(
            GenerationJob job, PageRow page, Set<String> expectedSources,
            GenerationOutput output) {
        boolean valid = switch (output.scope()) {
            case SOURCE_OBJECT -> expectedSources.contains(output.sourceObjectId());
            case WORK_UNIT -> output.sourceObjectId().equals(page.pageId());
            case GENERATION -> output.sourceObjectId().equals(job.stagingGeneration());
        };
        if (!valid) {
            throw new IllegalArgumentException(
                    "artifact output scope identity differs from its work page");
        }
    }

    protected static long countSucceededPages(
            Connection connection, String tenantId, String jobId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COUNT(*) FROM artifact_build_pages "
                        + "WHERE tenant_id=? AND job_id=? AND state='SUCCEEDED'")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    protected static boolean failPageInTransaction(
            Connection connection, GenerationLease lease,
            String failureReason, Instant now) throws SQLException {
        PageRow page = findPage(
                connection, lease.tenantId(), lease.jobId(), lease.pageId(), true).orElse(null);
        if (!matchesLiveLease(page, lease, now)) {
            return false;
        }
        String normalized = normalizeFailure(failureReason);
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_pages SET state='FAILED', lease_owner=NULL, "
                        + "lease_expires_at=NULL, failure_reason=?, updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND page_id=? AND state='LEASED' "
                        + "AND fencing_token=? AND owner_epoch=?")) {
            statement.setString(1, normalized);
            setInstant(statement, 2, now);
            statement.setString(3, lease.tenantId());
            statement.setString(4, lease.jobId());
            statement.setString(5, lease.pageId());
            statement.setLong(6, lease.fencingToken());
            statement.setLong(7, lease.ownerEpoch());
            if (statement.executeUpdate() != 1) {
                return false;
            }
        }
        failJob(connection, lease.tenantId(), lease.jobId(), normalized, now);
        return true;
    }

    protected static void failExhaustedPage(
            Connection connection, PageRow page, Instant now) throws SQLException {
        String reason = "artifact work page exhausted its retry budget";
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_pages SET state='FAILED', lease_owner=NULL, "
                        + "lease_expires_at=NULL, failure_reason=?, updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? AND page_id=? AND attempt_id=?")) {
            statement.setString(1, reason);
            setInstant(statement, 2, now);
            statement.setString(3, page.tenantId());
            statement.setString(4, page.jobId());
            statement.setString(5, page.pageId());
            statement.setInt(6, page.attemptId());
            statement.executeUpdate();
        }
        failJob(connection, page.tenantId(), page.jobId(), reason, now);
    }

    protected static void failJob(
            Connection connection, String tenantId, String jobId,
            String reason, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='FAILED', failure_reason=?, updated_at=? "
                        + "WHERE tenant_id=? AND job_id=? "
                        + "AND state IN ('QUEUED','RUNNING')")) {
            statement.setString(1, reason);
            setInstant(statement, 2, now);
            statement.setString(3, tenantId);
            statement.setString(4, jobId);
            statement.executeUpdate();
        }
    }

    protected static boolean matchesLiveLease(
            PageRow page, GenerationLease lease, Instant now) {
        return page != null && "LEASED".equals(page.state())
                && page.tenantId().equals(lease.tenantId())
                && page.fencingToken() == lease.fencingToken()
                && page.ownerEpoch() == lease.ownerEpoch()
                && Objects.equals(page.leaseOwner(), lease.leaseOwner())
                && Objects.equals(page.executionOwner(),
                        org.pangrova.generation.api.execution.GenerationExecutionOwner.from(lease.executionAnchor()))
                && page.leaseExpiresAt() != null && !page.leaseExpiresAt().isBefore(now);
    }

    protected static void validateIdempotentCreate(
            Connection connection, GenerationJob existing,
            GenerationJob requested, GenerationDispatchSpec dispatchSpec,
            List<GenerationPage> pages) throws SQLException {
        if (!PostgresGenerationAdmission.sameIdentity(existing, requested)) {
            throw new IllegalStateException("artifact job id is already bound to different work");
        }
        if (!INPUTS.findDispatchSpec(
                connection, existing.tenantId(), existing.jobId()).orElseThrow()
                .equals(dispatchSpec)) {
            throw new IllegalStateException(
                    "artifact job retry supplied a different dispatch template");
        }
        List<GenerationPage> stored = INPUTS.loadPages(
                connection, existing.tenantId(), existing.jobId());
        if (!stored.equals(pages)) {
            throw new IllegalStateException("artifact job retry supplied different work pages");
        }
    }

}
