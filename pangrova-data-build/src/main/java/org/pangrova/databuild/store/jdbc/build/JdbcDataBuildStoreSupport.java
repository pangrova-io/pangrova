package org.pangrova.databuild.store.jdbc.build;

import org.pangrova.databuild.protocol.DataBuildStageGraphCodec;
import org.pangrova.databuild.store.jdbc.query.JdbcDataBuildQueries;

import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildLease;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.format.DataBuildOutputFormatContracts;

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

import static org.pangrova.common.jdbc.JdbcSupport.setInstant;
import static org.pangrova.databuild.store.jdbc.build.JdbcDataBuildPageSelection.findPage;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
abstract class JdbcDataBuildStoreSupport extends JdbcDataBuildStoreMappings {

    JdbcDataBuildStoreSupport(DataSource dataSource) {
        super(dataSource);
    }

    protected static void insertDispatchSpec(
            Connection connection, DataBuildDispatchSpec spec) throws SQLException {
        byte[] graphBytes = DataBuildStageGraphCodec.encode(spec.stageGraph());
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
            statement.setBytes(parameter, org.pangrova.security.protocol.DeferredAuthorizationScopeCodec.encode(
                    spec.authorizationScope()));
            statement.executeUpdate();
        }
    }

    protected static void insertPage(
            Connection connection, String tenantId,
            DataBuildPage page, Instant createdAt) throws SQLException {
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
            Connection connection, String tenantId, DataBuildPage page) throws SQLException {
        String sql = "INSERT INTO artifact_build_page_sources "
                + "(tenant_id,job_id,page_id,source_ordinal,source_object_id,"
                + "split_bytes,split_sha256) VALUES (?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < page.splits().size(); i++) {
                DataBuildSplit split = page.splits().get(i);
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
            Connection connection, String tenantId, DataBuildPage page) throws SQLException {
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
        DataBuildJob job = findJob(connection, tenantId, jobId, true).orElseThrow();
        if (job.state() != DataBuildJobState.RUNNING
                && job.state() != DataBuildJobState.QUEUED) {
            throw new IllegalStateException("artifact job cannot accept completed pages");
        }
        int completed = Math.incrementExact(job.completedPages());
        DataBuildJobState next = completed == job.expectedPages()
                ? DataBuildJobState.READY_TO_PUBLISH : DataBuildJobState.RUNNING;
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
            DataBuildOutput output) throws SQLException {
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
            DataBuildJob job, PageRow page,
            List<String> expectedSources, List<DataBuildOutput> outputs) {
        if (page.kind() == DataBuildPageKind.RELATIONAL_COMPUTE) {
            if (!outputs.isEmpty()) {
                throw new IllegalArgumentException("Shuffle producer completion cannot publish artifact objects");
            }
            return;
        }
        Map<String, DataBuildOutputFormatContracts.RoleContract> roles =
                DataBuildOutputFormatContracts.require(job.formatId(), job.artifactType())
                        .rolesFor(page.kind());
        var contract = DataBuildOutputFormatContracts.require(job.formatId(), job.artifactType());
        if (roles.isEmpty() || outputs.size() != roles.size()) {
            throw new IllegalArgumentException(
                    "artifact outputs differ from the work-page role contract");
        }
        Set<String> expected = Set.copyOf(expectedSources);
        Set<String> ids = new HashSet<>();
        Set<String> uris = new HashSet<>();
        Set<String> actualRoles = new HashSet<>();
        for (DataBuildOutput output : outputs) {
            DataBuildOutputFormatContracts.RoleContract role = roles.get(output.objectRole());
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

    private static void validateEmptyOutput(DataBuildJob job,
            DataBuildOutputFormatContracts.FormatContract contract, DataBuildOutput output) {
        if (output.rowCount() == 0
                && (job.emptyPartitionPolicy()
                        != org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                        || !contract.supportsEmptyPartitions())) {
            throw new IllegalArgumentException("zero-row artifact output is forbidden by job or format policy");
        }
    }

    private static boolean registerOutput(
            DataBuildOutput output,
            DataBuildOutputFormatContracts.RoleContract role,
            Set<String> ids,
            Set<String> uris,
            Set<String> actualRoles) {
        return ids.add(output.objectId()) && uris.add(output.opaqueUri())
                && actualRoles.add(output.objectRole()) && role != null
                && output.ordinal() == role.ordinal() && output.scope() == role.scope();
    }

    protected static void validateOutputScope(
            DataBuildJob job, PageRow page, Set<String> expectedSources,
            DataBuildOutput output) {
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
            Connection connection, DataBuildLease lease,
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
            PageRow page, DataBuildLease lease, Instant now) {
        return page != null && "LEASED".equals(page.state())
                && page.tenantId().equals(lease.tenantId())
                && page.fencingToken() == lease.fencingToken()
                && page.ownerEpoch() == lease.ownerEpoch()
                && Objects.equals(page.leaseOwner(), lease.leaseOwner())
                && Objects.equals(page.executionOwner(),
                        org.pangrova.databuild.api.execution.DataBuildExecutionOwner.from(lease.executionAnchor()))
                && page.leaseExpiresAt() != null && !page.leaseExpiresAt().isBefore(now);
    }

    protected static void validateIdempotentCreate(
            Connection connection, DataBuildJob existing,
            DataBuildJob requested, DataBuildDispatchSpec dispatchSpec,
            List<DataBuildPage> pages) throws SQLException {
        if (!JdbcDataBuildAdmission.sameIdentity(existing, requested)) {
            throw new IllegalStateException("artifact job id is already bound to different work");
        }
        if (!JdbcDataBuildQueries.findDispatchSpec(
                connection, existing.tenantId(), existing.jobId()).orElseThrow()
                .equals(dispatchSpec)) {
            throw new IllegalStateException(
                    "artifact job retry supplied a different dispatch template");
        }
        List<DataBuildPage> stored = JdbcDataBuildQueries.loadPages(
                connection, existing.tenantId(), existing.jobId());
        if (!stored.equals(pages)) {
            throw new IllegalStateException("artifact job retry supplied different work pages");
        }
    }

}
