package org.pangrova.metadata.artifact.jdbc.build;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildLease;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.contract.ArtifactBuildPageKind;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildSplit;
import org.pangrova.metadata.artifact.contract.ArtifactFormatContracts;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.pangrova.metadata.store.JdbcSupport.setInstant;
import static org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildPageSelection.findPage;

/** PostgreSQL-backed implementation of the durable artifact-build state machine. */
abstract class JdbcArtifactBuildStoreSupport extends JdbcArtifactBuildStoreMappings {


    JdbcArtifactBuildStoreSupport(DataSource dataSource) {
        super(dataSource);
    }

    protected static void insertDispatchSpec(
            Connection connection, ArtifactBuildDispatchSpec spec) throws SQLException {
        String sql = "INSERT INTO artifact_build_dispatch_specs "
                + "(tenant_id,job_id,stage_graph_bytes,stage_graph_sha256,principal,max_memory_bytes,"
                + "max_threads,max_spill_bytes,task_timeout_millis,authorization_scope_bytes) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, spec.tenantId());
            statement.setString(parameter++, spec.jobId());
            statement.setBytes(parameter++, spec.stageGraph().canonicalBytes());
            statement.setString(parameter++, spec.stageGraph().sha256());
            statement.setString(parameter++, spec.principal());
            statement.setLong(parameter++, spec.maxMemoryBytes());
            statement.setInt(parameter++, spec.maxThreads());
            statement.setLong(parameter++, spec.maxSpillBytes());
            statement.setLong(parameter++, spec.taskTimeoutMillis());
            statement.setBytes(parameter, org.pangrova.security.authz.DeferredAuthorizationScopeCodec.encode(
                    spec.authorizationScope()));
            statement.executeUpdate();
        }
    }

    protected static void insertPage(
            Connection connection, String tenantId,
            ArtifactBuildPage page, Instant createdAt) throws SQLException {
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
            Connection connection, String tenantId, ArtifactBuildPage page) throws SQLException {
        String sql = "INSERT INTO artifact_build_page_sources "
                + "(tenant_id,job_id,page_id,source_ordinal,source_object_id,"
                + "split_bytes,split_sha256) VALUES (?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < page.splits().size(); i++) {
                ArtifactBuildSplit split = page.splits().get(i);
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
            Connection connection, String tenantId, ArtifactBuildPage page) throws SQLException {
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


    protected static List<ArtifactBuildSplit> loadSplits(
            Connection connection, String tenantId, String jobId, String pageId)
            throws SQLException {
        List<ArtifactBuildSplit> splits = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT source_object_id,split_bytes,split_sha256 "
                        + "FROM artifact_build_page_sources "
                        + "WHERE tenant_id=? AND job_id=? AND page_id=? "
                        + "ORDER BY source_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setString(3, pageId);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    byte[] bytes = result.getBytes(2);
                    String checksum = result.getString(3);
                    if (bytes == null || bytes.length == 0 || checksum == null) {
                        throw new IllegalStateException(
                                "artifact page predates replayable canonical split storage");
                    }
                    splits.add(new ArtifactBuildSplit(
                            result.getString(1), Base64.getEncoder().encodeToString(bytes),
                            checksum));
                }
            }
        }
        return List.copyOf(splits);
    }

    protected static List<String> loadDependencies(
            Connection connection, String tenantId, String jobId, String pageId)
            throws SQLException {
        List<String> dependencies = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT dependency_page_id FROM artifact_build_page_dependencies "
                        + "WHERE tenant_id=? AND job_id=? AND page_id=? "
                        + "ORDER BY dependency_ordinal")) {
            statement.setString(1, tenantId);
            statement.setString(2, jobId);
            statement.setString(3, pageId);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    dependencies.add(result.getString(1));
                }
            }
        }
        return List.copyOf(dependencies);
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
        ArtifactBuildJob job = findJob(connection, tenantId, jobId, true).orElseThrow();
        if (job.state() != ArtifactBuildJobState.RUNNING
                && job.state() != ArtifactBuildJobState.QUEUED) {
            throw new IllegalStateException("artifact job cannot accept completed pages");
        }
        int completed = Math.incrementExact(job.completedPages());
        ArtifactBuildJobState next = completed == job.expectedPages()
                ? ArtifactBuildJobState.READY_TO_PUBLISH : ArtifactBuildJobState.RUNNING;
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
            ArtifactBuildOutput output) throws SQLException {
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
            ArtifactBuildJob job, PageRow page,
            List<String> expectedSources, List<ArtifactBuildOutput> outputs) {
        if (page.kind() == ArtifactBuildPageKind.RELATIONAL_COMPUTE) {
            if (!outputs.isEmpty()) {
                throw new IllegalArgumentException("Shuffle producer completion cannot publish artifact objects");
            }
            return;
        }
        Map<String, ArtifactFormatContracts.RoleContract> roles =
                ArtifactFormatContracts.require(job.formatId(), job.artifactType())
                        .rolesFor(page.kind());
        var contract = ArtifactFormatContracts.require(job.formatId(), job.artifactType());
        if (roles.isEmpty() || outputs.size() != roles.size()) {
            throw new IllegalArgumentException(
                    "artifact outputs differ from the work-page role contract");
        }
        Set<String> expected = Set.copyOf(expectedSources);
        Set<String> ids = new HashSet<>();
        Set<String> uris = new HashSet<>();
        Set<String> actualRoles = new HashSet<>();
        for (ArtifactBuildOutput output : outputs) {
            ArtifactFormatContracts.RoleContract role = roles.get(output.objectRole());
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

    private static void validateEmptyOutput(ArtifactBuildJob job,
            ArtifactFormatContracts.FormatContract contract, ArtifactBuildOutput output) {
        if (output.rowCount() == 0
                && (job.emptyPartitionPolicy()
                        != org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                        || !contract.supportsEmptyPartitions())) {
            throw new IllegalArgumentException("zero-row artifact output is forbidden by job or format policy");
        }
    }

    private static boolean registerOutput(
            ArtifactBuildOutput output,
            ArtifactFormatContracts.RoleContract role,
            Set<String> ids,
            Set<String> uris,
            Set<String> actualRoles) {
        return ids.add(output.objectId()) && uris.add(output.opaqueUri())
                && actualRoles.add(output.objectRole()) && role != null
                && output.ordinal() == role.ordinal() && output.scope() == role.scope();
    }

    protected static void validateOutputScope(
            ArtifactBuildJob job, PageRow page, Set<String> expectedSources,
            ArtifactBuildOutput output) {
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
            Connection connection, ArtifactBuildLease lease,
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
            PageRow page, ArtifactBuildLease lease, Instant now) {
        return page != null && "LEASED".equals(page.state())
                && page.tenantId().equals(lease.tenantId())
                && page.fencingToken() == lease.fencingToken()
                && page.ownerEpoch() == lease.ownerEpoch()
                && Objects.equals(page.leaseOwner(), lease.leaseOwner())
                && Objects.equals(page.executionOwner(),
                        org.pangrova.metadata.artifact.build.ArtifactExecutionOwner.from(lease.executionAnchor()))
                && page.leaseExpiresAt() != null && !page.leaseExpiresAt().isBefore(now);
    }

    protected static void validateIdempotentCreate(
            Connection connection, ArtifactBuildJob existing,
            ArtifactBuildJob requested, ArtifactBuildDispatchSpec dispatchSpec,
            List<ArtifactBuildPage> pages) throws SQLException {
        if (!JdbcArtifactBuildPageLoader.sameIdentity(existing, requested)) {
            throw new IllegalStateException("artifact job id is already bound to different work");
        }
        if (!findDispatchSpec(
                connection, existing.tenantId(), existing.jobId()).orElseThrow()
                .equals(dispatchSpec)) {
            throw new IllegalStateException(
                    "artifact job retry supplied a different dispatch template");
        }
        List<ArtifactBuildPage> stored = JdbcArtifactBuildPageLoader.loadPages(
                connection, existing.tenantId(), existing.jobId());
        if (!stored.equals(pages)) {
            throw new IllegalStateException("artifact job retry supplied different work pages");
        }
    }


}
