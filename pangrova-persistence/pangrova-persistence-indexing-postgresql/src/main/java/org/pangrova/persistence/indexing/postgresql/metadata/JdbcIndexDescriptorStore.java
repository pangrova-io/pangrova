package org.pangrova.persistence.indexing.postgresql.metadata;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import org.pangrova.persistence.jdbc.api.security.policy.CurrentTableAuthorization;

import org.pangrova.persistence.indexing.api.metadata.IndexBuildGenerations.IndexBuildAbandonment;
import org.pangrova.persistence.indexing.api.metadata.IndexBuildGenerations.IndexBuildCandidate;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.indexing.api.exception.IndexAlreadyExistsException;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.exception.AccessDeniedException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.bindDescriptor;
import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.mapDescriptor;
import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.setJson;
import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.toJson;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/**
 * JDBC operations for {@link IndexDescriptor} CRUD, schema evolution,
 * and state queries.
 */
final class JdbcIndexDescriptorStore extends JdbcStoreSupport {
    private static final int BUILD_PRINCIPAL_PARAMETER = 17;
    private static final int BUILD_SCOPE_PARAMETER = 18;
    private static final int SOURCE_SCHEMA_FINGERPRINT_PARAMETER = 19;

    private final CurrentTableAuthorization authorization;

    JdbcIndexDescriptorStore(DataSource dataSource, CurrentTableAuthorization authorization) {
        super(dataSource);
        this.authorization = java.util.Objects.requireNonNull(authorization, "authorization");
    }

    void registerIndex(IndexDescriptor descriptor) {
        registerIndex(descriptor, null, null);
    }

    void registerIndexWithBuildAuthority(IndexDescriptor descriptor, String principal) {
        if (principal == null || principal.isBlank()) {
            throw new AccessDeniedException("Scalar index build requires its authenticated subject");
        }
        registerIndex(descriptor, principal, exactScope(descriptor));
    }

    private void registerIndex(IndexDescriptor descriptor, String principal, DeferredAuthorizationScope scope) {
        String sql = "INSERT INTO index_descriptors "
                + "(index_id, index_name, catalog_name, schema_name, table_name, "
                + "column_names, index_type, properties, state, stale_reason, "
                + "generation, build_snapshot_id, last_indexed_snapshot_id, created_at, updated_at, "
                + "tenant_id, build_principal, build_authorization_scope, source_schema_fingerprint) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement lock = connection.prepareStatement(
                    "SELECT pangrova_assert_index_schema_scope_available(?,?,?)");
                 PreparedStatement ps = connection.prepareStatement(sql)) {
                lock.setString(1, descriptor.catalogName());
                lock.setString(2, descriptor.schemaName());
                lock.setString(3, descriptor.tableName());
                lock.executeQuery().close();
                bindDescriptor(ps, descriptor);
                ps.setString(BUILD_PRINCIPAL_PARAMETER, principal);
                ps.setBytes(BUILD_SCOPE_PARAMETER,
                        scope == null ? null : DeferredAuthorizationScopeCodec.encode(scope));
                ps.setString(SOURCE_SCHEMA_FINGERPRINT_PARAMETER, descriptor.sourceSchemaFingerprint());
                ps.executeUpdate();
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            MetastoreException e = new MetastoreException("Failed to register index", failure);
            if (isUniqueConstraintViolation(e)) {
                throw new IndexAlreadyExistsException(descriptor.indexName(), descriptor.tableName());
            }
            throw e;
        }
    }

    String loadAuthorizedBuildPrincipal(IndexDescriptor descriptor) {
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                String principal = readAndAuthorizeBuildPrincipal(connection, descriptor, true);
                connection.commit();
                return principal;
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to authorize scalar index build", failure);
        }
    }

    String loadDroppedIndexCleanupPrincipal(IndexDescriptor descriptor) {
        if (descriptor.state() != IndexState.DROPPED) {
            throw new AccessDeniedException("Cleanup authority requires a DROPPED index");
        }
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                String principal = readAndAuthorizeBuildPrincipal(connection, descriptor, false);
                connection.commit();
                return principal;
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to authorize scalar index cleanup", failure);
        }
    }

    void withIndexLifecycleLock(String indexId, Runnable action) {
        if (indexId == null || indexId.isBlank() || action == null) {
            throw new IllegalArgumentException("Index lifecycle lock requires an index ID and action");
        }
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement lock = connection.prepareStatement(
                    "SELECT pg_advisory_xact_lock(17001, hashtext(?))")) {
                lock.setString(1, indexId);
                lock.executeQuery().close();
                action.run();
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to hold index lifecycle lock", failure);
        }
    }

    private String readAndAuthorizeBuildPrincipal(Connection connection,
                                                          IndexDescriptor descriptor,
                                                          boolean requireCurrentGrant)
            throws SQLException {
        String sql = "SELECT tenant_id, catalog_name, schema_name, table_name, column_names, "
                + "index_type, state, build_principal, build_authorization_scope "
                + "FROM index_descriptors WHERE index_id=? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, descriptor.indexId());
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new AccessDeniedException("Scalar index no longer exists");
                }
                String principal = row.getString("build_principal");
                byte[] bytes = row.getBytes("build_authorization_scope");
                if (principal == null || principal.isBlank() || bytes == null) {
                    throw new AccessDeniedException("Scalar index has no durable build authority");
                }
                DeferredAuthorizationScope actual = decodeScope(bytes);
                if (!descriptor.tenantId().equals(row.getString("tenant_id"))
                        || !descriptor.catalogName().equals(row.getString("catalog_name"))
                        || !descriptor.schemaName().equals(row.getString("schema_name"))
                        || !descriptor.tableName().equals(row.getString("table_name"))
                        || !descriptor.columnNames().equals(
                                IndexMetadataMapper.fromJsonList(row.getString("column_names")))
                        || !descriptor.indexType().name().equals(row.getString("index_type"))
                        || !actual.equals(exactScope(descriptor))) {
                    throw new AccessDeniedException("Scalar index differs from its durable build authority");
                }
                if (!requireCurrentGrant && !IndexState.DROPPED.name().equals(row.getString("state"))) {
                    throw new AccessDeniedException("Index is no longer DROPPED for cleanup");
                }
                if (requireCurrentGrant) {
                    JdbcTenantStore.requireDeferredTenant(connection, principal, descriptor.tenantId(), actual);
                    authorization.requireCurrent(
                            connection, principal, actual);
                }
                return principal;
            }
        }
    }

    void requireCurrentScalarPublicationAuthority(Connection connection, String indexId)
            throws SQLException {
        IndexDescriptor descriptor;
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM index_descriptors WHERE index_id=? FOR UPDATE")) {
            statement.setString(1, indexId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new AccessDeniedException("Index no longer exists before publication");
                }
                descriptor = mapDescriptor(row);
            }
        }
        if (descriptor.indexType() == IndexType.VECTOR || descriptor.indexType() == IndexType.INVERTED) {
            return;
        }
        readAndAuthorizeBuildPrincipal(connection, descriptor, true);
    }

    private static DeferredAuthorizationScope decodeScope(byte[] bytes) {
        try {
            return DeferredAuthorizationScopeCodec.decode(bytes);
        } catch (IllegalArgumentException corrupt) {
            throw new AccessDeniedException("Scalar index build authority is corrupt");
        }
    }

    private static DeferredAuthorizationScope exactScope(IndexDescriptor descriptor) {
        var table = new QualifiedTableName(
                descriptor.catalogName(), descriptor.schemaName(), descriptor.tableName());
        return DeferredAuthorizationScope.raw(table, Set.copyOf(descriptor.columnNames()), table);
    }

    boolean isIndexSnapshotBuildable(String indexId, String snapshotId) {
        if (snapshotId == null || snapshotId.isBlank()) {
            return false;
        }
        String sql = "SELECT 1 FROM index_descriptors WHERE index_id=? "
                + "AND state='BUILDING' AND build_snapshot_id=? "
                + "UNION ALL SELECT 1 FROM index_generation_builds WHERE index_id=? "
                + "AND state='BUILDING' AND source_snapshot=? LIMIT 1";
        return executeQuery(sql, statement -> {
            statement.setString(1, indexId);
            statement.setString(2, snapshotId);
            statement.setString(3, indexId);
            statement.setString(4, snapshotId);
        }, ResultSet::next);
    }

    void updateIndexState(String indexId, String newState, Instant timestamp) {
        executeUpdate("UPDATE index_descriptors SET state=?, updated_at=? WHERE index_id=?",
                ps -> {
                    ps.setString(1, newState);
                    setInstant(ps, 2, timestamp);
                    ps.setString(3, indexId);
                });
    }

    void updateIndexState(String indexId, String newState, Instant timestamp, String staleReason) {
        executeUpdate("UPDATE index_descriptors SET state=?, updated_at=?, stale_reason=? WHERE index_id=?",
                ps -> {
                    ps.setString(1, newState);
                    setInstant(ps, 2, timestamp);
                    ps.setString(3, "STALE".equals(newState) ? staleReason : null);
                    ps.setString(4, indexId);
                });
    }

    boolean updateIndexState(String indexId, String newState,
                             Instant timestamp, Instant expectedUpdatedAt) {
        String sql = "UPDATE index_descriptors SET state=?, updated_at=? "
                + "WHERE index_id=? AND updated_at=?";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newState);
            setInstant(ps, 2, timestamp);
            ps.setString(3, indexId);
            setInstant(ps, 4, expectedUpdatedAt);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new MetastoreException("Failed to update index state with optimistic lock", e);
        }
    }

    boolean beginIndexGeneration(
            String indexId,
            int newGeneration,
            String buildSnapshotId,
            Instant timestamp,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration) {
        return beginIndexGeneration(indexId, newGeneration, buildSnapshotId, timestamp,
                expectedUpdatedAt, expectedState, expectedGeneration, null);
    }

    boolean beginIndexGeneration(
            String indexId,
            int newGeneration,
            String buildSnapshotId,
            Instant timestamp,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration,
            String sourceSchemaFingerprint) {
        validateGenerationTransition(
                newGeneration, expectedState, expectedGeneration);
        try (Connection conn = dataSource().getConnection()) {
            return reserveGeneration(conn, new GenerationReservation(
                    indexId, newGeneration, buildSnapshotId, timestamp,
                    expectedUpdatedAt, expectedState, expectedGeneration, sourceSchemaFingerprint));
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to begin fenced index generation", e);
        }
    }

    private static void bindSourceSchema(
            Connection conn, String indexId, String sourceSchemaFingerprint) throws SQLException {
        if (sourceSchemaFingerprint == null) {
            return;
        }
        try (PreparedStatement statement = conn.prepareStatement(
                "UPDATE index_descriptors SET source_schema_fingerprint=? WHERE index_id=?")) {
            statement.setString(1, sourceSchemaFingerprint);
            statement.setString(2, indexId);
            statement.executeUpdate();
        }
    }

    private static boolean reserveGeneration(Connection conn, GenerationReservation request)
            throws SQLException {
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement lock = conn.prepareStatement(
                    "SELECT pangrova_assert_index_schema_descriptor_available(?)")) {
                lock.setString(1, request.indexId());
                lock.executeQuery().close();
            }
            if (!lockExpectedDescriptor(
                    conn, request.indexId(), request.expectedUpdatedAt(),
                    request.expectedState(), request.expectedGeneration())
                    || hasBuildingGeneration(conn, request.indexId())) {
                conn.rollback();
                return false;
            }
            bindSourceSchema(conn, request.indexId(), request.sourceSchemaFingerprint());
            if (!restartFailedGeneration(
                    conn, request.indexId(), request.newGeneration(),
                    request.expectedGeneration(), request.buildSnapshotId(),
                    request.timestamp())) {
                insertBuildingGeneration(
                        conn, request.indexId(), request.newGeneration(),
                        request.expectedGeneration(), request.buildSnapshotId(),
                        request.timestamp());
            }
            conn.commit();
            return true;
        } catch (SQLException | RuntimeException failure) {
            rollbackAfterFailure(conn, failure);
            throw failure;
        }
    }

    boolean failIndexGeneration(
            String indexId,
            int generation,
            Instant expectedBuildUpdatedAt,
            Instant failedAt,
            String failureReason) {
        String reason = truncateReason(failureReason);
        try (Connection conn = dataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                FailureTarget target = lockFailureTarget(conn, indexId, generation, expectedBuildUpdatedAt);
                if (target == null) {
                    conn.rollback();
                    return false;
                }
                boolean failed = target.initial()
                        ? failInitialGeneration(conn, indexId, generation, target.snapshot(), reason, failedAt)
                        : failStagingGeneration(conn, indexId, generation, reason, failedAt);
                if (!failed) {
                    conn.rollback();
                    return false;
                }
                conn.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new MetastoreException("Failed to fence failed index generation", e);
        }
    }

    private static FailureTarget lockFailureTarget(
            Connection conn, String indexId, int generation, Instant expectedBuildUpdatedAt)
            throws SQLException {
        IndexState state;
        int activeGeneration;
        String snapshot;
        Instant descriptorUpdatedAt;
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT state,generation,build_snapshot_id,updated_at FROM index_descriptors "
                        + "WHERE index_id=? FOR UPDATE")) {
            statement.setString(1, indexId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                state = IndexState.valueOf(row.getString("state"));
                activeGeneration = row.getInt("generation");
                snapshot = row.getString("build_snapshot_id");
                descriptorUpdatedAt = getInstant(row, "updated_at");
            }
        }
        if (state == IndexState.BUILDING && activeGeneration == generation
                && descriptorUpdatedAt.equals(expectedBuildUpdatedAt)) {
            return new FailureTarget(true, snapshot);
        }
        if ((state != IndexState.READY && state != IndexState.STALE)
                || activeGeneration != generation - 1) {
            return null;
        }
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT 1 FROM index_generation_builds WHERE index_id=? AND generation=? "
                        + "AND base_generation=? AND state='BUILDING' AND updated_at=? FOR UPDATE")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, activeGeneration);
            setInstant(statement, 4, expectedBuildUpdatedAt);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? new FailureTarget(false, snapshot) : null;
            }
        }
    }

    private record FailureTarget(boolean initial, String snapshot) {}

    private static boolean failInitialGeneration(
            Connection conn, String indexId, int generation, String snapshot,
            String reason, Instant failedAt) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "UPDATE index_descriptors SET state='STALE', stale_reason=?, updated_at=? "
                        + "WHERE index_id=? AND state='BUILDING' AND generation=?")) {
            statement.setString(1, reason);
            setInstant(statement, 2, failedAt);
            statement.setString(3, indexId);
            statement.setInt(4, generation);
            if (statement.executeUpdate() != 1) {
                return false;
            }
        }
        String insert = "INSERT INTO index_generation_builds "
                + "(index_id,generation,base_generation,source_snapshot,state,failure_reason,started_at,updated_at) "
                + "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (index_id,generation) DO UPDATE "
                + "SET state='FAILED', failure_reason=EXCLUDED.failure_reason, updated_at=EXCLUDED.updated_at "
                + "WHERE index_generation_builds.state IN ('BUILDING','FAILED')";
        try (PreparedStatement statement = conn.prepareStatement(insert)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, generation);
            statement.setString(4, snapshot == null ? "" : snapshot);
            statement.setString(5, "FAILED");
            statement.setString(6, reason);
            setInstant(statement, 7, failedAt);
            setInstant(statement, 8, failedAt);
            return statement.executeUpdate() == 1;
        }
    }

    List<IndexBuildCandidate> findIndexBuildRecoveryCandidates() {
        String sql = """
                SELECT d.*, COALESCE(b.generation,d.generation) AS recovery_generation,
                       COALESCE(b.state,'BUILDING') AS recovery_state,
                       COALESCE(b.updated_at,d.updated_at) AS recovery_updated_at,
                       c.heartbeat_at AS claim_heartbeat_at
                FROM index_descriptors d
                LEFT JOIN index_generation_builds b ON b.index_id=d.index_id AND (
                    (d.state='BUILDING' AND b.generation=d.generation) OR
                    (d.state='READY' AND b.generation=d.generation+1) OR
                    (d.state='STALE' AND (b.generation=d.generation+1 OR
                        (b.generation=d.generation AND b.state='FAILED'))))
                LEFT JOIN index_build_claims c ON c.index_id=d.index_id
                    AND c.generation=COALESCE(b.generation,d.generation)
                WHERE d.state IN ('BUILDING','READY','STALE')
                  AND (d.state='BUILDING' OR b.state IN ('BUILDING','FAILED'))
                ORDER BY COALESCE(b.updated_at,d.updated_at), d.index_id
                """;
        return executeQuery(sql, statement -> { }, rows -> {
            List<IndexBuildCandidate> candidates = new java.util.ArrayList<>();
            while (rows.next()) {
                candidates.add(new IndexBuildCandidate(mapDescriptor(rows),
                        rows.getInt("recovery_generation"), rows.getString("recovery_state"),
                        getInstant(rows, "recovery_updated_at"),
                        rows.getTimestamp("claim_heartbeat_at") == null ? null
                                : rows.getTimestamp("claim_heartbeat_at").toInstant()));
            }
            return List.copyOf(candidates);
        });
    }

    Optional<IndexBuildAbandonment> abandonIndexGeneration(
            String indexId, int generation, Instant expectedUpdatedAt,
            Instant cutoff, Instant abandonedAt, String reason) {
        Objects.requireNonNull(expectedUpdatedAt, "expectedUpdatedAt");
        Objects.requireNonNull(cutoff, "cutoff");
        Objects.requireNonNull(abandonedAt, "abandonedAt");
        try (Connection conn = dataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                Optional<IndexBuildAbandonment> abandoned = abandonExactGeneration(
                        conn, indexId, generation, expectedUpdatedAt, cutoff, abandonedAt,
                        truncateReason(reason));
                if (abandoned.isEmpty()) {
                    conn.rollback();
                } else {
                    conn.commit();
                }
                return abandoned;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to abandon fenced index generation", failure);
        }
    }

    private static Optional<IndexBuildAbandonment> abandonExactGeneration(
            Connection conn, String indexId, int generation, Instant expectedUpdatedAt,
            Instant cutoff, Instant abandonedAt, String reason) throws SQLException {
        IndexState state;
        int activeGeneration;
        String snapshot;
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT state,generation,build_snapshot_id FROM index_descriptors "
                        + "WHERE index_id=? AND updated_at=? FOR UPDATE")) {
            statement.setString(1, indexId);
            setInstant(statement, 2, expectedUpdatedAt);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                state = IndexState.valueOf(row.getString("state"));
                activeGeneration = row.getInt("generation");
                snapshot = row.getString("build_snapshot_id");
            }
        }
        boolean initial = state == IndexState.BUILDING && activeGeneration == generation;
        if (!initial && !((state == IndexState.READY || state == IndexState.STALE)
                && activeGeneration == generation - 1)) {
            return Optional.empty();
        }
        Instant heartbeat = lockClaimHeartbeat(conn, indexId, generation);
        Instant buildUpdatedAt = initial ? expectedUpdatedAt
                : lockBuildingGeneration(conn, indexId, generation, activeGeneration);
        if (buildUpdatedAt == null) {
            return Optional.empty();
        }
        Instant lastActivity = heartbeat == null || heartbeat.isBefore(buildUpdatedAt)
                ? buildUpdatedAt : heartbeat;
        if (!lastActivity.isBefore(cutoff)) {
            return Optional.empty();
        }
        if (initial) {
            if (!failInitialGeneration(conn, indexId, generation, snapshot, reason, abandonedAt)) {
                return Optional.empty();
            }
        } else if (!failStagingGeneration(conn, indexId, generation, reason, abandonedAt)) {
            return Optional.empty();
        }
        deleteGenerationClaim(conn, indexId, generation);
        List<IndexFileMapping> mappings = loadGenerationMappings(conn, indexId, generation);
        Set<String> paths = findUnreferencedPaths(conn, indexId, generation);
        IndexDescriptor descriptor;
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT * FROM index_descriptors WHERE index_id=?")) {
            statement.setString(1, indexId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    return Optional.empty();
                }
                descriptor = mapDescriptor(row);
            }
        }
        return Optional.of(new IndexBuildAbandonment(descriptor, mappings, paths));
    }

    private static Instant lockClaimHeartbeat(
            Connection conn, String indexId, int generation) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT heartbeat_at FROM index_build_claims "
                        + "WHERE index_id=? AND generation=? FOR UPDATE")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? row.getTimestamp(1).toInstant() : null;
            }
        }
    }

    private static Instant lockBuildingGeneration(
            Connection conn, String indexId, int generation, int baseGeneration) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT updated_at FROM index_generation_builds WHERE index_id=? AND generation=? "
                        + "AND base_generation=? AND state='BUILDING' FOR UPDATE")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, baseGeneration);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? getInstant(row, "updated_at") : null;
            }
        }
    }

    private static boolean failStagingGeneration(
            Connection conn, String indexId, int generation, String reason, Instant failedAt)
            throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "UPDATE index_generation_builds SET state='FAILED', failure_reason=?, updated_at=? "
                        + "WHERE index_id=? AND generation=? AND state='BUILDING'")) {
            statement.setString(1, reason);
            setInstant(statement, 2, failedAt);
            statement.setString(3, indexId);
            statement.setInt(4, generation);
            return statement.executeUpdate() == 1;
        }
    }

    private static void deleteGenerationClaim(
            Connection conn, String indexId, int generation) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "DELETE FROM index_build_claims WHERE index_id=? AND generation=?")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.executeUpdate();
        }
    }

    private static List<IndexFileMapping> loadGenerationMappings(
            Connection conn, String indexId, int generation) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT * FROM index_file_mappings WHERE index_id=? AND generation=? "
                        + "ORDER BY data_file_path")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (ResultSet rows = statement.executeQuery()) {
                return IndexMetadataMapper.mapMappingList(rows);
            }
        }
    }

    private static Set<String> findUnreferencedPaths(
            Connection conn, String indexId, int generation) throws SQLException {
        String sql = "SELECT DISTINCT mapping.index_file_path FROM index_file_mappings mapping "
                + "WHERE mapping.index_id=? AND mapping.generation=? AND NOT EXISTS ("
                + "SELECT 1 FROM index_file_mappings owner WHERE owner.index_id=mapping.index_id "
                + "AND owner.index_file_path=mapping.index_file_path AND owner.generation<>?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, generation);
            try (ResultSet rows = statement.executeQuery()) {
                Set<String> paths = new java.util.LinkedHashSet<>();
                while (rows.next()) {
                    paths.add(rows.getString(1));
                }
                return Set.copyOf(paths);
            }
        }
    }

    boolean completeIndexBuildCleanup(
            String indexId, int generation, Instant expectedUpdatedAt,
            Instant expectedBuildUpdatedAt, IndexState expectedState, int expectedGeneration) {
        try (Connection conn = dataSource().getConnection()) {
            conn.setAutoCommit(false);
            try {
                if (!lockExpectedDescriptor(conn, indexId, expectedUpdatedAt,
                        expectedState, expectedGeneration)
                        || !lockFailedGeneration(conn, indexId, generation, expectedBuildUpdatedAt)) {
                    conn.rollback();
                    return false;
                }
                deleteGenerationClaim(conn, indexId, generation);
                try (PreparedStatement statement = conn.prepareStatement(
                        "DELETE FROM index_file_mappings WHERE index_id=? AND generation=?")) {
                    statement.setString(1, indexId);
                    statement.setInt(2, generation);
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = conn.prepareStatement(
                        "DELETE FROM index_generation_builds WHERE index_id=? AND generation=?")) {
                    statement.setString(1, indexId);
                    statement.setInt(2, generation);
                    statement.executeUpdate();
                }
                conn.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to complete fenced index build cleanup", failure);
        }
    }

    private static boolean lockFailedGeneration(
            Connection conn, String indexId, int generation, Instant expectedUpdatedAt)
            throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT 1 FROM index_generation_builds WHERE index_id=? AND generation=? "
                        + "AND state='FAILED' AND updated_at=? FOR UPDATE")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            setInstant(statement, 3, expectedUpdatedAt);
            try (ResultSet row = statement.executeQuery()) {
                return row.next();
            }
        }
    }

    boolean isIndexGenerationBuildable(String indexId, int generation) {
        if (generation < 0) {
            return false;
        }
        String sql = "SELECT 1 FROM index_descriptors d WHERE d.index_id=? AND ("
                + "(d.state='BUILDING' AND d.generation=?) OR "
                + "(d.state IN ('READY','STALE') AND d.generation=? AND EXISTS ("
                + "SELECT 1 FROM index_generation_builds b WHERE b.index_id=d.index_id "
                + "AND b.generation=? AND b.base_generation=d.generation "
                + "AND b.state='BUILDING')))";
        try (Connection connection = dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, generation - 1);
            statement.setInt(4, generation);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException failure) {
            throw new MetastoreException(
                    "Failed to validate buildable index generation", failure);
        }
    }

    private static final String ABANDONED_NATIVE_RESERVATIONS = """
            SELECT d.*, COALESCE(b.generation,d.generation) AS recovery_generation,
                   COALESCE(b.updated_at,d.updated_at) AS recovery_updated_at
            FROM index_descriptors d
            LEFT JOIN index_generation_builds b ON b.index_id=d.index_id
                 AND b.base_generation=d.generation AND b.generation=d.generation+1 AND b.state='BUILDING'
            WHERE d.index_type IN ('VECTOR','INVERTED')
              AND ((d.state='BUILDING' AND b.generation IS NULL)
                   OR (d.state IN ('READY','STALE') AND b.generation IS NOT NULL))
              AND COALESCE(b.updated_at,d.updated_at)<=?
              AND NOT EXISTS (SELECT 1 FROM artifact_snapshot_pins p
                   WHERE p.tenant_id=d.tenant_id AND p.index_id=d.index_id
                     AND p.generation=COALESCE(b.generation,d.generation))
              AND NOT EXISTS (SELECT 1 FROM artifact_build_jobs j
                   WHERE j.tenant_id=d.tenant_id AND j.artifact_id=d.index_id
                     AND j.staging_generation=CAST(COALESCE(b.generation,d.generation) AS text))
            """;

    List<IndexBuildCandidate> listAbandonedNativeReservations(int limit, Instant cutoff) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Objects.requireNonNull(cutoff, "cutoff");
        String sql = ABANDONED_NATIVE_RESERVATIONS + " ORDER BY recovery_updated_at,d.index_id LIMIT ?";
        return executeQuery(sql, statement -> {
            setInstant(statement, 1, cutoff);
            statement.setInt(2, limit);
        }, rows -> {
            List<IndexBuildCandidate> candidates = new java.util.ArrayList<>();
            while (rows.next()) {
                candidates.add(mapAbandonedNativeReservation(rows));
            }
            return List.copyOf(candidates);
        });
    }

    Optional<IndexBuildCandidate> findAbandonedNativeReservation(String indexId, int generation, Instant cutoff) {
        Objects.requireNonNull(cutoff, "cutoff");
        String sql = ABANDONED_NATIVE_RESERVATIONS + " AND d.index_id=? AND COALESCE(b.generation,d.generation)=?";
        return executeQuery(sql, statement -> {
            setInstant(statement, 1, cutoff);
            statement.setString(2, indexId);
            statement.setInt(3, generation);
        }, rows -> rows.next() ? Optional.of(mapAbandonedNativeReservation(rows)) : Optional.empty());
    }

    private static IndexBuildCandidate mapAbandonedNativeReservation(ResultSet row) throws SQLException {
        return new IndexBuildCandidate(mapDescriptor(row), row.getInt("recovery_generation"), "BUILDING",
                getInstant(row, "recovery_updated_at"), null);
    }

    Optional<Instant> findIndexGenerationSnapshotReservation(String indexId, int generation, String snapshotId) {
        if (generation < 0 || snapshotId == null || snapshotId.isBlank()) {
            return Optional.empty();
        }
        String sql = "SELECT d.updated_at FROM index_descriptors d WHERE d.index_id=? "
                + "AND d.state='BUILDING' AND d.generation=? AND d.build_snapshot_id=? "
                + "UNION ALL SELECT b.updated_at FROM index_generation_builds b "
                + "JOIN index_descriptors d ON d.index_id=b.index_id WHERE d.index_id=? "
                + "AND d.state IN ('READY','STALE') AND d.generation=? AND b.base_generation=d.generation "
                + "AND b.generation=? AND b.state='BUILDING' AND b.source_snapshot=?";
        return executeQuery(sql, statement -> {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setString(3, snapshotId);
            statement.setString(4, indexId);
            statement.setInt(5, generation - 1);
            statement.setInt(6, generation);
            statement.setString(7, snapshotId);
        }, rows -> rows.next() ? Optional.of(getInstant(rows, "updated_at")) : Optional.empty());
    }

    boolean isIndexGenerationSnapshotBuildable(
            String indexId, int generation, String snapshotId, Instant reservationUpdatedAt) {
        if (reservationUpdatedAt == null) {
            return false;
        }
        return findIndexGenerationSnapshotReservation(indexId, generation, snapshotId)
                .map(updatedAt -> updatedAt.equals(
                        org.pangrova.persistence.jdbc.support.JdbcSupport.canonicalInstant(reservationUpdatedAt)))
                .orElse(false);
    }

    private static boolean lockExpectedDescriptor(
            Connection connection,
            String indexId,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration) throws SQLException {
        String sql = "SELECT state,generation,updated_at FROM index_descriptors "
                + "WHERE index_id=? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        && expectedState.name().equals(result.getString("state"))
                        && expectedGeneration == result.getInt("generation")
                        && expectedUpdatedAt.equals(getInstant(result, "updated_at"));
            }
        }
    }

    private static boolean hasBuildingGeneration(
            Connection connection, String indexId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM index_generation_builds "
                        + "WHERE index_id=? AND state='BUILDING'")) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static void insertBuildingGeneration(
            Connection connection,
            String indexId,
            int generation,
            int baseGeneration,
            String sourceSnapshot,
            Instant timestamp) throws SQLException {
        String sql = "INSERT INTO index_generation_builds "
                + "(index_id,generation,base_generation,source_snapshot,state,"
                + "failure_reason,started_at,updated_at) VALUES (?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, baseGeneration);
            statement.setString(4, sourceSnapshot == null ? "" : sourceSnapshot);
            statement.setString(5, "BUILDING");
            statement.setString(6, null);
            setInstant(statement, 7, timestamp);
            setInstant(statement, 8, timestamp);
            statement.executeUpdate();
        }
    }

    private static boolean restartFailedGeneration(
            Connection connection,
            String indexId,
            int generation,
            int baseGeneration,
            String sourceSnapshot,
            Instant timestamp) throws SQLException {
        String update = "UPDATE index_generation_builds SET base_generation=?, "
                + "source_snapshot=?, state='BUILDING', failure_reason=NULL, "
                + "started_at=?, updated_at=? WHERE index_id=? AND generation=? "
                + "AND state='FAILED'";
        try (PreparedStatement statement = connection.prepareStatement(update)) {
            statement.setInt(1, baseGeneration);
            statement.setString(2, sourceSnapshot == null ? "" : sourceSnapshot);
            setInstant(statement, 3, timestamp);
            setInstant(statement, 4, timestamp);
            statement.setString(5, indexId);
            statement.setInt(6, generation);
            if (statement.executeUpdate() != 1) {
                return false;
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM index_file_mappings WHERE index_id=? AND generation=?")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.executeUpdate();
        }
        return true;
    }

    private static String truncateReason(String failureReason) {
        String reason = failureReason == null || failureReason.isBlank()
                ? "state_transition" : failureReason;
        return reason.substring(0, Math.min(reason.length(), 1024));
    }

    boolean updateIndexState(String indexId, String newState,
                             Instant timestamp, Instant expectedUpdatedAt,
                             String expectedState, int expectedGeneration) {
        return updateIndexState(indexId, newState, timestamp, null,
                expectedUpdatedAt, expectedState, expectedGeneration);
    }

    boolean updateIndexState(
            String indexId,
            String newState,
            Instant timestamp,
            String staleReason,
            Instant expectedUpdatedAt,
            String expectedState,
            int expectedGeneration) {
        String sql = "UPDATE index_descriptors SET state=?, updated_at=?, stale_reason=? "
                + "WHERE index_id=? AND updated_at=? AND state=? AND generation=?";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newState);
            setInstant(ps, 2, timestamp);
            ps.setString(3, IndexState.STALE.name().equals(newState)
                    ? truncateReason(staleReason) : null);
            ps.setString(4, indexId);
            setInstant(ps, 5, expectedUpdatedAt);
            ps.setString(6, expectedState);
            ps.setInt(7, expectedGeneration);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to update index state with state+generation fencing", e);
        }
    }

    Optional<IndexDescriptor> findIndex(String indexId) {
        String sql = "SELECT * FROM index_descriptors WHERE index_id=?";
        return executeQuery(sql, ps -> ps.setString(1, indexId),
                rs -> rs.next() ? Optional.of(mapDescriptor(rs)) : Optional.empty());
    }

    List<IndexDescriptor> findIndexesByTable(
            String tenantId, String catalog, String schema, String table) {
        String sql = "SELECT * FROM index_descriptors "
                + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? AND table_name=?";
        return executeQuery(sql, ps -> {
            ps.setString(1, tenantId);
            ps.setString(2, catalog);
            ps.setString(3, schema);
            ps.setString(4, table);
        }, IndexMetadataMapper::mapDescriptorList);
    }

    List<IndexDescriptor> findIndexesByTableForGlobalMaintenance(
            String catalog, String schema, String table) {
        String sql = "SELECT * FROM index_descriptors "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=?";
        return executeQuery(sql, ps -> {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
        }, IndexMetadataMapper::mapDescriptorList);
    }

    List<IndexDescriptor> findReadyIndexesForColumns(
            String tenantId, String catalog, String schema, String table,
            List<String> columns, boolean includeStale) {
        try (var connection = dataSource().getConnection();
                var selection = JdbcArraySelection.create(connection, columns);
                var statement = connection.prepareStatement("""
                        SELECT * FROM index_descriptors
                        WHERE tenant_id=? AND catalog_name=? AND schema_name=? AND table_name=?
                        AND (state='READY' OR (? AND state='STALE'))
                        AND jsonb_exists_any(column_names, ?)
                        """)) {
            statement.setString(1, tenantId);
            statement.setString(2, catalog);
            statement.setString(3, schema);
            statement.setString(4, table);
            statement.setBoolean(5, includeStale);
            statement.setArray(6, selection.array());
            try (var rows = statement.executeQuery()) {
                return IndexMetadataMapper.mapDescriptorList(rows).stream()
                        .filter(index -> index.columnNames().stream().anyMatch(columns::contains))
                        .toList();
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Ready index selection failed", failure);
        }
    }

    void dropIndex(String indexId, Instant timestamp) {
        updateIndexState(indexId, IndexState.DROPPED.name(), timestamp);
    }

    void purgeIndex(String indexId) {
        int deleted = executeUpdateCount(
                "DELETE FROM index_descriptors WHERE index_id=? AND state='DROPPED'",
                ps -> ps.setString(1, indexId));
        if (deleted == 0 && findIndex(indexId).isPresent()) {
            throw new IllegalStateException("Only a dropped index may be purged: " + indexId);
        }
    }

    // --- Schema evolution ---

    List<IndexDescriptor> findIndexesByColumn(
            String tenantId, String catalog, String schema, String table, String column) {
        return findIndexesByTable(tenantId, catalog, schema, table).stream()
                .filter(idx -> idx.columnNames().contains(column))
                .toList();
    }

    void renameColumnInIndexes(
            String tenantId, String catalog, String schema, String table,
            String oldColumn, String newColumn) {
        List<IndexDescriptor> indexes = findIndexesByColumn(
                tenantId, catalog, schema, table, oldColumn);
        for (IndexDescriptor idx : indexes) {
            List<String> updatedColumns = idx.columnNames().stream()
                    .map(c -> c.equals(oldColumn) ? newColumn : c)
                    .toList();
            executeUpdate("UPDATE index_descriptors SET column_names=?, updated_at=? WHERE index_id=?",
                    ps -> {
                        setJson(ps, 1, toJson(updatedColumns));
                        setInstant(ps, 2, Instant.now());
                        ps.setString(3, idx.indexId());
                    });
        }
    }

    void dropAllIndexesForTable(
            String tenantId, String catalog, String schema, String table, Instant timestamp) {
        executeUpdate("UPDATE index_descriptors SET state=?, updated_at=? "
                        + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? "
                        + "AND table_name=? AND state<>?",
                ps -> {
                    ps.setString(1, IndexState.DROPPED.name());
                    setInstant(ps, 2, timestamp);
                    ps.setString(3, tenantId);
                    ps.setString(4, catalog);
                    ps.setString(5, schema);
                    ps.setString(6, table);
                    ps.setString(7, IndexState.DROPPED.name());
                });
    }

    void renameTable(
            String tenantId, String catalog, String schema, String oldTable, String newTable) {
        executeUpdate("UPDATE index_descriptors SET table_name=?, updated_at=? "
                        + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? AND table_name=?",
                ps -> {
                    ps.setString(1, newTable);
                    setInstant(ps, 2, Instant.now());
                    ps.setString(3, tenantId);
                    ps.setString(4, catalog);
                    ps.setString(5, schema);
                    ps.setString(6, oldTable);
                });
    }

    void updateLastIndexedSnapshotId(String indexId, String snapshotId) {
        executeUpdate("UPDATE index_descriptors SET last_indexed_snapshot_id=?, updated_at=? "
                        + "WHERE index_id=?",
                ps -> {
                    ps.setString(1, snapshotId);
                    setInstant(ps, 2, Instant.now());
                    ps.setString(3, indexId);
                });
    }


    private static void validateGenerationTransition(
            int newGeneration, IndexState expectedState, int expectedGeneration) {
        if (newGeneration != Math.addExact(expectedGeneration, 1)) {
            throw new IllegalArgumentException(
                    "newGeneration must advance exactly one generation");
        }
        if (expectedState != IndexState.READY && expectedState != IndexState.STALE) {
            throw new IllegalArgumentException(
                    "Only READY or STALE indexes can reserve an online generation");
        }
    }
}
