package org.pangrova.indexing.store.jdbc;

import org.pangrova.metadata.exception.IndexAlreadyExistsException;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.persistence.jdbc.security.codec.DeferredAuthorizationScopeCodec;
import org.pangrova.persistence.jdbc.security.transaction.AuthorizationTransactionGate;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.AccessDeniedException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.indexing.store.jdbc.IndexMetadataMapper.bindDescriptor;
import static org.pangrova.indexing.store.jdbc.IndexMetadataMapper.mapDescriptor;
import static org.pangrova.indexing.store.jdbc.IndexMetadataMapper.setJson;
import static org.pangrova.indexing.store.jdbc.IndexMetadataMapper.toJson;
import static org.pangrova.indexing.store.jdbc.JdbcIndexSql.buildColumnIndexSql;
import static org.pangrova.common.jdbc.JdbcSupport.getInstant;
import static org.pangrova.common.jdbc.JdbcSupport.setInstant;

/**
 * JDBC operations for {@link IndexDescriptor} CRUD, schema evolution,
 * and state queries.
 */
final class JdbcIndexDescriptorStore extends JdbcStoreSupport {
    private static final int BUILD_PRINCIPAL_PARAMETER = 17;
    private static final int BUILD_SCOPE_PARAMETER = 18;

    JdbcIndexDescriptorStore(DataSource dataSource) {
        super(dataSource);
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
                + "tenant_id, build_principal, build_authorization_scope) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            executeUpdate(sql, ps -> {
                bindDescriptor(ps, descriptor);
                ps.setString(BUILD_PRINCIPAL_PARAMETER, principal);
                ps.setBytes(BUILD_SCOPE_PARAMETER,
                        scope == null ? null : DeferredAuthorizationScopeCodec.encode(scope));
            });
        } catch (MetastoreException e) {
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

    private static String readAndAuthorizeBuildPrincipal(Connection connection,
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
                    org.pangrova.metadata.service.TableControlDeferredPolicy.requireCurrent(
                            connection, principal, actual);
                }
                return principal;
            }
        }
    }

    static void requireCurrentScalarPublicationAuthority(Connection connection, String indexId)
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
        JdbcIndexSql.validateGenerationTransition(
                newGeneration, expectedState, expectedGeneration);
        try (Connection conn = dataSource().getConnection()) {
            return reserveGeneration(conn, new GenerationReservation(
                    indexId, newGeneration, buildSnapshotId, timestamp,
                    expectedUpdatedAt, expectedState, expectedGeneration));
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to begin fenced index generation", e);
        }
    }

    private static boolean reserveGeneration(Connection conn, GenerationReservation request)
            throws SQLException {
        conn.setAutoCommit(false);
        try {
            if (!lockExpectedDescriptor(
                    conn, request.indexId(), request.expectedUpdatedAt(),
                    request.expectedState(), request.expectedGeneration())
                    || hasBuildingGeneration(conn, request.indexId())) {
                conn.rollback();
                return false;
            }
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
            rollback(conn, failure);
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
        String sql = "UPDATE index_generation_builds SET state='FAILED', "
                + "failure_reason=?, updated_at=? WHERE index_id=? AND generation=? "
                + "AND state='BUILDING' AND updated_at=?";
        try (Connection conn = dataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, reason);
            setInstant(ps, 2, failedAt);
            ps.setString(3, indexId);
            ps.setInt(4, generation);
            setInstant(ps, 5, expectedBuildUpdatedAt);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new MetastoreException("Failed to fence failed index generation", e);
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

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
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
        List<String> states = includeStale ? List.of("READY", "STALE") : List.of("READY");
        String sql = buildColumnIndexSql(states.size(), columns.size());

        List<IndexDescriptor> results = executeQuery(sql, ps -> {
            int idx = 1;
            ps.setString(idx++, tenantId);
            ps.setString(idx++, catalog);
            ps.setString(idx++, schema);
            ps.setString(idx++, table);
            for (String state : states) {
                ps.setString(idx++, state);
            }
            for (String col : columns) {
                ps.setString(idx++, col);
            }
        }, IndexMetadataMapper::mapDescriptorList);

        return results.stream()
                .filter(idx -> idx.columnNames().stream().anyMatch(columns::contains))
                .toList();
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

}
