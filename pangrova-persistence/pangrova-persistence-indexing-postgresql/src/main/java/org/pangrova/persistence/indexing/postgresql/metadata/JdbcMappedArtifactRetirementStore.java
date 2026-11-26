package org.pangrova.persistence.indexing.postgresql.metadata;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.metadata.IndexCleanup.MappedArtifactRetirement;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

/** Durable mapped-artifact retirement receipt, shared by transactional removal and maintenance. */
final class JdbcMappedArtifactRetirementStore extends JdbcStoreSupport {
    private static final String TABLE = "mapped_index_artifact_retirements";
    private static final String COLUMNS =
            "retirement_id,index_id,tenant_id,catalog_name,schema_name,table_name,"
                    + "data_file_path,artifact_path,retired_generation";
    private static final String QUALIFIED_COLUMNS =
            "r.retirement_id,r.index_id,r.tenant_id,r.catalog_name,r.schema_name,r.table_name,"
                    + "r.data_file_path,r.artifact_path,r.retired_generation";
    private static final String UNREFERENCED =
            "NOT EXISTS (SELECT 1 FROM index_file_mappings m WHERE "
                    + "md5(m.index_file_path)=md5(r.artifact_path) AND m.index_file_path=r.artifact_path) "
                    + "AND NOT EXISTS (SELECT 1 FROM scalar_index_artifact_intents i WHERE "
                    + "i.artifact_digest=r.artifact_digest AND i.artifact_path=r.artifact_path)";

    JdbcMappedArtifactRetirementStore(DataSource source) {
        super(source);
    }

    /** One lock namespace shared with the migration's mapping/pre-write insert trigger. */
    static void lockPaths(Connection connection, List<String> paths) throws SQLException {
        if (paths.isEmpty()) {
            return;
        }
        try (var values = JdbcArraySelection.create(connection, paths);
                var statement = connection.prepareStatement(
                        "SELECT pg_advisory_xact_lock(17002,hashtext(path)) FROM "
                                + "(SELECT DISTINCT unnest(?::text[]) AS path ORDER BY path) paths")) {
            statement.setArray(1, values.array());
            statement.executeQuery().close();
        }
    }

    /** Caller holds the descriptor and exact path locks and owns the mapping removal transaction. */
    static void record(Connection connection, String indexId, List<RetiredPath> paths) throws SQLException {
        if (paths.isEmpty()) return;
        String sql = "INSERT INTO " + TABLE + " "
                + "(artifact_digest,index_id,descriptor_ref,tenant_id,catalog_name,schema_name,table_name,"
                + "data_file_path,artifact_path,retired_generation,state) "
                + "SELECT ?,d.index_id,d.index_id,d.tenant_id,d.catalog_name,d.schema_name,d.table_name,"
                + "?,?,?,'PENDING' "
                + "FROM index_descriptors d WHERE d.index_id=?";
        try (var statement = connection.prepareStatement(sql)) {
            for (RetiredPath path : paths) {
                statement.setString(1, DigestUtils.sha256Hex(path.artifactPath()));
                statement.setString(2, path.dataFilePath());
                statement.setString(3, path.artifactPath());
                statement.setInt(4, path.retiredGeneration());
                statement.setString(5, indexId);
                statement.addBatch();
            }
            for (int count : statement.executeBatch()) {
                if (count != 1 && count != Statement.SUCCESS_NO_INFO) {
                    throw new IllegalStateException("Mapped artifact retirement lost its descriptor");
                }
            }
        }
    }

    List<MappedArtifactRetirement> poll(int limit) {
        if (limit <= 0) throw new IllegalArgumentException("A positive mapped artifact batch limit is required");
        String sql = "WITH candidates AS (SELECT r.retirement_id FROM " + TABLE + " r "
                + "WHERE r.state='PENDING' AND " + UNREFERENCED
                + " ORDER BY r.queue_order,r.retirement_id LIMIT ? FOR UPDATE OF r SKIP LOCKED) "
                + "UPDATE " + TABLE + " r SET queue_order=nextval('mapped_index_artifact_retirement_queue_seq') "
                + "FROM candidates c WHERE r.retirement_id=c.retirement_id RETURNING " + QUALIFIED_COLUMNS;
        return executeQuery(sql, statement -> statement.setInt(1, limit), rows -> {
            List<MappedArtifactRetirement> items = new ArrayList<>();
            while (rows.next()) items.add(read(rows));
            return List.copyOf(items);
        });
    }

    boolean begin(MappedArtifactRetirement retirement) {
        return checkOrComplete(retirement, false);
    }

    void complete(MappedArtifactRetirement retirement) {
        if (!checkOrComplete(retirement, true)) {
            throw new IllegalStateException("Mapped artifact deletion acknowledgement lost its exact receipt");
        }
    }

    private boolean checkOrComplete(MappedArtifactRetirement retirement, boolean complete) {
        try (Connection connection = dataSource().getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                boolean changed = checkOrComplete(connection, retirement, complete);
                connection.commit();
                return changed;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to reconcile mapped artifact retirement", failure);
        }
    }

    private static boolean checkOrComplete(Connection connection, MappedArtifactRetirement retirement,
            boolean complete) throws SQLException {
        boolean scopeCurrent = lockDescriptor(connection, retirement);
        lockPaths(connection, List.of(retirement.artifactPath()));
        var current = lockReceipt(connection, retirement.retirementId());
        if (current == null || !current.retirement().equals(retirement)) {
            return false;
        }
        if ("DELETED".equals(current.state())) {
            return complete;
        }
        if (!scopeCurrent || !"PENDING".equals(current.state()) || !current.unreferenced()) {
            return false;
        }
        if (!complete) return true;
        try (var statement = connection.prepareStatement("UPDATE " + TABLE
                + " SET state='DELETED',deleted_at=CURRENT_TIMESTAMP,descriptor_ref=NULL "
                + "WHERE retirement_id=? AND state='PENDING'")) {
            statement.setObject(1, retirement.retirementId());
            return statement.executeUpdate() == 1;
        }
    }

    private static boolean lockDescriptor(Connection connection, MappedArtifactRetirement retirement)
            throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT 1 FROM index_descriptors WHERE index_id=? AND tenant_id=? AND catalog_name=? "
                        + "AND schema_name=? AND table_name=? FOR UPDATE")) {
            statement.setString(1, retirement.indexId());
            statement.setString(2, retirement.tenantId());
            statement.setString(3, retirement.catalogName());
            statement.setString(4, retirement.schemaName());
            statement.setString(5, retirement.tableName());
            try (var rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private static LockedReceipt lockReceipt(Connection connection, UUID retirementId) throws SQLException {
        String sql = "SELECT " + COLUMNS + ",state,(" + UNREFERENCED + ") AS unreferenced FROM " + TABLE
                + " r WHERE retirement_id=? FOR UPDATE";
        try (var statement = connection.prepareStatement(sql)) {
            statement.setObject(1, retirementId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) return null;
                return new LockedReceipt(read(rows), rows.getString("state"), rows.getBoolean("unreferenced"));
            }
        }
    }

    private static MappedArtifactRetirement read(ResultSet rows) throws SQLException {
        return new MappedArtifactRetirement(rows.getObject("retirement_id", UUID.class),
                rows.getString("index_id"), rows.getString("tenant_id"), rows.getString("catalog_name"),
                rows.getString("schema_name"), rows.getString("table_name"), rows.getString("data_file_path"),
                rows.getString("artifact_path"), rows.getInt("retired_generation"));
    }

    boolean hasPending(String indexId) {
        return executeQuery("SELECT 1 FROM " + TABLE + " WHERE descriptor_ref=? LIMIT 1",
                statement -> statement.setString(1, indexId), ResultSet::next);
    }

    record RetiredPath(String artifactPath, String dataFilePath, int retiredGeneration) {}

    private record LockedReceipt(MappedArtifactRetirement retirement, String state, boolean unreferenced) {}
}
