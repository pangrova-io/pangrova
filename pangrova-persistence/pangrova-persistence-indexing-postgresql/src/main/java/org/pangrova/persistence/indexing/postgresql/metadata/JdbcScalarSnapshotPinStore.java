package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.persistence.indexing.api.metadata.ScalarSnapshotPins.ScalarPin;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Exact attempt identity and terminal-generation fence for scalar construction pin release. */
final class JdbcScalarSnapshotPinStore extends JdbcStoreSupport {
    private static final Duration RELEASE_RETRY_DELAY = Duration.ofMinutes(15);
    private static final String KEY = "index_id=? AND generation=? AND attempt_owner=?";
    private static final String RESERVE = """
            INSERT INTO index_scalar_snapshot_pins
              (index_id,generation,attempt_owner,tenant_id,catalog_name,schema_name,table_name,
               source_snapshot,state,created_at,updated_at,next_attempt_at)
            SELECT d.index_id,c.generation,c.owner_id,d.tenant_id,d.catalog_name,d.schema_name,d.table_name,
                   ?,'INTENDED',?,?,?
            FROM index_descriptors d JOIN index_build_claims c ON c.index_id=d.index_id
            WHERE d.index_id=? AND d.tenant_id=? AND d.catalog_name=? AND d.schema_name=? AND d.table_name=?
              AND c.generation=? AND c.owner_id=? AND d.index_type NOT IN ('VECTOR','INVERTED')
              AND d.build_principal IS NOT NULL AND d.build_authorization_scope IS NOT NULL
              AND ((d.state='BUILDING' AND d.generation=c.generation AND d.build_snapshot_id=?)
                OR (d.state IN ('READY','STALE') AND EXISTS
                    (SELECT 1 FROM index_generation_builds b
                     WHERE b.index_id=d.index_id AND b.generation=c.generation
                       AND b.base_generation=d.generation AND b.state='BUILDING' AND b.source_snapshot=?)))
            ON CONFLICT DO NOTHING
            """;
    private static final String ELIGIBLE = """
            p.next_attempt_at<=? AND p.state IN ('DRAINED','RELEASING')
            AND NOT EXISTS (SELECT 1 FROM index_build_claims c WHERE c.index_id=p.index_id
                AND c.generation=p.generation AND c.owner_id=p.attempt_owner)
            AND EXISTS (SELECT 1 FROM index_descriptors d WHERE d.index_id=p.index_id
                AND d.tenant_id=p.tenant_id AND d.catalog_name=p.catalog_name
                AND d.schema_name=p.schema_name AND d.table_name=p.table_name
                AND d.index_type NOT IN ('VECTOR','INVERTED')
                AND (d.state='DROPPED' OR d.generation>p.generation
                  OR EXISTS (SELECT 1 FROM index_generation_manifests m WHERE m.index_id=p.index_id
                     AND m.generation=p.generation AND m.source_snapshot=p.source_snapshot)
                  OR EXISTS (SELECT 1 FROM index_generation_builds b WHERE b.index_id=p.index_id
                     AND b.generation=p.generation AND b.source_snapshot=p.source_snapshot AND b.state='FAILED')
                  OR (d.state='STALE' AND d.generation=p.generation AND d.build_snapshot_id=p.source_snapshot))
                AND NOT EXISTS (SELECT 1 FROM index_generation_builds b WHERE b.index_id=p.index_id
                    AND b.generation=p.generation AND b.state='BUILDING'))
            """;
    private static final String LIST = "SELECT p.* FROM index_scalar_snapshot_pins p WHERE " + ELIGIBLE
            + " ORDER BY p.next_attempt_at,p.created_at,p.index_id,p.generation,p.attempt_owner LIMIT ?";
    private static final String CLAIM = "UPDATE index_scalar_snapshot_pins p "
            + "SET state='RELEASING',updated_at=?,next_attempt_at=? WHERE " + KEY + " AND " + ELIGIBLE
            + " RETURNING p.*";

    JdbcScalarSnapshotPinStore(DataSource dataSource) {
        super(dataSource);
    }

    boolean reserve(IndexDescriptor descriptor, String snapshot, SnapshotPinOwner owner, Instant now) {
        if (!descriptor.indexId().equals(owner.indexId()) || snapshot == null || snapshot.isBlank()) {
            throw new IllegalArgumentException("Exact scalar attempt and source snapshot are required");
        }
        return executeUpdateCount(RESERVE, statement -> {
            int parameter = 1;
            statement.setString(parameter++, snapshot);
            setInstant(statement, parameter++, now);
            setInstant(statement, parameter++, now);
            setInstant(statement, parameter++, now);
            statement.setString(parameter++, descriptor.indexId());
            statement.setString(parameter++, descriptor.tenantId());
            statement.setString(parameter++, descriptor.catalogName());
            statement.setString(parameter++, descriptor.schemaName());
            statement.setString(parameter++, descriptor.tableName());
            statement.setInt(parameter++, owner.generation());
            statement.setString(parameter++, owner.attemptId());
            statement.setString(parameter++, snapshot);
            statement.setString(parameter, snapshot);
        }) == 1;
    }

    void markDrained(SnapshotPinOwner owner, String snapshot, Instant now) {
        // Missing intent means reservation was rejected before any connector operation.
        String sql = "UPDATE index_scalar_snapshot_pins SET state='DRAINED',updated_at=?,next_attempt_at=? "
                + "WHERE " + KEY + " AND source_snapshot=? AND state='INTENDED'";
        executeUpdate(sql, statement -> {
            setInstant(statement, 1, now);
            setInstant(statement, 2, now);
            bindOwner(statement, 3, owner);
            statement.setString(6, snapshot);
        });
    }

    boolean hasUnreleased(String indexId) {
        return executeQuery("SELECT 1 FROM index_scalar_snapshot_pins WHERE index_id=? AND state<>'RELEASED' LIMIT 1",
                statement -> statement.setString(1, indexId), ResultSet::next);
    }

    Optional<ScalarPin> find(SnapshotPinOwner owner) {
        return executeQuery("SELECT * FROM index_scalar_snapshot_pins WHERE " + KEY,
                statement -> bindOwner(statement, 1, owner),
                rows -> rows.next() ? Optional.of(map(rows)) : Optional.empty());
    }

    List<ScalarPin> list(Instant now, int limit) {
        if (limit <= 0 || limit > 1000) {
            throw new IllegalArgumentException("Scalar pin cleanup limit must be between 1 and 1000");
        }
        return executeQuery(LIST, statement -> {
            setInstant(statement, 1, now);
            statement.setInt(2, limit);
        }, rows -> {
            List<ScalarPin> pins = new ArrayList<>();
            while (rows.next()) {
                pins.add(map(rows));
            }
            return pins;
        });
    }

    Optional<ScalarPin> claim(SnapshotPinOwner owner, Instant now) {
        return executeQuery(CLAIM, statement -> {
            setInstant(statement, 1, now);
            setInstant(statement, 2, now.plus(RELEASE_RETRY_DELAY));
            bindOwner(statement, 3, owner);
            setInstant(statement, 6, now);
        }, rows -> rows.next() ? Optional.of(map(rows)) : Optional.empty());
    }

    void markReleased(SnapshotPinOwner owner, String snapshot, Instant now) {
        String sql = "UPDATE index_scalar_snapshot_pins SET state='RELEASED',updated_at=? "
                + "WHERE " + KEY + " AND source_snapshot=? AND state IN ('RELEASING','RELEASED')";
        int changed = executeUpdateCount(sql, statement -> {
            setInstant(statement, 1, now);
            bindOwner(statement, 2, owner);
            statement.setString(5, snapshot);
        });
        if (changed != 1) {
            throw new IllegalStateException("Scalar snapshot pin release claim is absent");
        }
    }

    private static ScalarPin map(ResultSet rows) throws SQLException {
        return new ScalarPin(rows.getString("tenant_id"), rows.getString("catalog_name"),
                rows.getString("schema_name"), rows.getString("table_name"), rows.getString("source_snapshot"),
                new SnapshotPinOwner(rows.getString("index_id"), rows.getInt("generation"),
                        rows.getString("attempt_owner")), rows.getString("state"));
    }

    private static void bindOwner(PreparedStatement statement, int offset, SnapshotPinOwner owner)
            throws SQLException {
        statement.setString(offset, owner.indexId());
        statement.setInt(offset + 1, owner.generation());
        statement.setString(offset + 2, owner.attemptId());
    }
}
