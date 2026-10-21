package org.pangrova.metadata.dynamictable;

import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.store.JdbcSupport;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** JDBC authority for immutable definitions, owned refresh candidates and atomic generation heads. */
public final class JdbcDynamicTablePublicationStore {
    private static final int OWNER_EXPIRES_PARAMETER = 11;
    private static final int CONTROL_EPOCH_PARAMETER = 12;
    private static final int CANDIDATE_PARAMETER = 13;
    private static final String COLUMNS = "tenant_id, logical_name, incarnation_id, definition_sql, "
            + "binding_catalog, binding_schema, published_table, revision, lifecycle_state, created_at, updated_at";
    private final DataSource dataSource;

    /** A repeated reservation cannot start Native work again. Its durable outcome must be reconciled. */
    public record Reservation(DynamicTablePublication publication, DynamicTableRefreshAttempt attempt, boolean first) {}

    /** Opens a publication authority on the migrated metadata database. */
    public JdbcDynamicTablePublicationStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Atomically reserves the immutable definition and first Native candidate under the actual issuer. */
    public Reservation create(DynamicTableDefinition definition, DynamicTableRefreshIssuer issuer) {
        return transaction("create", issuer, (connection, admitted) -> {
            lockAttempt(connection, definition.tenantId(), issuer.owner().executionId());
            var existing = attempt(connection, definition.tenantId(), issuer.owner().executionId(), true);
            if (existing.isPresent()) {
                var current = load(connection, definition.tenantId(), definition.logicalName(), true).orElseThrow();
                requireDefinition(current, definition);
                requireScope(current, existing.orElseThrow());
                requirePrincipal(existing.orElseThrow(), issuer);
                return new Reservation(current, existing.orElseThrow(), false);
            }
            String sql = "INSERT INTO dynamic_table_publications (" + COLUMNS
                    + ") VALUES (?, ?, ?, ?, ?, ?, NULL, 0, 'CREATING', ?, ?) RETURNING " + COLUMNS;
            DynamicTablePublication publication;
            try (var insert = connection.prepareStatement(sql)) {
                insert.setString(1, definition.tenantId());
                insert.setString(2, definition.logicalName());
                insert.setObject(3, UUID.randomUUID());
                insert.setString(4, definition.definitionSql());
                insert.setString(5, definition.bindingCatalog());
                insert.setString(6, definition.bindingSchema());
                Instant now = Instant.now();
                insert.setTimestamp(7, Timestamp.from(now));
                insert.setTimestamp(8, Timestamp.from(now));
                try (var rows = insert.executeQuery()) {
                    if (!rows.next()) throw new IllegalStateException("Dynamic Table definition was not reserved");
                    publication = read(rows);
                }
            }
            return new Reservation(publication, issue(connection, publication, admitted), true);
        });
    }

    /** Reserves an immutable candidate before Native I/O, against the exact observed logical head. */
    public Reservation reserveRefresh(DynamicTablePublication expected, DynamicTableRefreshIssuer issuer) {
        return transaction("reserve refresh", issuer, (connection, admitted) -> {
            lockAttempt(connection, expected.tenantId(), issuer.owner().executionId());
            var existing = attempt(connection, expected.tenantId(), issuer.owner().executionId(), true);
            if (existing.isPresent()) {
                requireScope(expected, existing.orElseThrow());
                requirePrincipal(existing.orElseThrow(), issuer);
                return new Reservation(expected, existing.orElseThrow(), false);
            }
            var current = load(connection, expected.tenantId(), expected.logicalName(), true).orElseThrow();
            if (!current.equals(expected)) {
                throw changed();
            }
            return new Reservation(current, issue(connection, current, admitted), true);
        });
    }

    /** Advances the head only after this candidate's exact Native commit, retaining an idempotent receipt. */
    public DynamicTableRefreshAttempt publish(DynamicTableRefreshAttempt expected,
            DynamicTableRefreshIssuer publisher) {
        if (!expected.executionId().equals(publisher.owner().executionId())
                || !expected.issuer().principal().equals(publisher.principal())) {
            throw new IllegalArgumentException("Refresh publisher differs from its admitted execution/principal");
        }
        return transaction("publish", publisher, (connection, admitted) -> {
            lockAttempt(connection, expected.tenantId(), expected.executionId());
            var actual = attempt(connection, expected.tenantId(), expected.executionId(), true).orElseThrow();
            requireAttemptIdentity(expected, actual);
            if (actual.state() == DynamicTableRefreshAttempt.State.PUBLISHED) return actual;
            String sql = "UPDATE dynamic_table_publications SET published_table=?, revision=revision+1, "
                    + "lifecycle_state='ACTIVE', updated_at=clock_timestamp(), last_refresh_execution_id=?, "
                    + "publisher_owner_id=?, publisher_owner_incarnation=?, publisher_owner_token=?, "
                    + "publication_control_epoch=? WHERE tenant_id=? AND logical_name=? AND incarnation_id=? "
                    + "AND revision=?";
            try (var update = connection.prepareStatement(sql)) {
                update.setString(1, actual.candidateTable());
                update.setString(2, actual.executionId());
                update.setString(3, publisher.owner().ownerId());
                update.setString(4, publisher.owner().ownerIncarnation());
                update.setLong(5, publisher.owner().fencingToken());
                update.setLong(6, publisher.controlEpoch());
                update.setString(7, actual.tenantId());
                update.setString(8, actual.logicalName());
                update.setObject(9, UUID.fromString(actual.incarnationId()));
                update.setLong(10, actual.baseRevision());
                if (update.executeUpdate() != 1) throw changed();
            }
            return attempt(connection, actual.tenantId(), actual.executionId(), false).orElseThrow();
        });
    }

    /** Loads an immutable candidate/outcome even after its logical name is dropped or replaced. */
    public Optional<DynamicTableRefreshAttempt> findRefresh(String tenantId, String executionId) {
        try (var connection = dataSource.getConnection()) {
            return attempt(connection, tenantId, executionId, false);
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot load Dynamic Table refresh", failure);
        }
    }

    /** Removes only an unpublished, unchanged creation; its candidate ownership remains for reconciliation/GC. */
    public void abandonCreation(DynamicTablePublication expected) {
        String sql = "DELETE FROM dynamic_table_publications WHERE tenant_id=? AND logical_name=? "
                + "AND incarnation_id=? AND revision=0 AND lifecycle_state='CREATING'";
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            statement.setString(1, expected.tenantId());
            statement.setString(2, expected.logicalName());
            statement.setObject(3, UUID.fromString(expected.incarnationId()));
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot abandon Dynamic Table " + expected.logicalName(), failure);
        }
    }

    /** Loads one tenant-scoped logical definition and its current head. */
    public Optional<DynamicTablePublication> load(String tenantId, String logicalName) {
        try (var connection = dataSource.getConnection()) {
            return load(connection, tenantId, logicalName, false);
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot load Dynamic Table " + logicalName, failure);
        }
    }

    /** Lists only definitions belonging to the requested tenant. */
    public List<DynamicTablePublication> list(String tenantId) {
        String sql = "SELECT " + COLUMNS + " FROM dynamic_table_publications WHERE tenant_id=?";
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            try (var rows = statement.executeQuery()) {
                var result = new ArrayList<DynamicTablePublication>();
                while (rows.next()) result.add(read(rows));
                return List.copyOf(result);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot list Dynamic Tables for tenant " + tenantId, failure);
        }
    }

    /** Removes a specific logical incarnation; candidate and publication receipts survive for pinned readers. */
    public void delete(DynamicTablePublication expected) {
        String sql = "DELETE FROM dynamic_table_publications WHERE tenant_id=? AND logical_name=? "
                + "AND revision=? AND incarnation_id=?";
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            statement.setString(1, expected.tenantId());
            statement.setString(2, expected.logicalName());
            statement.setLong(3, expected.revision());
            statement.setObject(4, UUID.fromString(expected.incarnationId()));
            if (statement.executeUpdate() != 1) throw changed();
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot drop Dynamic Table " + expected.logicalName(), failure);
        }
    }

    private static DynamicTableRefreshAttempt issue(Connection connection, DynamicTablePublication publication,
            DynamicTableRefreshIssuer issuer) throws SQLException {
        String[] parts = publication.logicalName().split("\\.", -1);
        if (parts.length != 3 || java.util.Arrays.stream(parts)
                .anyMatch(part -> !part.matches("[A-Za-z_][A-Za-z_0-9]*"))) {
            throw new IllegalArgumentException("Refresh candidate requires a qualified logical identity");
        }
        String candidate = parts[0] + "." + parts[1] + ".__pg_dt_" + UUID.randomUUID().toString().replace("-", "");
        String sql = "INSERT INTO dynamic_table_refresh_attempts (tenant_id,execution_id,logical_name,incarnation_id,"
                + "base_revision,principal,owner_id,owner_incarnation,owner_token,owner_acquired_at,owner_expires_at,"
                + "control_epoch,candidate_table,lifecycle_state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'PREPARING')";
        var owner = issuer.owner();
        try (var insert = connection.prepareStatement(sql)) {
            insert.setString(1, publication.tenantId());
            insert.setString(2, owner.executionId());
            insert.setString(3, publication.logicalName());
            insert.setObject(4, UUID.fromString(publication.incarnationId()));
            insert.setLong(5, publication.revision());
            insert.setString(6, issuer.principal());
            insert.setString(7, owner.ownerId());
            insert.setString(8, owner.ownerIncarnation());
            insert.setLong(9, owner.fencingToken());
            JdbcSupport.setInstant(insert, 10, owner.acquiredAt());
            JdbcSupport.setInstant(insert, OWNER_EXPIRES_PARAMETER, owner.expiresAt());
            insert.setLong(CONTROL_EPOCH_PARAMETER, issuer.controlEpoch());
            insert.setString(CANDIDATE_PARAMETER, candidate);
            insert.executeUpdate();
        }
        return attempt(connection, publication.tenantId(), owner.executionId(), false).orElseThrow();
    }

    private static DynamicTableRefreshIssuer requireFences(Connection connection, DynamicTableRefreshIssuer issuer)
            throws SQLException {
        var current = JdbcExecutionAnchorStore.requireCurrent(connection, issuer.owner());
        try (var lock = connection.prepareStatement("SELECT epoch FROM distributed_lease "
                + "WHERE lease_key='coordinator_leader' FOR SHARE"); var rows = lock.executeQuery()) {
            if (!rows.next() || rows.getLong("epoch") != issuer.controlEpoch()) {
                throw new IllegalStateException("Dynamic Table refresh control term is absent or replaced");
            }
        }
        // Observe database time after the row lock, never in a pre-wait SELECT projection.
        try (var check = connection.prepareStatement("SELECT expires_at>clock_timestamp() AS live "
                + "FROM distributed_lease WHERE lease_key='coordinator_leader'"); var rows = check.executeQuery()) {
            if (!rows.next() || !rows.getBoolean("live")) {
                throw new IllegalStateException("Dynamic Table refresh control term is expired");
            }
        }
        current = JdbcExecutionAnchorStore.requireCurrent(connection, current);
        return new DynamicTableRefreshIssuer(current, issuer.controlEpoch(), issuer.principal());
    }

    private static Optional<DynamicTablePublication> load(Connection connection, String tenantId,
            String logicalName, boolean lock) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT " + COLUMNS
                + " FROM dynamic_table_publications WHERE tenant_id=? AND logical_name=?"
                + (lock ? " FOR UPDATE" : ""))) {
            statement.setString(1, tenantId);
            statement.setString(2, logicalName);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(read(rows)) : Optional.empty();
            }
        }
    }

    private static Optional<DynamicTableRefreshAttempt> attempt(Connection connection, String tenantId,
            String executionId, boolean lock) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM dynamic_table_refresh_attempts "
                + "WHERE tenant_id=? AND execution_id=?" + (lock ? " FOR UPDATE" : ""))) {
            statement.setString(1, tenantId);
            statement.setString(2, executionId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                var owner = new ExecutionAnchor(executionId, rows.getString("owner_id"),
                        rows.getString("owner_incarnation"), rows.getLong("owner_token"),
                        JdbcSupport.getInstant(rows, "owner_acquired_at"),
                        JdbcSupport.getInstant(rows, "owner_expires_at"));
                var issuer = new DynamicTableRefreshIssuer(owner, rows.getLong("control_epoch"),
                        rows.getString("principal"));
                var state = DynamicTableRefreshAttempt.State.valueOf(rows.getString("lifecycle_state"));
                var proof = state == DynamicTableRefreshAttempt.State.PUBLISHED ? new NativeWriteFinalization(
                        NativeWriteFinalization.Outcome.COMMITTED, rows.getLong("published_version"),
                        rows.getBytes("manifest_sha256")) : null;
                return Optional.of(new DynamicTableRefreshAttempt(tenantId, rows.getString("logical_name"),
                        rows.getString("incarnation_id"), rows.getLong("base_revision"), issuer,
                        rows.getString("candidate_table"), state, rows.getString("commit_fingerprint"), proof));
            }
        }
    }

    private static void requireDefinition(DynamicTablePublication actual, DynamicTableDefinition definition) {
        if (!actual.tenantId().equals(definition.tenantId()) || !actual.logicalName().equals(definition.logicalName())
                || !actual.definitionSql().equals(definition.definitionSql())
                || !Objects.equals(actual.bindingCatalog(), definition.bindingCatalog())
                || !Objects.equals(actual.bindingSchema(), definition.bindingSchema())) {
            throw changed();
        }
    }

    private static void requireScope(DynamicTablePublication actual, DynamicTableRefreshAttempt attempt) {
        if (!actual.tenantId().equals(attempt.tenantId()) || !actual.logicalName().equals(attempt.logicalName())
                || !actual.incarnationId().equals(attempt.incarnationId())
                || attempt.state() == DynamicTableRefreshAttempt.State.PREPARING
                        && actual.revision() != attempt.baseRevision()) {
            throw changed();
        }
    }

    private static void requireAttemptIdentity(DynamicTableRefreshAttempt expected,
            DynamicTableRefreshAttempt actual) {
        if (!expected.tenantId().equals(actual.tenantId()) || !expected.logicalName().equals(actual.logicalName())
                || !expected.incarnationId().equals(actual.incarnationId())
                || expected.baseRevision() != actual.baseRevision()
                || !expected.issuer().equals(actual.issuer())
                || !expected.candidateTable().equals(actual.candidateTable())) {
            throw new IllegalArgumentException("Refresh candidate differs from its durable attempt identity");
        }
    }

    private static void requirePrincipal(DynamicTableRefreshAttempt attempt, DynamicTableRefreshIssuer issuer) {
        if (!attempt.issuer().principal().equals(issuer.principal())) {
            throw new IllegalArgumentException("Refresh principal differs from its durable attempt");
        }
    }

    private static void lockAttempt(Connection connection, String tenant, String execution) throws SQLException {
        try (var lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
            lock.setString(1, "dynamic-refresh:" + tenant.length() + ":" + tenant + execution);
            lock.executeQuery().close();
        }
    }

    @FunctionalInterface
    private interface RefreshTransaction<T> {
        T apply(Connection connection, DynamicTableRefreshIssuer admitted) throws SQLException;
    }

    private <T> T transaction(String operation, DynamicTableRefreshIssuer issuer,
                              RefreshTransaction<T> body) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                var admitted = requireFences(connection, issuer);
                T result = body.apply(connection, admitted);
                requireFences(connection, issuer);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error failure) {
                try {
                    connection.rollback();
                } catch (SQLException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot " + operation + " Dynamic Table publication", failure);
        }
    }

    private static IllegalStateException changed() {
        return new IllegalStateException("Dynamic Table publication changed during refresh");
    }

    private static DynamicTablePublication read(ResultSet result) throws SQLException {
        return new DynamicTablePublication(result.getString("tenant_id"), result.getString("logical_name"),
                result.getString("incarnation_id"), result.getString("definition_sql"),
                result.getString("binding_catalog"), result.getString("binding_schema"),
                result.getString("published_table"), result.getLong("revision"),
                DynamicTablePublication.State.valueOf(result.getString("lifecycle_state")),
                result.getTimestamp("created_at").toInstant(), result.getTimestamp("updated_at").toInstant());
    }
}
