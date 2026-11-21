package org.pangrova.persistence.catalog.postgresql.synchronization;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import org.pangrova.persistence.catalog.postgresql.enums.SyncCompletionRelation;

import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncConflictException;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncOutcomeUnknownException;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncRepository;
import org.pangrova.persistence.catalog.api.exception.MetadataSyncPendingException;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import org.pangrova.persistence.catalog.postgresql.projection.CatalogProjectionSql;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** PostgreSQL owner of source progress and immutable run receipts; the DataSource is borrowed. */
public final class PostgresMetadataSyncRepository implements MetadataSyncRepository {
    private final DataSource dataSource;

    public PostgresMetadataSyncRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public Optional<Instant> loadCheckpoint(CatalogDeploymentIdentity scope) {
        try (Connection connection = dataSource.getConnection()) {
            return readCheckpoint(connection, scope);
        } catch (SQLException failure) {
            throw new MetastoreException("Cannot read metadata sync checkpoint", failure);
        }
    }

    @Override
    public Optional<MetadataSyncCompletion> findCompletion(UUID runId) {
        try (Connection connection = dataSource.getConnection()) {
            return SyncCompletionRows.read(connection, runId, SyncCompletionRelation.RECEIPT);
        } catch (SQLException failure) {
            throw new MetastoreException("Cannot confirm metadata sync completion: " + runId, failure);
        }
    }

    @Override
    public void complete(long observationEpoch, MetadataSyncCompletion completion) {
        Objects.requireNonNull(completion, "completion");
        transact(observationEpoch, completion, true);
        transact(observationEpoch, completion, false);
    }

    private void transact(long observationEpoch, MetadataSyncCompletion completion, boolean reserve) {
        boolean commitStarted = false;
        try (Connection connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                if (reserve) reserveOrReplay(connection, observationEpoch, completion);
                else applyOrReplay(connection, completion);
                commitStarted = true;
                connection.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException | RuntimeException failure) {
            if (commitStarted) throw new MetadataSyncOutcomeUnknownException(completion.runId(), failure);
            if (failure instanceof SQLException sql && "23505".equals(sql.getSQLState())) {
                throw new MetadataSyncConflictException("Metadata sync run ID is already reserved");
            }
            if (!reserve && !(failure instanceof MetadataSyncConflictException)) {
                throw new MetadataSyncPendingException(completion.runId(), failure);
            }
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new MetastoreException("Cannot complete metadata synchronization: " + completion.runId(), failure);
        }
    }

    @Override
    public void recoverPending(CatalogDeploymentIdentity scope) {
        Objects.requireNonNull(scope, "scope");
        UUID pendingId = null;
        boolean commitStarted = false;
        try (Connection connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                lockDeployment(connection, scope);
                var pending = SyncIntentJournal.pending(connection, scope.catalogName());
                if (pending.isPresent()) {
                    var completion = pending.orElseThrow();
                    pendingId = completion.runId();
                    if (!scope.equals(completion.scope())) {
                        SyncIntentJournal.finish(connection, pendingId, "REJECTED");
                    } else {
                        applyOrReplay(connection, completion);
                    }
                }
                commitStarted = true;
                connection.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException | RuntimeException failure) {
            if (commitStarted && pendingId != null) throw new MetadataSyncOutcomeUnknownException(pendingId, failure);
            if (pendingId != null && !(failure instanceof MetadataSyncConflictException)) {
                throw new MetadataSyncPendingException(pendingId, failure);
            }
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new MetastoreException("Cannot recover metadata synchronization: " + scope, failure);
        }
    }

    private static void reserveOrReplay(Connection connection, long observationEpoch,
            MetadataSyncCompletion completion) throws SQLException {
        if (lockUnlessReplay(connection, completion)) return;
        var existing = SyncCompletionRows.read(connection, completion.runId(), SyncCompletionRelation.INTENT);
        if (existing.isPresent()) {
            if (!existing.orElseThrow().equals(completion)) {
                throw new MetadataSyncConflictException("Metadata sync run ID already has different content");
            }
            SyncIntentJournal.requirePending(connection, completion.runId());
            return;
        }
        var pending = SyncIntentJournal.pending(connection, completion.scope().catalogName());
        if (pending.isPresent()) {
            throw new MetadataSyncPendingException(pending.orElseThrow().runId(),
                    new IllegalStateException("Recover the existing catalog completion before submitting another run"));
        }
        requireCheckpoint(connection, completion);
        if (!CatalogProjectionSql.isCurrentObservation(connection, completion.scope().catalogName(),
                observationEpoch)) {
            throw new MetadataSyncConflictException("Catalog observation has been superseded");
        }
        SyncCompletionRows.insert(connection, completion, SyncCompletionRelation.INTENT);
    }

    private static void applyOrReplay(Connection connection, MetadataSyncCompletion completion) throws SQLException {
        if (lockUnlessReplay(connection, completion)) return;
        var intent = SyncCompletionRows.read(connection, completion.runId(), SyncCompletionRelation.INTENT);
        if (intent.isEmpty() || !intent.orElseThrow().equals(completion)) {
            throw new MetadataSyncConflictException("Metadata sync has no matching durable intent");
        }
        SyncIntentJournal.requirePending(connection, completion.runId());
        requireCheckpoint(connection, completion);
        SyncCompletionRows.insert(connection, completion, SyncCompletionRelation.RECEIPT);
        if (completion.nextCheckpoint().isPresent()) {
            advance(connection, completion.scope(), completion.nextCheckpoint().orElseThrow());
        }
        SyncIntentJournal.finish(connection, completion.runId(), "APPLIED");
    }

    private static boolean lockUnlessReplay(Connection connection, MetadataSyncCompletion completion)
            throws SQLException {
        if (checkReplay(connection, completion)) return true;
        try {
            lockDeployment(connection, completion.scope());
        } catch (MetadataSyncConflictException stale) {
            if (checkReplay(connection, completion)) return true;
            throw stale;
        }
        return checkReplay(connection, completion);
    }

    private static void requireCheckpoint(Connection connection, MetadataSyncCompletion completion)
            throws SQLException {
        if (!readCheckpoint(connection, completion.scope()).equals(completion.expectedCheckpoint())) {
            throw new MetadataSyncConflictException("Metadata sync checkpoint has changed");
        }
    }

    private static void lockDeployment(Connection connection, CatalogDeploymentIdentity scope) throws SQLException {
        // Serialize completion with Catalog revision/replacement and all completions for this name.
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT incarnation_id, revision FROM catalog_definitions WHERE name=? FOR UPDATE")) {
            statement.setString(1, scope.catalogName());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !scope.incarnationId().equals(rows.getString(1))
                        || scope.revision() != rows.getLong(2)) {
                    throw new MetadataSyncConflictException("Metadata synchronization deployment is no longer current");
                }
            }
        }
    }

    private static Optional<Instant> readCheckpoint(Connection connection, CatalogDeploymentIdentity scope)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT checkpoint FROM metadata_sync_progress "
                        + "WHERE catalog_name=? AND incarnation_id=? AND revision=?")) {
            bindScope(statement, scope, 1);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(Instant.parse(rows.getString(1))) : Optional.empty();
            }
        }
    }

    private static boolean checkReplay(Connection connection, MetadataSyncCompletion completion) throws SQLException {
        Optional<MetadataSyncCompletion> previous = SyncCompletionRows.read(
                connection, completion.runId(), SyncCompletionRelation.RECEIPT);
        if (previous.isPresent() && !previous.get().equals(completion)) {
            throw new MetadataSyncConflictException("Metadata sync run ID already has different content");
        }
        return previous.isPresent();
    }

    private static void advance(Connection connection, CatalogDeploymentIdentity scope, Instant checkpoint)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO metadata_sync_progress(catalog_name,incarnation_id,revision,checkpoint) VALUES (?,?,?,?) "
                        + "ON CONFLICT (catalog_name,incarnation_id,revision) "
                        + "DO UPDATE SET checkpoint=EXCLUDED.checkpoint")) {
            bindScope(statement, scope, 1);
            statement.setString(4, checkpoint.toString());
            statement.executeUpdate();
        }
    }

    private static void bindScope(PreparedStatement statement, CatalogDeploymentIdentity scope, int offset)
            throws SQLException {
        statement.setString(offset, scope.catalogName());
        statement.setString(offset + 1, scope.incarnationId());
        statement.setLong(offset + 2, scope.revision());
    }
}
