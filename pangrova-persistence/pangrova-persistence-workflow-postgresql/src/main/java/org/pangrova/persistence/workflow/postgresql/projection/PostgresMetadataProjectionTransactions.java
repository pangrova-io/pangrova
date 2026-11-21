package org.pangrova.persistence.workflow.postgresql.projection;

import org.pangrova.persistence.catalog.postgresql.projection.CatalogProjectionSql;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import com.fasterxml.jackson.core.type.TypeReference;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionConflictException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionOutcomeUnknownException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionTransactions;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Sole projection mutation coordinator; borrows the DataSource and owns each acquired connection. */
public final class PostgresMetadataProjectionTransactions implements MetadataProjectionTransactions {
    private final DataSource dataSource;

    public PostgresMetadataProjectionTransactions(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    // A reserved operation can be executed/recovered by its original ID. Refuse to
    // advance until these reservations have completed under their own observation.
    private static void requireNoPendingWork(Connection connection, CatalogDeploymentIdentity deployment)
            throws SQLException {
        try (var pending = connection.prepareStatement(
                "SELECT 1 FROM metadata_projection_intents WHERE catalog_name=? AND state='PENDING' "
                        + "UNION ALL SELECT 1 FROM metadata_sync_intents "
                        + "WHERE catalog_name=? AND state='PENDING'")) {
            pending.setString(1, deployment.catalogName());
            pending.setString(2, deployment.catalogName());
            try (var rows = pending.executeQuery()) {
                if (rows.next()) {
                    throw new MetadataProjectionConflictException("Recover pending catalog work before observing");
                }
            }
        }
    }

    private static long advanceObservationEpoch(Connection connection, CatalogDeploymentIdentity deployment)
            throws SQLException {
        try (var advance = connection.prepareStatement("UPDATE catalog_definitions SET "
                + "observation_epoch=observation_epoch+1 WHERE name=? RETURNING observation_epoch")) {
            advance.setString(1, deployment.catalogName());
            try (var rows = advance.executeQuery()) {
                if (!rows.next()) {
                    throw new MetadataProjectionConflictException("Catalog deployment no longer exists");
                }
                return rows.getLong(1);
            }
        }
    }

    @Override
    public long beginObservation(CatalogDeploymentIdentity deployment) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                lockDeployment(connection, deployment);
                requireNoPendingWork(connection, deployment);
                long epoch = advanceObservationEpoch(connection, deployment);
                connection.commit();
                return epoch;
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            // No epoch is returned on a lost acknowledgement, so the caller cannot publish from it.
            throw new MetastoreException("Cannot begin catalog observation: " + deployment, failure);
        }
    }

    @Override
    public MetadataProjectionReceipt apply(long observationEpoch, MetadataProjectionMutation mutation) {
        Objects.requireNonNull(mutation, "mutation");
        ProjectionRequestValidation.validate(mutation);
        Optional<MetadataProjectionReceipt> replay = transact(observationEpoch, mutation, true);
        return replay.orElseGet(() -> transact(observationEpoch, mutation, false).orElseThrow());
    }

    private Optional<MetadataProjectionReceipt> transact(long observationEpoch, MetadataProjectionMutation mutation,
            boolean reserve) {
        boolean commitStarted = false;
        try (Connection connection = dataSource.getConnection()) {
            // Receipt rechecks must observe a winner that committed while the row lock was awaited.
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                Optional<MetadataProjectionReceipt> receipt = reserve ? reserveOrReplay(connection,
                        observationEpoch, mutation)
                        : Optional.of(applyOrReplay(connection, mutation));
                commitStarted = true;
                connection.commit();
                return receipt;
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException | RuntimeException failure) {
            if (commitStarted) {
                throw new MetadataProjectionOutcomeUnknownException(mutation.operationId(), failure);
            }
            if (failure instanceof SQLException sql && "23505".equals(sql.getSQLState())) {
                // Different catalog locks can race on the global operation ID. This transaction
                // has rolled back; inspect the winner rather than rewriting either projection.
                Optional<MetadataProjectionReceipt> winner = confirmCollision(mutation.operationId(), failure);
                if (winner.isPresent()) return Optional.of(requireReplay(mutation, winner.orElseThrow()));
                throw new MetadataProjectionConflictException("Projection operation ID is already reserved");
            }
            if (!reserve && !(failure instanceof MetadataProjectionConflictException)) {
                throw new MetadataProjectionPendingException(mutation.operationId(), failure);
            }
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new MetastoreException("Metadata projection transaction failed: " + mutation.operationId()
                    + " sqlState=" + ((SQLException) failure).getSQLState(), failure);
        }
    }

    @Override
    public void recoverPending(CatalogDeploymentIdentity deployment) {
        Objects.requireNonNull(deployment, "deployment");
        UUID pendingId = null;
        boolean commitStarted = false;
        try (Connection connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                lockDeployment(connection, deployment);
                var pending = ProjectionIntentJournal.pending(connection, deployment.catalogName());
                if (pending.isPresent()) {
                    pendingId = pending.orElseThrow().mutation().operationId();
                    recover(connection, deployment, pending.orElseThrow());
                }
                commitStarted = true;
                connection.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException | RuntimeException failure) {
            if (commitStarted && pendingId != null) {
                throw new MetadataProjectionOutcomeUnknownException(pendingId, failure);
            }
            if (pendingId != null && !(failure instanceof MetadataProjectionConflictException)) {
                throw new MetadataProjectionPendingException(pendingId, failure);
            }
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new MetastoreException("Cannot recover pending metadata projection: " + deployment, failure);
        }
    }

    private static Optional<MetadataProjectionReceipt> reserveOrReplay(
            Connection connection, long observationEpoch, MetadataProjectionMutation mutation) throws SQLException {
        var receipt = readReceipt(connection, mutation.operationId());
        if (receipt.isPresent()) return Optional.of(requireReplay(mutation, receipt.orElseThrow()));
        try {
            lockDeployment(connection, mutation.deployment());
        } catch (MetadataProjectionConflictException stale) {
            receipt = readReceipt(connection, mutation.operationId());
            if (receipt.isPresent()) return Optional.of(requireReplay(mutation, receipt.orElseThrow()));
            throw stale;
        }
        receipt = readReceipt(connection, mutation.operationId());
        if (receipt.isPresent()) return Optional.of(requireReplay(mutation, receipt.orElseThrow()));
        var existing = ProjectionIntentJournal.find(connection, mutation.operationId());
        if (existing.isPresent()) {
            existing.orElseThrow().requirePending(mutation);
            return Optional.empty();
        }
        var pending = ProjectionIntentJournal.pending(connection, mutation.deployment().catalogName());
        if (pending.isPresent()) {
            throw new MetadataProjectionPendingException(pending.orElseThrow().mutation().operationId(),
                    new IllegalStateException("Recover the existing catalog operation before submitting a new ID"));
        }
        if (!CatalogProjectionSql.isCurrentObservation(connection, mutation.deployment().catalogName(),
                observationEpoch)) {
            throw new MetadataProjectionConflictException("Catalog observation has been superseded");
        }
        ProjectionIntentJournal.insert(connection, mutation);
        return Optional.empty();
    }

    private static void recover(Connection connection, CatalogDeploymentIdentity current,
            ProjectionIntentJournal.Intent pending) throws SQLException {
        // The current deployment row is held before inspecting an older intent. Its old executor
        // must acquire the same row and revalidate authority before any effect can be published.
        if (!current.equals(pending.mutation().deployment())) {
            ProjectionIntentJournal.finish(connection, pending.mutation().operationId(), "REJECTED");
        } else {
            applyOrReplay(connection, pending.mutation());
        }
    }

    @Override
    public Optional<MetadataProjectionReceipt> findReceipt(UUID operationId) {
        Objects.requireNonNull(operationId, "operationId");
        try (Connection connection = dataSource.getConnection()) {
            return readReceipt(connection, operationId);
        } catch (SQLException failure) {
            throw new MetastoreException("Cannot confirm metadata projection: " + operationId, failure);
        }
    }

    private Optional<MetadataProjectionReceipt> confirmCollision(UUID id, Throwable primary) {
        try {
            return findReceipt(id);
        } catch (RuntimeException confirmationFailure) {
            if (primary != confirmationFailure) primary.addSuppressed(confirmationFailure);
            throw new MetastoreException("Cannot confirm projection operation ID collision: " + id, primary);
        }
    }

    private static MetadataProjectionReceipt applyOrReplay(Connection connection, MetadataProjectionMutation mutation)
            throws SQLException {
        Optional<MetadataProjectionReceipt> previous = readReceipt(connection, mutation.operationId());
        if (previous.isPresent()) return requireReplay(mutation, previous.orElseThrow());
        Set<String> tenants;
        try {
            tenants = lockDeployment(connection, mutation.deployment());
        } catch (MetadataProjectionConflictException stale) {
            // An identical in-flight operation may have committed after our optimistic receipt
            // read but before this deployment was replaced. Confirm history, never re-authorize it.
            previous = readReceipt(connection, mutation.operationId());
            if (previous.isPresent()) return requireReplay(mutation, previous.orElseThrow());
            throw stale;
        }
        previous = readReceipt(connection, mutation.operationId());
        if (previous.isPresent()) return requireReplay(mutation, previous.orElseThrow());
        ProjectionIntentJournal.find(connection, mutation.operationId()).orElseThrow(
                () -> new MetadataProjectionConflictException("Projection has no durable intent"))
                .requirePending(mutation);
        MetadataProjectionReceipt receipt = mutate(connection, mutation, tenants);
        if (receipt.changed()) insertInvalidation(connection, mutation);
        insertReceipt(connection, receipt);
        ProjectionIntentJournal.finish(connection, mutation.operationId(), "APPLIED");
        return receipt;
    }

    private static MetadataProjectionReceipt requireReplay(MetadataProjectionMutation requested,
            MetadataProjectionReceipt receipt) {
        if (!requested.equals(receipt.mutation())) {
            throw new MetadataProjectionConflictException("Projection operation ID already has different content");
        }
        return receipt;
    }

    private static Set<String> lockDeployment(Connection connection, CatalogDeploymentIdentity scope)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT incarnation_id, revision, allowed_tenants FROM catalog_definitions WHERE name=? FOR UPDATE")) {
            statement.setString(1, scope.catalogName());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !scope.incarnationId().equals(rows.getString(1))
                        || scope.revision() != rows.getLong(2)) {
                    throw new MetadataProjectionConflictException(
                            "Metadata projection deployment is no longer current");
                }
                try {
                    return Set.copyOf(CanonicalJsonMapper.readValue(
                            rows.getString(3), new TypeReference<Set<String>>() {}));
                } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
                    throw new MetastoreException("Cannot read current projection tenant impact", failure);
                }
            }
        }
    }

    private static MetadataProjectionReceipt mutate(Connection connection, MetadataProjectionMutation mutation,
            Set<String> tenants) throws SQLException {
        Optional<TableMetadata> previous = Optional.empty();
        boolean changed;
        if (mutation instanceof MetadataProjectionMutation.UpsertTable upsert) {
            TableMetadata table = upsert.table();
            previous = CatalogProjectionSql.load(connection,
                    new QualifiedTableRef(table.catalog(), table.schema(), table.table()));
            // A certified table observation also proves its parent schema exists. Publish both
            // under this request's fence, so incremental discovery does not depend on a full scan.
            CatalogProjectionSql.observeSchemas(connection, table.catalog(), java.util.List.of(table.schema()));
            changed = CatalogProjectionSql.upsert(connection, table);
        } else if (mutation instanceof MetadataProjectionMutation.DeleteTable deletion) {
            previous = CatalogProjectionSql.load(connection, deletion.table());
            changed = CatalogProjectionSql.delete(connection, deletion.table());
        } else if (mutation instanceof MetadataProjectionMutation.ObserveSchemas observation) {
            changed = CatalogProjectionSql.observeSchemas(
                    connection, observation.deployment().catalogName(), observation.schemas());
        } else {
            throw new IllegalArgumentException("Unsupported projection mutation");
        }
        return new MetadataProjectionReceipt(mutation, changed, previous, tenants);
    }

    private static void insertInvalidation(Connection connection, MetadataProjectionMutation mutation)
            throws SQLException {
        String key = mutation.deployment().catalogName();
        String type = "CATALOG";
        if (mutation instanceof MetadataProjectionMutation.UpsertTable upsert) {
            key = tableKey(upsert.table().catalog(), upsert.table().schema(), upsert.table().table());
            type = "TABLE";
        } else if (mutation instanceof MetadataProjectionMutation.DeleteTable deletion) {
            key = tableKey(deletion.table().catalog(), deletion.table().schema(), deletion.table().table());
            type = "TABLE";
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO invalidation_log (cache_key, invalidation_type, created_at, updated_at) "
                        + "VALUES (?, ?, NOW(), NOW())")) {
            statement.setString(1, key);
            statement.setString(2, type);
            statement.executeUpdate();
        }
    }

    private static void insertReceipt(Connection connection, MetadataProjectionReceipt receipt) throws SQLException {
        var mutation = receipt.mutation();
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO metadata_projection_mutations"
                        + "(operation_id,catalog_name,incarnation_id,revision,request_json,receipt_json) "
                        + "VALUES (?,?,?,?,?::jsonb,?::jsonb)")) {
            statement.setObject(1, mutation.operationId());
            statement.setString(2, mutation.deployment().catalogName());
            statement.setString(3, mutation.deployment().incarnationId());
            statement.setLong(4, mutation.deployment().revision());
            statement.setString(5, ProjectionReceiptCodec.request(mutation));
            statement.setString(6, ProjectionReceiptCodec.result(receipt));
            statement.executeUpdate();
        }
    }

    private static Optional<MetadataProjectionReceipt> readReceipt(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT catalog_name,incarnation_id,revision,request_json,receipt_json "
                        + "FROM metadata_projection_mutations WHERE operation_id=?")) {
            statement.setObject(1, id);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                var scope = new CatalogDeploymentIdentity(rows.getString(1), rows.getString(2), rows.getLong(3));
                return Optional.of(ProjectionReceiptCodec.decode(id, scope, rows.getString(4), rows.getString(5)));
            }
        }
    }

    private static String tableKey(String catalog, String schema, String table) {
        return catalog + "\u001F" + schema + "\u001F" + table;
    }
}
