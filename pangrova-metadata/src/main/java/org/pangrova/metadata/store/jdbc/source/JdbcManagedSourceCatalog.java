package org.pangrova.metadata.store.jdbc.source;

import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.spi.connector.metadata.AcceptedSourceSnapshot;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog;
import org.pangrova.spi.connector.metadata.PreparedSourcePublication;
import org.pangrova.spi.connector.metadata.SourcePublicationReservation;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.SourcePublicationTransaction;
import org.pangrova.spi.coordination.WriteCommitEntry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.pangrova.metadata.store.jdbc.source.JdbcManagedSourceCatalogMappings.bindCatalog;
import static org.pangrova.metadata.store.jdbc.source.JdbcManagedSourceCatalogMappings.bindHead;
import static org.pangrova.metadata.store.jdbc.source.JdbcManagedSourceCatalogMappings.bindKey;
import static org.pangrova.metadata.store.jdbc.source.JdbcManagedSourceCatalogMappings.bindNamespace;

/** Sole metadata authority for managed immutable source manifests and their durable write intents. */
public final class JdbcManagedSourceCatalog implements ManagedSourceCatalog {
    private final DataSource dataSource;
    private final Deployment deployment;
    private final Function<String, ExecutionAnchor> currentOwner;
    private final SourcePublicationTransaction publicationTransaction;

    /** Borrows the assembly-owned source; transactions close connections, never the shared source. */
    public JdbcManagedSourceCatalog(DataSource dataSource, Deployment deployment,
        Function<String, ExecutionAnchor> currentOwner, SourcePublicationTransaction publicationTransaction) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.deployment = Objects.requireNonNull(deployment);
        this.currentOwner = Objects.requireNonNull(currentOwner);
        this.publicationTransaction = Objects.requireNonNull(publicationTransaction);
    }

    @Override public Deployment deployment() { return deployment; }

    @Override public void createSchema(String tenant, String schema) {
        text(schema);
        transaction(connection -> {
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, tenant);
            try (var statement = connection.prepareStatement("INSERT INTO managed_source_schemas VALUES(?,?,?,?) "
                + "ON CONFLICT DO NOTHING")) {
                bindNamespace(statement, deployment, tenant, schema);
                statement.executeUpdate();
            }
            return null;
        });
    }

    @Override public List<String> listSchemas(String tenant) {
        return transaction(connection -> {
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, tenant);
            try (var statement = connection.prepareStatement("SELECT schema_name FROM managed_source_schemas "
                + "WHERE catalog_name=? AND catalog_incarnation=? AND tenant_id=? ORDER BY schema_name")) {
                bindCatalog(statement, deployment, tenant);
                try (var result = statement.executeQuery()) {
                    List<String> values = new ArrayList<>();
                    while (result.next()) values.add(result.getString(1));
                    return List.copyOf(values);
                }
            }
        });
    }

    @Override public List<SchemaTableName> listTables(String tenant) {
        return transaction(connection -> {
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, tenant);
            try (var statement = connection.prepareStatement(
                "SELECT schema_name,table_name FROM managed_source_heads "
                + "WHERE catalog_name=? AND catalog_incarnation=? AND tenant_id=? ORDER BY "
                + "schema_name,table_name")) {
                bindCatalog(statement, deployment, tenant);
                try (var result = statement.executeQuery()) {
                    List<SchemaTableName> values = new ArrayList<>();
                    while (result.next()) values.add(new SchemaTableName(result.getString(1), result.getString(2)));
                    return List.copyOf(values);
                }
            }
        });
    }

    @Override public void create(TableState table) {
        if (table.version() != 0 || table.revision() != 1 || table.generation() != 1 || table.rows() != 0) {
            throw new IllegalArgumentException("New source head must be an empty first generation");
        }
        transaction(connection -> {
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, table.key().tenant());
            JdbcSourceNamespace.requireCurrent(connection, deployment, table.datasetUri());
            try (var statement = connection.prepareStatement("INSERT INTO managed_source_heads VALUES "
                + "(?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                bindHead(statement, deployment, table);
                statement.executeUpdate();
            }
            return null;
        });
    }

    @Override public Optional<TableState> load(TableKey key) {
        return transaction(connection -> {
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, key.tenant());
            var table = JdbcManagedSourceCatalogMappings.head(connection, deployment, key, false);
            if (table != null) JdbcSourceNamespace.requireCurrent(connection, deployment, table.datasetUri());
            return Optional.ofNullable(table);
        });
    }

    @Override public void drop(TableState expected) {
        transaction(connection -> {
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, expected.key().tenant());
            JdbcManagedSourceCatalogMappings.requireBase(
                JdbcManagedSourceCatalogMappings.head(connection, deployment, expected.key(), true), expected);
            JdbcSourceNamespace.requireCurrent(connection, deployment, expected.datasetUri());
            try (var statement = connection.prepareStatement(
                "DELETE FROM managed_source_heads WHERE " + JdbcManagedSourceCatalogMappings.HEAD_KEY)) {
                bindKey(statement, deployment, expected.key());
                if (statement.executeUpdate() != 1) throw new IllegalStateException("Source head disappeared");
            }
            // Historical memberships and reservations retain all referenced objects after a SQL drop.
            return null;
        });
    }

    @Override public Snapshot snapshot(TableState selector) {
        if (selector.version() <= 0) throw new IllegalArgumentException("Snapshot requires a positive version");
        return transaction(connection -> {
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, selector.key().tenant());
            JdbcManagedSourceCatalogMappings.requireIncarnation(
                JdbcManagedSourceCatalogMappings.head(connection, deployment, selector.key(), false), selector);
            JdbcSourceNamespace.requireCurrent(connection, deployment, selector.datasetUri());
            return JdbcManagedSourceCatalogMappings.exactSnapshot(connection, deployment, selector);
        });
    }

    @Override public SourcePublicationReservation reserve(TableState base, List<byte[]> fragments,
        DistributedWriteCommitContext context) {
        ExecutionAnchor expected = currentOwner.apply(context.executionId());
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            publicationTransaction.requireCurrent(connection, expected);
            var intent = publicationTransaction.lockIntent(connection, context.executionId());
            JdbcManagedSourceCatalogMappings.requireIntent(intent, base, context, fragments, deployment);
            if (intent.state() == WriteCommitEntry.State.ABORTED) {
                throw new IllegalStateException("Source has an abort barrier");
            }
            JdbcManagedSourceCatalogMappings.requireProvenanceOwner(intent, expected);
            JdbcManagedSourceCatalogMappings.requireWriteAuthority(connection, intent);
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, base.key().tenant());
            JdbcManagedSourceCatalogMappings.requireBase(
                JdbcManagedSourceCatalogMappings.head(connection, deployment, base.key(), true), base);
            JdbcSourceNamespace.requireCurrent(connection, deployment, base.datasetUri());
            var baseSnapshot = base.version() == 0 ? Optional.<AcceptedSourceSnapshot>empty()
                : Optional.of(JdbcManagedSourceCatalogMappings.exactSnapshot(connection, deployment, base)
                .reference());
            var reservation = new SourcePublicationReservation(base, context.executionId(),
                context.commitFingerprint(),
                "_pangrova/prepared/" + context.commitFingerprint() + ".manifest", baseSnapshot);
            JdbcManagedSourceCatalogMappings.insertReservation(connection, deployment, reservation, intent);
            JdbcManagedSourceCatalogMappings.requireReservation(connection, deployment, reservation, false);
            publicationTransaction.requireCurrent(connection, expected);
            return reservation;
        });
    }

    @Override public org.pangrova.spi.connector.metadata.SourcePrepareInvocation issuePreparation(
        SourcePublicationReservation reservation, int pageIndex, String afterPath, String workerEndpoint,
        byte[] requestSha256) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        return preparationTransaction(reservation, expected, connection -> {
            String state = JdbcManagedSourceCatalogMappings.requireReservation(
                connection, deployment, reservation, true);
            if (!(state.equals("PREPARING") || state.equals("PREPARED"))) {
                throw new IllegalStateException("Preparation cannot cross source publication or abort barrier");
            }
            return JdbcSourcePreparationInvocations.issue(connection, reservation,
                expected, pageIndex, afterPath, workerEndpoint, requestSha256);
        });
    }

    @Override public void acknowledgePreparation(SourcePublicationReservation reservation,
        org.pangrova.spi.connector.metadata.SourcePrepareInvocation invocation, byte[] responseSha256) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        preparationTransaction(reservation, expected, connection -> {
            JdbcManagedSourceCatalogMappings.requireReservation(connection, deployment, reservation, true);
            JdbcSourcePreparationInvocations.acknowledge(connection, reservation, invocation, responseSha256);
            return null;
        });
    }

    private <T> T preparationTransaction(SourcePublicationReservation reservation, ExecutionAnchor expected,
        SqlOperation<T> work) {
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            publicationTransaction.requireCurrent(connection, expected);
            var intent = publicationTransaction.lockIntent(connection, reservation.executionId());
            JdbcManagedSourceCatalogMappings.requireReservationIntent(reservation, intent, deployment);
            JdbcManagedSourceCatalogMappings.requireProvenanceOwner(intent, expected);
            JdbcManagedSourceCatalogMappings.requireWriteAuthority(connection, intent);
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, reservation.base().key().tenant());
            JdbcManagedSourceCatalogMappings.head(connection, deployment, reservation.base().key(), true);
            JdbcSourceNamespace.requireCurrent(connection, deployment, reservation.base().datasetUri());
            T value = work.execute(connection);
            publicationTransaction.requireCurrent(connection, expected);
            return value;
        });
    }

    @Override public void registerObjects(SourcePublicationReservation reservation,
        PreparedSourcePublication publication,
        org.pangrova.spi.connector.metadata.SourceObjectPage page) {
        JdbcManagedSourceCatalogMappings.requireCandidateReference(reservation, publication, deployment);
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            publicationTransaction.requireCurrent(connection, expected);
            var intent = publicationTransaction.lockIntent(connection, reservation.executionId());
            JdbcManagedSourceCatalogMappings.requireReservationIntent(reservation, intent, deployment);
            JdbcManagedSourceCatalogMappings.requireProvenanceOwner(intent, expected);
            JdbcManagedSourceCatalogMappings.requireWriteAuthority(connection, intent);
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, reservation.base().key().tenant());
            JdbcManagedSourceCatalogMappings.requireBase(JdbcManagedSourceCatalogMappings.head(
                connection, deployment, reservation.base().key(), true), reservation.base());
            JdbcSourceNamespace.requireCurrent(connection, deployment, reservation.base().datasetUri());
            String state = JdbcManagedSourceCatalogMappings.requireReservation(
                connection, deployment, reservation, false);
            if (!(state.equals("PREPARING") || state.equals("PREPARED"))) {
                throw new IllegalStateException("Source object pages require an unpublished candidate");
            }
            JdbcSourcePreparationInvocations.requireJoinedPage(connection, reservation, page);
            JdbcSourceObjectRegistration.register(connection, deployment, reservation, publication, page);
            publicationTransaction.requireCurrent(connection, expected);
            return null;
        });
    }

    @Override public Snapshot accept(SourcePublicationReservation reservation, PreparedSourcePublication publication,
        TableState next) {
        JdbcManagedSourceCatalogMappings.requireCandidate(reservation, publication, next, deployment);
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            publicationTransaction.requireCurrent(connection, expected);
            var intent = publicationTransaction.lockIntent(connection, reservation.executionId());
            JdbcManagedSourceCatalogMappings.requireReservationIntent(reservation, intent, deployment);
            JdbcManagedSourceCatalogMappings.requireProvenanceOwner(intent, expected);
            JdbcManagedSourceCatalogMappings.requireWriteAuthority(connection, intent);
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, next.key().tenant());
            TableState current = JdbcManagedSourceCatalogMappings.head(connection, deployment, next.key(), true);
            JdbcManagedSourceCatalogMappings.requireIncarnation(current, reservation.base());
            JdbcSourceNamespace.requireCurrent(connection, deployment, current.datasetUri());
            String state = JdbcManagedSourceCatalogMappings.requireReservation(
                connection, deployment, reservation, true);
            if (state.equals("ACCEPTED")) {
                Snapshot accepted = JdbcManagedSourceCatalogMappings.byFingerprint(
                    connection, deployment, reservation.base(), reservation.fingerprint())
                    .orElseThrow();
                JdbcManagedSourceCatalogMappings.requireSameSnapshot(accepted, publication, next);
                publicationTransaction.commitPublication(connection, intent,
                    JdbcManagedSourceCatalogMappings.finalization(accepted));
                publicationTransaction.requireCurrent(connection, expected);
                return accepted;
            }
            JdbcManagedSourceCatalogMappings.requireBase(current, reservation.base());
            if (intent.state() != WriteCommitEntry.State.PENDING || intent.publication() != null) {
                throw new IllegalStateException("Unaccepted source requires its exact pending intent");
            }
            JdbcSourceObjectRegistration.requireHeader(connection, reservation, publication, true);
            JdbcManagedSourceCatalogMappings.acceptCandidate(connection, reservation, publication, next);
            JdbcManagedSourceCatalogMappings.updateHead(connection, deployment, next);
            Snapshot accepted = new Snapshot(next, publication.candidate());
            publicationTransaction.commitPublication(connection, intent,
                JdbcManagedSourceCatalogMappings.finalization(accepted));
            // All possibly waiting catalog, candidate, object and journal writes have completed.
            // The exact anchor remains locked; this final independent wall-clock sample is followed by COMMIT only.
            publicationTransaction.requireCurrent(connection, expected);
            return accepted;
        });
    }

    @Override public Optional<Snapshot> accepted(TableState base, List<byte[]> fragments,
        DistributedWriteCommitContext context) {
        ExecutionAnchor expected = currentOwner.apply(context.executionId());
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            publicationTransaction.requireCurrent(connection, expected);
            var intent = publicationTransaction.lockIntent(connection, context.executionId());
            JdbcManagedSourceCatalogMappings.requireIntent(intent, base, context, fragments, deployment);
            JdbcManagedSourceCatalogMappings.requireProvenanceOwner(intent, expected);
            JdbcManagedSourceCatalogMappings.requireWriteAuthority(connection, intent);
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, base.key().tenant());
            JdbcManagedSourceCatalogMappings.requireIncarnation(
                JdbcManagedSourceCatalogMappings.head(connection, deployment, base.key(), false), base);
            JdbcSourceNamespace.requireCurrent(connection, deployment, base.datasetUri());
            var accepted = JdbcManagedSourceCatalogMappings.byFingerprint(
                connection, deployment, base, context.commitFingerprint());
            if (accepted.isPresent()) {
                var reference = accepted.orElseThrow().reference();
                // Read reauthorization can use a later revision; original write replay cannot.
                if (reference.acceptedOriginRevision() != deployment.revision()
                    || intent.state() != WriteCommitEntry.State.COMMITTED || intent.publication() == null
                    || intent.publication().publishedVersion() != reference.version()
                    || !Arrays.equals(intent.publication().manifestSha256(), reference.manifestSha256())) {
                    throw new SecurityException("Accepted write replay differs from its original epoch or "
                        + "exact committed intent");
                }
            } else if (intent.state() != WriteCommitEntry.State.PENDING || intent.publication() != null) {
                throw new IllegalStateException("Source intent state has no matching accepted publication");
            }
            publicationTransaction.requireCurrent(connection, expected);
            return accepted;
        });
    }

    @Override public Optional<org.pangrova.spi.connector.metadata.SourceCleanupBatch> reserveCleanup(
        SourcePublicationReservation reservation) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        return preparationTransaction(reservation, expected, connection -> {
            String state = JdbcManagedSourceCatalogMappings.requireReservation(
                connection, deployment, reservation, true);
            if (!(state.equals("ACCEPTED") || state.equals("ABORTED"))) return Optional.empty();
            return JdbcSourceObjectCleanup.reserve(connection, reservation, state.equals("ACCEPTED"));
        });
    }

    @Override public void completeCleanup(SourcePublicationReservation reservation,
        org.pangrova.spi.connector.metadata.SourceCleanupBatch batch) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        preparationTransaction(reservation, expected, connection -> {
            String state = JdbcManagedSourceCatalogMappings.requireReservation(
                connection, deployment, reservation, true);
            if (!(state.equals("ACCEPTED") || state.equals("ABORTED"))) {
                throw new IllegalStateException("Object cleanup cannot cross an unpublished candidate");
            }
            JdbcSourceObjectCleanup.complete(connection, reservation, batch);
            return null;
        });
    }

    @Override public org.pangrova.spi.connector.metadata.DistributedWriteAbortReceipt abort(
        SourcePublicationReservation reservation) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            publicationTransaction.requireCurrent(connection, expected);
            var intent = publicationTransaction.lockIntent(connection, reservation.executionId());
            JdbcManagedSourceCatalogMappings.requireReservationIntent(reservation, intent, deployment);
            JdbcManagedSourceCatalogMappings.requireProvenanceOwner(intent, expected);
            JdbcManagedSourceCatalogMappings.requireWriteAuthority(connection, intent);
            JdbcManagedSourceCatalogMappings.requireCatalog(connection, deployment, reservation.base().key().tenant());
            // A missing/replaced SQL head is not itself evidence of an unpublished candidate.
            JdbcManagedSourceCatalogMappings.head(connection, deployment, reservation.base().key(), true);
            // A barrier can precede the first RPC. Candidate insertion and journal ABORTED
            // share this transaction, so no later reserve/page/accept may cross it.
            if (intent.publication() != null || intent.state() == WriteCommitEntry.State.COMMITTED) {
                throw new IllegalStateException("Published source intent cannot be aborted");
            }
            if (reservation.base().version() > 0) {
                var acceptedBase = JdbcManagedSourceCatalogMappings.exactSnapshot(
                    connection, deployment, reservation.base()).reference();
                if (!JdbcManagedSourceCatalogMappings.sameReference(acceptedBase,
                    reservation.baseSnapshot().orElseThrow())) {
                    throw new IllegalArgumentException("Abort accepted base differs from historical membership");
                }
            }
            JdbcManagedSourceCatalogMappings.insertReservation(connection, deployment, reservation, intent);
            String state = JdbcManagedSourceCatalogMappings.requireReservation(
                connection, deployment, reservation, true);
            if (state.equals("ACCEPTED")) throw new IllegalStateException("Accepted source cannot be aborted");
            try (var statement = connection.prepareStatement(
                "UPDATE managed_source_publications SET state='ABORTED' "
                + "WHERE fingerprint=? AND state IN ('PREPARING','PREPARED','ABORTED')")) {
                statement.setString(1, reservation.fingerprint());
                if (statement.executeUpdate() != 1) {
                    throw new IllegalStateException("Source abort barrier was lost");
                }
            }
            publicationTransaction.abortIntent(connection, intent);
            publicationTransaction.requireCurrent(connection, expected);
            return new org.pangrova.spi.connector.metadata.DistributedWriteAbortReceipt(
                reservation.executionId(), reservation.fingerprint());
        });
    }

    private <T> T transaction(SqlOperation<T> operation) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = operation.execute(connection);
                connection.commit();
                return result;
            } catch (RuntimeException | SQLException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    failure.addSuppressed(rollback);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            // A lost COMMIT acknowledgement is uncertain; callers retain the reservation and resolve PG accepted truth.
            throw new IllegalStateException("Managed source authority transaction is unconfirmed", failure);
        }
    }

    private static void text(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Source identity is required");
    }

    @FunctionalInterface private interface SqlOperation<T> { T execute(Connection connection) throws SQLException; }
}
