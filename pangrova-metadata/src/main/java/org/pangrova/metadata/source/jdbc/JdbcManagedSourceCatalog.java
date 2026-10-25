package org.pangrova.metadata.source.jdbc;

import org.pangrova.metadata.artifact.jdbc.GeneratedModelCurrentAuthority;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.coordination.JdbcWriteCommitJournal;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.security.authz.DeferredAuthorizationScopeCodec;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.spi.connector.metadata.AcceptedSourceSnapshot;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.connector.metadata.PreparedSourcePublication;
import org.pangrova.spi.connector.metadata.SourcePublicationReservation;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.WriteCommitEntry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Sole metadata authority for managed immutable source manifests and their durable write intents. */
public final class JdbcManagedSourceCatalog implements ManagedSourceCatalog {
    private static final String HEAD_KEY = "catalog_name=? AND catalog_incarnation=? AND tenant_id=? "
        + "AND schema_name=? AND table_name=?";
    private static final int RESERVATION_GENERATION_PARAMETER = 11;
    private static final int RESERVATION_BASE_REVISION_PARAMETER = 12;
    private static final int RESERVATION_BASE_VERSION_PARAMETER = 13;
    private static final int RESERVATION_MANIFEST_PARAMETER = 14;
    private static final int RESERVATION_VERSION_PARAMETER = 15;
    private static final int RESERVATION_OWNER_PARAMETER = 17;
    private static final int RESERVATION_OWNER_INCARNATION_PARAMETER = 18;
    private static final int RESERVATION_OWNER_TOKEN_PARAMETER = 19;
    private static final int RESERVATION_INHERITED_PARAMETER = 20;
    private static final int HEAD_ROWS_PARAMETER = 11;
    private static final int HEAD_SCHEMA_PARAMETER = 12;
    private static final int HEAD_PAYLOAD_PARAMETER = 13;
    private final DataSource dataSource;
    private final Deployment deployment;
    private final Function<String, ExecutionAnchor> currentOwner;

    /** Borrows the assembly-owned source; transactions close connections, never the shared source. */
    public JdbcManagedSourceCatalog(DataSource dataSource, Deployment deployment,
        Function<String, ExecutionAnchor> currentOwner) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.deployment = Objects.requireNonNull(deployment);
        this.currentOwner = Objects.requireNonNull(currentOwner);
    }

    @Override public Deployment deployment() { return deployment; }

    @Override public void createSchema(String tenant, String schema) {
        text(schema);
        transaction(connection -> {
            requireCatalog(connection, tenant);
            try (var statement = connection.prepareStatement("INSERT INTO managed_source_schemas VALUES(?,?,?,?) "
                + "ON CONFLICT DO NOTHING")) {
                bindNamespace(statement, tenant, schema);
                statement.executeUpdate();
            }
            return null;
        });
    }

    @Override public List<String> listSchemas(String tenant) {
        return transaction(connection -> {
            requireCatalog(connection, tenant);
            try (var statement = connection.prepareStatement("SELECT schema_name FROM managed_source_schemas "
                + "WHERE catalog_name=? AND catalog_incarnation=? AND tenant_id=? ORDER BY schema_name")) {
                bindCatalog(statement, tenant);
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
            requireCatalog(connection, tenant);
            try (var statement = connection.prepareStatement(
                "SELECT schema_name,table_name FROM managed_source_heads "
                + "WHERE catalog_name=? AND catalog_incarnation=? AND tenant_id=? ORDER BY "
                + "schema_name,table_name")) {
                bindCatalog(statement, tenant);
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
            requireCatalog(connection, table.key().tenant());
            JdbcSourceNamespace.requireCurrent(connection, deployment, table.datasetUri());
            try (var statement = connection.prepareStatement("INSERT INTO managed_source_heads VALUES "
                + "(?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                bindHead(statement, table);
                statement.executeUpdate();
            }
            return null;
        });
    }

    @Override public Optional<TableState> load(TableKey key) {
        return transaction(connection -> {
            requireCatalog(connection, key.tenant());
            var table = head(connection, key, false);
            if (table != null) JdbcSourceNamespace.requireCurrent(connection, deployment, table.datasetUri());
            return Optional.ofNullable(table);
        });
    }

    @Override public void drop(TableState expected) {
        transaction(connection -> {
            requireCatalog(connection, expected.key().tenant());
            requireBase(head(connection, expected.key(), true), expected);
            JdbcSourceNamespace.requireCurrent(connection, deployment, expected.datasetUri());
            try (var statement = connection.prepareStatement(
                "DELETE FROM managed_source_heads WHERE " + HEAD_KEY)) {
                bindKey(statement, expected.key());
                if (statement.executeUpdate() != 1) throw new IllegalStateException("Source head disappeared");
            }
            // Historical memberships and reservations retain all referenced objects after a SQL drop.
            return null;
        });
    }

    @Override public Snapshot snapshot(TableState selector) {
        if (selector.version() <= 0) throw new IllegalArgumentException("Snapshot requires a positive version");
        return transaction(connection -> {
            requireCatalog(connection, selector.key().tenant());
            requireIncarnation(head(connection, selector.key(), false), selector);
            JdbcSourceNamespace.requireCurrent(connection, deployment, selector.datasetUri());
            return exactSnapshot(connection, selector);
        });
    }

    @Override public SourcePublicationReservation reserve(TableState base, List<byte[]> fragments,
        DistributedWriteCommitContext context) {
        ExecutionAnchor expected = currentOwner.apply(context.executionId());
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            var intent = JdbcWriteCommitJournal.lockIntent(connection, context.executionId());
            requireIntent(intent, base, context, fragments);
            if (intent.state() == WriteCommitEntry.State.ABORTED) {
                throw new IllegalStateException("Source has an abort barrier");
            }
            requireProvenanceOwner(intent, expected);
            requireWriteAuthority(connection, intent);
            requireCatalog(connection, base.key().tenant());
            requireBase(head(connection, base.key(), true), base);
            JdbcSourceNamespace.requireCurrent(connection, deployment, base.datasetUri());
            var baseSnapshot = base.version() == 0 ? Optional.<AcceptedSourceSnapshot>empty()
                : Optional.of(exactSnapshot(connection, base).reference());
            var reservation = new SourcePublicationReservation(base, context.executionId(),
                context.commitFingerprint(),
                "_pangrova/prepared/" + context.commitFingerprint() + ".manifest", baseSnapshot);
            insertReservation(connection, reservation, intent);
            requireReservation(connection, reservation, false);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            return reservation;
        });
    }

    @Override public org.pangrova.spi.connector.metadata.SourcePrepareInvocation issuePreparation(
        SourcePublicationReservation reservation, int pageIndex, String afterPath, String workerEndpoint,
        byte[] requestSha256) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        return preparationTransaction(reservation, expected, connection -> {
            String state = requireReservation(connection, reservation, true);
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
            requireReservation(connection, reservation, true);
            JdbcSourcePreparationInvocations.acknowledge(connection, reservation, invocation, responseSha256);
            return null;
        });
    }

    private <T> T preparationTransaction(SourcePublicationReservation reservation, ExecutionAnchor expected,
        SqlOperation<T> work) {
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            var intent = JdbcWriteCommitJournal.lockIntent(connection, reservation.executionId());
            requireReservationIntent(reservation, intent);
            requireProvenanceOwner(intent, expected);
            requireWriteAuthority(connection, intent);
            requireCatalog(connection, reservation.base().key().tenant());
            head(connection, reservation.base().key(), true);
            JdbcSourceNamespace.requireCurrent(connection, deployment, reservation.base().datasetUri());
            T value = work.execute(connection);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            return value;
        });
    }

    @Override public void registerObjects(SourcePublicationReservation reservation,
        PreparedSourcePublication publication,
        org.pangrova.spi.connector.metadata.SourceObjectPage page) {
        requireCandidateReference(reservation, publication);
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            var intent = JdbcWriteCommitJournal.lockIntent(connection, reservation.executionId());
            requireReservationIntent(reservation, intent);
            requireProvenanceOwner(intent, expected);
            requireWriteAuthority(connection, intent);
            requireCatalog(connection, reservation.base().key().tenant());
            requireBase(head(connection, reservation.base().key(), true), reservation.base());
            JdbcSourceNamespace.requireCurrent(connection, deployment, reservation.base().datasetUri());
            String state = requireReservation(connection, reservation, false);
            if (!(state.equals("PREPARING") || state.equals("PREPARED"))) {
                throw new IllegalStateException("Source object pages require an unpublished candidate");
            }
            JdbcSourcePreparationInvocations.requireJoinedPage(connection, reservation, page);
            JdbcSourceObjectRegistration.register(connection, deployment, reservation, publication, page);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            return null;
        });
    }

    @Override public Snapshot accept(SourcePublicationReservation reservation, PreparedSourcePublication publication,
        TableState next) {
        requireCandidate(reservation, publication, next);
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            var intent = JdbcWriteCommitJournal.lockIntent(connection, reservation.executionId());
            requireReservationIntent(reservation, intent);
            requireProvenanceOwner(intent, expected);
            requireWriteAuthority(connection, intent);
            requireCatalog(connection, next.key().tenant());
            TableState current = head(connection, next.key(), true);
            requireIncarnation(current, reservation.base());
            JdbcSourceNamespace.requireCurrent(connection, deployment, current.datasetUri());
            String state = requireReservation(connection, reservation, true);
            if (state.equals("ACCEPTED")) {
                Snapshot accepted = byFingerprint(connection, reservation.base(), reservation.fingerprint())
                    .orElseThrow();
                requireSameSnapshot(accepted, publication, next);
                JdbcWriteCommitJournal.commitPublication(connection, intent, finalization(accepted));
                JdbcExecutionAnchorStore.requireCurrent(connection, expected);
                return accepted;
            }
            requireBase(current, reservation.base());
            if (intent.state() != WriteCommitEntry.State.PENDING || intent.publication() != null) {
                throw new IllegalStateException("Unaccepted source requires its exact pending intent");
            }
            JdbcSourceObjectRegistration.requireHeader(connection, reservation, publication, true);
            acceptCandidate(connection, reservation, publication, next);
            updateHead(connection, next);
            Snapshot accepted = new Snapshot(next, publication.candidate());
            JdbcWriteCommitJournal.commitPublication(connection, intent, finalization(accepted));
            // All possibly waiting catalog, candidate, object and journal writes have completed.
            // The exact anchor remains locked; this final independent wall-clock sample is followed by COMMIT only.
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            return accepted;
        });
    }

    @Override public Optional<Snapshot> accepted(TableState base, List<byte[]> fragments,
        DistributedWriteCommitContext context) {
        ExecutionAnchor expected = currentOwner.apply(context.executionId());
        return transaction(connection -> {
            AuthorizationTransactionGate.lockPublication(connection);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            var intent = JdbcWriteCommitJournal.lockIntent(connection, context.executionId());
            requireIntent(intent, base, context, fragments);
            requireProvenanceOwner(intent, expected);
            requireWriteAuthority(connection, intent);
            requireCatalog(connection, base.key().tenant());
            requireIncarnation(head(connection, base.key(), false), base);
            JdbcSourceNamespace.requireCurrent(connection, deployment, base.datasetUri());
            var accepted = byFingerprint(connection, base, context.commitFingerprint());
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
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            return accepted;
        });
    }

    @Override public Optional<org.pangrova.spi.connector.metadata.SourceCleanupBatch> reserveCleanup(
        SourcePublicationReservation reservation) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        return preparationTransaction(reservation, expected, connection -> {
            String state = requireReservation(connection, reservation, true);
            if (!(state.equals("ACCEPTED") || state.equals("ABORTED"))) return Optional.empty();
            return JdbcSourceObjectCleanup.reserve(connection, reservation, state.equals("ACCEPTED"));
        });
    }

    @Override public void completeCleanup(SourcePublicationReservation reservation,
        org.pangrova.spi.connector.metadata.SourceCleanupBatch batch) {
        ExecutionAnchor expected = currentOwner.apply(reservation.executionId());
        preparationTransaction(reservation, expected, connection -> {
            String state = requireReservation(connection, reservation, true);
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
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            var intent = JdbcWriteCommitJournal.lockIntent(connection, reservation.executionId());
            requireReservationIntent(reservation, intent);
            requireProvenanceOwner(intent, expected);
            requireWriteAuthority(connection, intent);
            requireCatalog(connection, reservation.base().key().tenant());
            // A missing/replaced SQL head is not itself evidence of an unpublished candidate.
            head(connection, reservation.base().key(), true);
            // A barrier can precede the first RPC. Candidate insertion and journal ABORTED
            // share this transaction, so no later reserve/page/accept may cross it.
            if (intent.publication() != null || intent.state() == WriteCommitEntry.State.COMMITTED) {
                throw new IllegalStateException("Published source intent cannot be aborted");
            }
            if (reservation.base().version() > 0) {
                var acceptedBase = exactSnapshot(connection, reservation.base()).reference();
                if (!sameReference(acceptedBase, reservation.baseSnapshot().orElseThrow())) {
                    throw new IllegalArgumentException("Abort accepted base differs from historical membership");
                }
            }
            insertReservation(connection, reservation, intent);
            String state = requireReservation(connection, reservation, true);
            if (state.equals("ACCEPTED")) throw new IllegalStateException("Accepted source cannot be aborted");
            try (var statement = connection.prepareStatement(
                "UPDATE managed_source_publications SET state='ABORTED' "
                + "WHERE fingerprint=? AND state IN ('PREPARING','PREPARED','ABORTED')")) {
                statement.setString(1, reservation.fingerprint());
                if (statement.executeUpdate() != 1) {
                    throw new IllegalStateException("Source abort barrier was lost");
                }
            }
            JdbcWriteCommitJournal.abortIntent(connection, intent);
            JdbcExecutionAnchorStore.requireCurrent(connection, expected);
            return new org.pangrova.spi.connector.metadata.DistributedWriteAbortReceipt(
                reservation.executionId(), reservation.fingerprint());
        });
    }

    private void requireCatalog(Connection connection, String tenant) throws SQLException {
        text(tenant);
        try (var statement = connection.prepareStatement("SELECT incarnation_id,revision,state,"
            + "(allowed_tenants IS NULL OR allowed_tenants='[]'::jsonb OR allowed_tenants ?? ?) AS allowed "
            + "FROM catalog_definitions WHERE name=? FOR SHARE")) {
            statement.setString(1, tenant);
            statement.setString(2, deployment.catalog());
            try (var result = statement.executeQuery()) {
                if (!result.next() || !deployment.incarnation().equals(result.getString("incarnation_id"))
                    || deployment.revision() != result.getLong("revision")
                    || !"ACTIVE".equals(result.getString("state")) || !result.getBoolean("allowed")) {
                    throw new SecurityException("Managed source catalog deployment or tenant is no longer current");
                }
            }
        }
    }

    private static void requireWriteAuthority(Connection connection, WriteCommitEntry intent) throws SQLException {
        var scope = DeferredAuthorizationScopeCodec.decode(intent.authorizationReceipt().authorizationScope());
        if (!DeferredAuthorizationScopeCodec.decodeWriteReceipt(intent.authorizationReceipt().authorizationScope())
            .equals(intent.authorizationReceipt())) {
            throw new SecurityException("Noncanonical source write receipt");
        }
        JdbcTenantStore.requireDeferredTenant(connection, intent.principal(), intent.tenantId(), scope);
        org.pangrova.metadata.service.TableControlDeferredPolicy.requireCurrent(connection, intent.principal(), scope);
        scope.definitionSnapshot().ifPresent(definition -> {
            if (!definition.declaration().table().catalog().equals(intent.catalogName())
                || !(definition.declaration().table().schema() + "." + definition.declaration().table().table())
                .equals(intent.tableName())) {
                throw new SecurityException("Source write definition receipt targets another table");
            }
        });
        if (scope.definitionSnapshot().isPresent()) {
            GeneratedModelCurrentAuthority.requireCurrent(connection, intent.principal(),
                scope.definitionSnapshot().orElseThrow().models());
        }
    }

    private TableState head(Connection connection, TableKey key, boolean update) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_heads WHERE " + HEAD_KEY
            + (update ? " FOR UPDATE" : " FOR SHARE"))) {
            bindKey(statement, key);
            try (var result = statement.executeQuery()) {
                return result.next() ? mapHead(result) : null;
            }
        }
    }

    private static TableState mapHead(ResultSet result) throws SQLException {
        return new TableState(new TableKey(result.getString("tenant_id"), new SchemaTableName(
            result.getString("schema_name"), result.getString("table_name"))), result.getString("table_id"),
            result.getString("dataset_uri"), result.getLong("generation"), result.getLong("revision"),
            result.getLong("visible_version"), result.getLong("row_count"), result.getBytes("schema_sha256"),
            result.getBytes("payload"));
    }

    private Snapshot exactSnapshot(Connection connection, TableState selector) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_publications WHERE "
            + "catalog_name=? AND catalog_incarnation=? AND tenant_id=? AND table_id=? "
            + "AND published_version=? AND state='ACCEPTED'")) {
            bindCatalog(statement, selector.key().tenant());
            statement.setString(4, selector.tableId());
            statement.setLong(5, selector.version());
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("Exact accepted source membership is absent");
                return mapSnapshot(result);
            }
        }
    }

    private Optional<Snapshot> byFingerprint(Connection connection, TableState base, String fingerprint)
        throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_publications WHERE "
            + "fingerprint=? AND catalog_name=? AND catalog_incarnation=? AND tenant_id=? AND table_id=? "
            + "AND state='ACCEPTED'")) {
            statement.setString(1, fingerprint);
            statement.setString(2, deployment.catalog());
            statement.setString(3, deployment.incarnation());
            statement.setString(4, base.key().tenant());
            statement.setString(5, base.tableId());
            try (var result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapSnapshot(result)) : Optional.empty();
            }
        }
    }

    private Snapshot mapSnapshot(ResultSet result) throws SQLException {
        var table = new TableState(new TableKey(result.getString("tenant_id"), new SchemaTableName(
            result.getString("schema_name"), result.getString("table_name"))), result.getString("table_id"),
            result.getString("dataset_uri"), result.getLong("generation"), result.getLong("base_revision") + 1,
            result.getLong("published_version"), result.getLong("row_count"), result.getBytes("schema_sha256"),
            result.getBytes("payload"));
        var reference = new AcceptedSourceSnapshot(deployment, table.key().tenant(),
            table.tableId(), table.generation(), table.datasetUri(), table.version(),
            result.getString("manifest_relative_path"), result.getBytes("manifest_sha256"), table.schemaSha256(),
            result.getLong("manifest_size"), result.getString("naming_scheme"), result.getString("fingerprint"),
            result.getLong("catalog_revision"));
        return new Snapshot(table, reference);
    }

    private void insertReservation(Connection connection, SourcePublicationReservation reservation,
        WriteCommitEntry intent)
        throws SQLException {
        try (var statement = connection.prepareStatement("INSERT INTO managed_source_publications "
            + "(fingerprint,execution_id,catalog_name,catalog_incarnation,catalog_revision,tenant_id,schema_name,"
            + "table_name,table_id,dataset_uri,generation,base_revision,base_version,state,manifest_relative_path,"
            + "published_version,schema_sha256,intent_owner_id,intent_owner_incarnation,"
            + "intent_owner_token,inherited_membership) "
            + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'PREPARING',?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING")) {
            var base = reservation.base();
            statement.setString(1, reservation.fingerprint());
            statement.setString(2, reservation.executionId());
            statement.setString(3, deployment.catalog());
            statement.setString(4, deployment.incarnation());
            statement.setLong(5, deployment.revision());
            statement.setString(6, base.key().tenant());
            statement.setString(7, base.key().table().schema());
            statement.setString(8, base.key().table().table());
            statement.setString(9, base.tableId());
            statement.setString(10, base.datasetUri());
            statement.setLong(RESERVATION_GENERATION_PARAMETER, base.generation());
            statement.setLong(RESERVATION_BASE_REVISION_PARAMETER, base.revision());
            statement.setLong(RESERVATION_BASE_VERSION_PARAMETER, base.version());
            statement.setString(RESERVATION_MANIFEST_PARAMETER, reservation.manifestRelativePath());
            statement.setLong(RESERVATION_VERSION_PARAMETER, Math.addExact(base.version(), 1));
            statement.setBytes(16, base.schemaSha256());
            statement.setString(RESERVATION_OWNER_PARAMETER, intent.ownerId());
            statement.setString(RESERVATION_OWNER_INCARNATION_PARAMETER, intent.ownerIncarnation());
            statement.setLong(RESERVATION_OWNER_TOKEN_PARAMETER, intent.executionFencingToken());
            statement.setString(RESERVATION_INHERITED_PARAMETER,
                intent.writeMode().equals("INSERT") && base.version() > 0
                ? reservation.baseSnapshot().orElseThrow().membershipId() : null);
            statement.executeUpdate();
        }
    }

    private String requireReservation(Connection connection, SourcePublicationReservation reservation,
        boolean allowAborted)
        throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_publications "
            + "WHERE fingerprint=? FOR UPDATE")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                var base = reservation.base();
                if (!result.next()) throw new IllegalStateException("Managed source candidate reservation differs");
                requireReservationExecution(result, reservation);
                if (!result.getString("tenant_id").equals(base.key().tenant())
                    || !result.getString("schema_name").equals(base.key().table().schema())
                    || !result.getString("table_name").equals(base.key().table().table())
                    || !result.getString("table_id").equals(base.tableId())
                    || !result.getString("dataset_uri").equals(base.datasetUri())
                    || result.getLong("generation") != base.generation()
                    || result.getLong("base_revision") != base.revision() || result
                    .getLong("base_version") != base.version()
                    || !result.getString("manifest_relative_path").equals(reservation.manifestRelativePath())
                    || !Arrays.equals(result.getBytes("schema_sha256"), base.schemaSha256())) {
                    throw new IllegalStateException("Managed source candidate reservation differs");
                }
                String state = result.getString("state");
                if (!allowAborted && state.equals("ABORTED")) {
                    throw new IllegalStateException("Source has an abort barrier");
                }
                return state;
            }
        }
    }

    private void requireReservationExecution(ResultSet result, SourcePublicationReservation reservation)
        throws SQLException {
        if (!result.getString("execution_id").equals(reservation.executionId())
            || !result.getString("catalog_name").equals(deployment.catalog())
            || !result.getString("catalog_incarnation").equals(deployment.incarnation())
            || result.getLong("catalog_revision") != deployment.revision()) {
            throw new IllegalStateException("Managed source candidate reservation differs");
        }
    }

    private static void acceptCandidate(Connection connection, SourcePublicationReservation reservation,
        PreparedSourcePublication publication, TableState next) throws SQLException {
        try (var statement = connection.prepareStatement("UPDATE managed_source_publications SET state='ACCEPTED',"
            + "manifest_sha256=?,manifest_size=?,naming_scheme=?,row_count=?,payload=? "
            + "WHERE fingerprint=? AND state='PREPARED'")) {
            statement.setBytes(1, publication.candidate().manifestSha256());
            statement.setLong(2, publication.candidate().manifestSize());
            statement.setString(3, publication.candidate().namingScheme());
            statement.setLong(4, next.rows());
            statement.setBytes(5, next.payload());
            statement.setString(6, reservation.fingerprint());
            if (statement.executeUpdate() != 1) throw new IllegalStateException("Source candidate cannot be accepted");
        }
    }

    private void updateHead(Connection connection, TableState next) throws SQLException {
        try (var statement = connection.prepareStatement("UPDATE managed_source_heads SET revision=?,visible_version=?,"
            + "row_count=?,payload=? WHERE " + HEAD_KEY)) {
            statement.setLong(1, next.revision());
            statement.setLong(2, next.version());
            statement.setLong(3, next.rows());
            statement.setBytes(4, next.payload());
            statement.setString(5, deployment.catalog());
            statement.setString(6, deployment.incarnation());
            statement.setString(7, next.key().tenant());
            statement.setString(8, next.key().table().schema());
            statement.setString(9, next.key().table().table());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Source head vanished during acceptance");
            }
        }
    }

    private void requireCandidate(SourcePublicationReservation reservation, PreparedSourcePublication publication,
        TableState next) {
        requireCandidateReference(reservation, publication);
        var base = reservation.base();
        requireIncarnation(next, base);
        if (next.version() != publication.candidate().version() || next.revision() != Math.addExact(base.revision(), 1)
            || next.rows() != publication.rows()) {
            throw new IllegalArgumentException("Prepared source next head differs");
        }
    }

    private void requireCandidateReference(SourcePublicationReservation reservation,
        PreparedSourcePublication publication) {
        var candidate = publication.candidate();
        var base = reservation.base();
        if (candidate.acceptedOriginRevision() != deployment.revision() || !candidate.deployment()
            .equals(deployment) || !candidate.tenant().equals(base.key().tenant())
            || !candidate.tableId().equals(base.tableId()) || candidate.generation() != base.generation()
            || !candidate.datasetUri().equals(base.datasetUri()) || candidate.version() != Math.addExact(base
            .version(), 1)
            || !candidate.manifestRelativePath().equals(reservation.manifestRelativePath())
            || !candidate.membershipId().equals(reservation.fingerprint())
            || !Arrays.equals(candidate.schemaSha256(), base.schemaSha256())) {
            throw new IllegalArgumentException("Prepared source does not match reserved identity");
        }
    }

    private static void requireSameSnapshot(Snapshot accepted, PreparedSourcePublication publication, TableState next) {
        var ref = accepted.reference();
        var candidate = publication.candidate();
        if (!Arrays.equals(accepted.table().payload(), next.payload()) || accepted.table().rows() != next.rows()
            || !sameReference(ref, candidate)
            || ref.version() != candidate.version() || !ref.manifestRelativePath().equals(candidate
            .manifestRelativePath())
            || !Arrays.equals(ref.manifestSha256(), candidate.manifestSha256()) || ref
            .manifestSize() != candidate.manifestSize()
            || !ref.namingScheme().equals(candidate.namingScheme())) {
            throw new IllegalStateException("Source replay differs from durable accepted publication");
        }
    }

    private void requireIntent(WriteCommitEntry intent, TableState base, DistributedWriteCommitContext context,
        List<byte[]> fragments) {
        requireIntentIdentity(intent, base, context.executionId(), context.commitFingerprint());
        if (intent.baseSnapshotId() != context.baseSnapshotId() || !intent.tenantId().equals(context.tenantId())
            || !intent.principal().equals(context.principal()) || !intent.authorizationReceipt().equals(context
            .authorizationReceipt())
            || !Arrays.equals(intent.writeOutputBinding(), context.writeOutputBinding()) || intent.fragments()
            .size() != fragments.size()) {
            throw new SecurityException("Source reservation differs from immutable write intent");
        }
        var retained = intent.fragments();
        for (int index = 0; index < fragments.size(); index++) {
            if (!Arrays.equals(retained.get(index), fragments.get(index))) {
                throw new SecurityException("Source winners differ");
            }
        }
    }

    private void requireReservationIntent(SourcePublicationReservation reservation, WriteCommitEntry intent) {
        requireIntentIdentity(intent, reservation.base(), reservation.executionId(), reservation.fingerprint());
    }

    private void requireIntentIdentity(WriteCommitEntry intent, TableState base, String executionId,
        String fingerprint) {
        if (!intent.queryId().equals(executionId) || !intent.commitFingerprint().equals(fingerprint)
            || !intent.catalogName().equals(deployment.catalog()) || !intent.tenantId().equals(base.key().tenant())
            || !intent.tableName().equals(base.key().table().toString()) || intent.baseSnapshotId() != base
            .version()
            || intent.ownerId() == null || intent.executionFencingToken() <= 0) {
            throw new SecurityException("Source publication is not the exact durably owned intent");
        }
    }

    private static void requireProvenanceOwner(WriteCommitEntry intent, ExecutionAnchor current) {
        if (current.fencingToken() < intent.executionFencingToken()
            || (current.fencingToken() == intent.executionFencingToken()
            && (!current.ownerId().equals(intent.ownerId())
            || !current.ownerIncarnation().equals(intent.ownerIncarnation())))) {
            throw new SecurityException("Current source owner does not fence the durable winner provenance");
        }
    }

    private static void requireBase(TableState current, TableState base) {
        requireIncarnation(current, base);
        if (current.revision() != base.revision() || current.version() != base.version()
            || !Arrays.equals(current.payload(), base.payload())) {
            throw new IllegalStateException("Source head CAS conflict");
        }
    }

    private static void requireIncarnation(TableState current, TableState expected) {
        if (current == null || !current.key().equals(expected.key()) || !current.tableId().equals(expected.tableId())
            || !current.datasetUri().equals(expected.datasetUri()) || current.generation() != expected.generation()
            || !Arrays.equals(current.schemaSha256(), expected.schemaSha256())) {
            throw new org.pangrova.spi.connector.metadata
                .SourceTableUnavailableException("Source table was dropped, replaced or changed generation");
        }
    }

    private static boolean sameReference(AcceptedSourceSnapshot left, AcceptedSourceSnapshot right) {
        return left.deployment().equals(right.deployment()) && left.tenant().equals(right.tenant())
            && left.tableId().equals(right.tableId()) && left.generation() == right.generation()
            && left.datasetUri().equals(right.datasetUri()) && left.version() == right.version()
            && left.manifestRelativePath().equals(right.manifestRelativePath())
            && left.manifestSize() == right.manifestSize() && left.namingScheme().equals(right.namingScheme())
            && left.acceptedOriginRevision() == right.acceptedOriginRevision()
            && left.membershipId().equals(right.membershipId())
            && Arrays.equals(left.manifestSha256(), right.manifestSha256())
            && Arrays.equals(left.schemaSha256(), right.schemaSha256());
    }

    private static NativeWriteFinalization finalization(Snapshot snapshot) {
        return new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, snapshot.reference().version(),
            snapshot.reference().manifestSha256());
    }

    private void bindCatalog(PreparedStatement statement, String tenant) throws SQLException {
        statement.setString(1, deployment.catalog());
        statement.setString(2, deployment.incarnation());
        statement.setString(3, tenant);
    }
    private void bindNamespace(PreparedStatement statement, String tenant, String schema) throws SQLException {
        bindCatalog(statement, tenant);
        statement.setString(4, schema);
    }
    private void bindKey(PreparedStatement statement, TableKey key) throws SQLException {
        bindNamespace(statement, key.tenant(), key.table().schema());
        statement.setString(5, key.table().table());
    }
    private void bindHead(PreparedStatement statement, TableState table) throws SQLException {
        bindKey(statement, table.key());
        statement.setString(6, table.tableId());
        statement.setString(7, table.datasetUri());
        statement.setLong(8, table.generation());
        statement.setLong(9, table.revision());
        statement.setLong(10, table.version());
        statement.setLong(HEAD_ROWS_PARAMETER, table.rows());
        statement.setBytes(HEAD_SCHEMA_PARAMETER, table.schemaSha256());
        statement.setBytes(HEAD_PAYLOAD_PARAMETER, table.payload());
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
