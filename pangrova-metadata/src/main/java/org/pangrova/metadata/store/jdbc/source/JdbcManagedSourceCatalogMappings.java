package org.pangrova.metadata.store.jdbc.source;

import org.pangrova.ai.store.jdbc.GeneratedModelCurrentAuthority;
import org.pangrova.security.tenant.store.JdbcTenantStore;
import org.pangrova.security.authz.DeferredAuthorizationScopeCodec;
import org.pangrova.spi.connector.metadata.AcceptedSourceSnapshot;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog.Deployment;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog.Snapshot;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog.TableKey;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog.TableState;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.connector.metadata.PreparedSourcePublication;
import org.pangrova.spi.connector.metadata.SourcePublicationReservation;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.WriteCommitEntry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Shared SQL mappings and durability guards behind {@link JdbcManagedSourceCatalog}.
 *
 * <p>All state is passed explicitly so the catalog keeps the assembly-owned
 * deployment and data source as its only fields.</p>
 */
final class JdbcManagedSourceCatalogMappings {

    static final String HEAD_KEY = "catalog_name=? AND catalog_incarnation=? AND tenant_id=? "
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

    private JdbcManagedSourceCatalogMappings() {
    }

    static void requireCatalog(Connection connection, Deployment deployment, String tenant)
        throws SQLException {
        requireText(tenant);
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

    static void requireWriteAuthority(Connection connection, WriteCommitEntry intent) throws SQLException {
        var scope = DeferredAuthorizationScopeCodec.decode(intent.authorizationReceipt().authorizationScope());
        if (!DeferredAuthorizationScopeCodec.decodeWriteReceipt(intent.authorizationReceipt().authorizationScope())
            .equals(intent.authorizationReceipt())) {
            throw new SecurityException("Noncanonical source write receipt");
        }
        JdbcTenantStore.requireDeferredTenant(connection, intent.principal(), intent.tenantId(), scope);
        org.pangrova.metadata.core.TableControlDeferredPolicy.requireCurrent(connection, intent.principal(), scope);
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

    static TableState head(Connection connection, Deployment deployment, TableKey key, boolean update)
        throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_heads WHERE " + HEAD_KEY
            + (update ? " FOR UPDATE" : " FOR SHARE"))) {
            bindKey(statement, deployment, key);
            try (var result = statement.executeQuery()) {
                return result.next() ? mapHead(result) : null;
            }
        }
    }

    static TableState mapHead(ResultSet result) throws SQLException {
        return new TableState(new TableKey(result.getString("tenant_id"), new SchemaTableName(
            result.getString("schema_name"), result.getString("table_name"))), result.getString("table_id"),
            result.getString("dataset_uri"), result.getLong("generation"), result.getLong("revision"),
            result.getLong("visible_version"), result.getLong("row_count"), result.getBytes("schema_sha256"),
            result.getBytes("payload"));
    }

    static Snapshot exactSnapshot(Connection connection, Deployment deployment, TableState selector)
        throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_publications WHERE "
            + "catalog_name=? AND catalog_incarnation=? AND tenant_id=? AND table_id=? "
            + "AND published_version=? AND state='ACCEPTED'")) {
            bindCatalog(statement, deployment, selector.key().tenant());
            statement.setString(4, selector.tableId());
            statement.setLong(5, selector.version());
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("Exact accepted source membership is absent");
                return mapSnapshot(result, deployment);
            }
        }
    }

    static Optional<Snapshot> byFingerprint(Connection connection, Deployment deployment, TableState base,
        String fingerprint)
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
                return result.next() ? Optional.of(mapSnapshot(result, deployment)) : Optional.empty();
            }
        }
    }

    static Snapshot mapSnapshot(ResultSet result, Deployment deployment) throws SQLException {
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

    static void insertReservation(Connection connection, Deployment deployment,
        SourcePublicationReservation reservation, WriteCommitEntry intent)
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

    static String requireReservation(Connection connection, Deployment deployment,
        SourcePublicationReservation reservation, boolean allowAborted)
        throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM managed_source_publications "
            + "WHERE fingerprint=? FOR UPDATE")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                var base = reservation.base();
                if (!result.next()) throw new IllegalStateException("Managed source candidate reservation differs");
                requireReservationExecution(result, deployment, reservation);
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

    static void requireReservationExecution(ResultSet result, Deployment deployment,
        SourcePublicationReservation reservation)
        throws SQLException {
        if (!result.getString("execution_id").equals(reservation.executionId())
            || !result.getString("catalog_name").equals(deployment.catalog())
            || !result.getString("catalog_incarnation").equals(deployment.incarnation())
            || result.getLong("catalog_revision") != deployment.revision()) {
            throw new IllegalStateException("Managed source candidate reservation differs");
        }
    }

    static void acceptCandidate(Connection connection, SourcePublicationReservation reservation,
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

    static void updateHead(Connection connection, Deployment deployment, TableState next) throws SQLException {
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

    static void requireCandidate(SourcePublicationReservation reservation, PreparedSourcePublication publication,
        TableState next, Deployment deployment) {
        requireCandidateReference(reservation, publication, deployment);
        var base = reservation.base();
        requireIncarnation(next, base);
        if (next.version() != publication.candidate().version() || next.revision() != Math.addExact(base.revision(), 1)
            || next.rows() != publication.rows()) {
            throw new IllegalArgumentException("Prepared source next head differs");
        }
    }

    static void requireCandidateReference(SourcePublicationReservation reservation,
        PreparedSourcePublication publication, Deployment deployment) {
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

    static void requireSameSnapshot(Snapshot accepted, PreparedSourcePublication publication, TableState next) {
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

    static void requireIntent(WriteCommitEntry intent, TableState base, DistributedWriteCommitContext context,
        List<byte[]> fragments, Deployment deployment) {
        requireIntentIdentity(intent, base, context.executionId(), context.commitFingerprint(), deployment);
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

    static void requireReservationIntent(SourcePublicationReservation reservation, WriteCommitEntry intent,
        Deployment deployment) {
        requireIntentIdentity(intent, reservation.base(), reservation.executionId(), reservation.fingerprint(),
            deployment);
    }

    static void requireIntentIdentity(WriteCommitEntry intent, TableState base, String executionId,
        String fingerprint, Deployment deployment) {
        if (!intent.queryId().equals(executionId) || !intent.commitFingerprint().equals(fingerprint)
            || !intent.catalogName().equals(deployment.catalog()) || !intent.tenantId().equals(base.key().tenant())
            || !intent.tableName().equals(base.key().table().toString()) || intent.baseSnapshotId() != base
            .version()
            || intent.ownerId() == null || intent.executionFencingToken() <= 0) {
            throw new SecurityException("Source publication is not the exact durably owned intent");
        }
    }

    static void requireProvenanceOwner(WriteCommitEntry intent, ExecutionAnchor current) {
        if (current.fencingToken() < intent.executionFencingToken()
            || (current.fencingToken() == intent.executionFencingToken()
            && (!current.ownerId().equals(intent.ownerId())
            || !current.ownerIncarnation().equals(intent.ownerIncarnation())))) {
            throw new SecurityException("Current source owner does not fence the durable winner provenance");
        }
    }

    static void requireBase(TableState current, TableState base) {
        requireIncarnation(current, base);
        if (current.revision() != base.revision() || current.version() != base.version()
            || !Arrays.equals(current.payload(), base.payload())) {
            throw new IllegalStateException("Source head CAS conflict");
        }
    }

    static void requireIncarnation(TableState current, TableState expected) {
        if (current == null || !current.key().equals(expected.key()) || !current.tableId().equals(expected.tableId())
            || !current.datasetUri().equals(expected.datasetUri()) || current.generation() != expected.generation()
            || !Arrays.equals(current.schemaSha256(), expected.schemaSha256())) {
            throw new org.pangrova.spi.connector.metadata
                .SourceTableUnavailableException("Source table was dropped, replaced or changed generation");
        }
    }

    static boolean sameReference(AcceptedSourceSnapshot left, AcceptedSourceSnapshot right) {
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

    static NativeWriteFinalization finalization(Snapshot snapshot) {
        return new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, snapshot.reference().version(),
            snapshot.reference().manifestSha256());
    }

    static void bindCatalog(PreparedStatement statement, Deployment deployment, String tenant) throws SQLException {
        statement.setString(1, deployment.catalog());
        statement.setString(2, deployment.incarnation());
        statement.setString(3, tenant);
    }

    static void bindNamespace(PreparedStatement statement, Deployment deployment, String tenant, String schema)
        throws SQLException {
        bindCatalog(statement, deployment, tenant);
        statement.setString(4, schema);
    }

    static void bindKey(PreparedStatement statement, Deployment deployment, TableKey key) throws SQLException {
        bindNamespace(statement, deployment, key.tenant(), key.table().schema());
        statement.setString(5, key.table().table());
    }

    static void bindHead(PreparedStatement statement, Deployment deployment, TableState table) throws SQLException {
        bindKey(statement, deployment, table.key());
        statement.setString(6, table.tableId());
        statement.setString(7, table.datasetUri());
        statement.setLong(8, table.generation());
        statement.setLong(9, table.revision());
        statement.setLong(10, table.version());
        statement.setLong(HEAD_ROWS_PARAMETER, table.rows());
        statement.setBytes(HEAD_SCHEMA_PARAMETER, table.schemaSha256());
        statement.setBytes(HEAD_PAYLOAD_PARAMETER, table.payload());
    }

    static void requireText(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Source identity is required");
    }
}
