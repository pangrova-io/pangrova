package org.pangrova.persistence.generation.postgresql.exchange;

import org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;

import org.pangrova.generation.codec.execution.GenerationExchangeEvidenceCodec;

import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.exchange.GenerationExchangeRegistration;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.graph.GenerationStageGraph.Exchange;
import org.pangrova.persistence.generation.api.exception.GenerationStorageException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Immutable external exchange evidence, gated by current security and the actual execution anchor. */
@org.pangrova.common.api.annotation.Internal
public final class PostgresGenerationStageExchangeStore implements GenerationStageExchangeStore {
    private static final String LOCKED_EXCHANGE_EVIDENCE = """
            WITH locked_edges AS MATERIALIZED (
                SELECT tenant_id, job_id, exchange_id, producer_stage_id, consumer_stage_id, graph_sha256,
                physical_producer_count
                FROM artifact_stage_exchanges WHERE tenant_id = ? AND job_id = ? %s
                ORDER BY exchange_id FOR SHARE
            ), locked_registrations AS MATERIALIZED (
                SELECT r.exchange_id, r.registration_bytes, r.registration_sha256
                FROM artifact_exchange_registrations r
                JOIN locked_edges e USING (tenant_id, job_id, exchange_id)
                ORDER BY r.exchange_id FOR SHARE OF r
            ), locked_finalizations AS MATERIALIZED (
                SELECT f.exchange_id, f.finalization_bytes, f.finalization_sha256
                FROM artifact_exchange_finalizations f
                JOIN locked_edges e USING (tenant_id, job_id, exchange_id)
                ORDER BY f.exchange_id FOR SHARE OF f
            )
            SELECT e.exchange_id, e.producer_stage_id, e.consumer_stage_id, e.graph_sha256, e.physical_producer_count,
                   j.source_snapshot, r.registration_bytes, r.registration_sha256,
                   f.finalization_bytes, f.finalization_sha256
            FROM locked_edges e
            JOIN artifact_build_jobs j USING (tenant_id, job_id)
            LEFT JOIN locked_registrations r ON r.exchange_id = e.exchange_id
            LEFT JOIN locked_finalizations f ON f.exchange_id = e.exchange_id
            ORDER BY e.exchange_id
            """;
    private static final String LOCKED_GRAPH_EVIDENCE = LOCKED_EXCHANGE_EVIDENCE.formatted("");
    private static final String LOCKED_STAGE_EVIDENCE = LOCKED_EXCHANGE_EVIDENCE.formatted(
            "AND (producer_stage_id = ? OR consumer_stage_id = ?)");
    private static final String LOCKED_REGISTRATION = """
            WITH exact_registration_edge AS MATERIALIZED (
                SELECT tenant_id, job_id, exchange_id, producer_stage_id, consumer_stage_id, graph_sha256,
                physical_producer_count
                FROM artifact_stage_exchanges
                WHERE tenant_id = ? AND job_id = ? AND exchange_id = ? FOR SHARE
            ), registration AS MATERIALIZED (
                SELECT r.exchange_id, r.registration_bytes, r.registration_sha256
                FROM artifact_exchange_registrations r
                JOIN exact_registration_edge e USING (tenant_id, job_id, exchange_id)
                FOR SHARE OF r
            )
            SELECT e.exchange_id, e.producer_stage_id, e.consumer_stage_id, e.graph_sha256, e.physical_producer_count,
                   j.source_snapshot, r.registration_bytes, r.registration_sha256
            FROM exact_registration_edge e
            JOIN artifact_build_jobs j USING (tenant_id, job_id)
            LEFT JOIN registration r ON r.exchange_id = e.exchange_id
            """;
    private final DataSource dataSource;
    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationExecutionTransactionFence executionFence;

    /** Uses the same PostgreSQL authority and owner transaction as durable stage claims. */
    public PostgresGenerationStageExchangeStore(
            DataSource dataSource, GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence) {
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Persists the original real allocation without replacing an earlier registration. */
    @Override
    public GenerationExchangeRegistration register(GenerationJobKey job, GenerationExecutionOwner owner,
            GenerationExchangeRegistration registration) {
        return transaction(connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, job.tenantId(), job.jobId(), owner);
            requireGraphEdges(connection, context);
            if (!registration.registeredOwner().equals(owner)) {
                throw new IllegalArgumentException("Registration must carry its actual issued owner");
            }
            var existing = lockRegistration(connection, context, registration.exchangeId(), registration);
            if (existing.isPresent()) {
                var original = existing.orElseThrow();
                if (!original.equals(withOriginalIssuer(registration, original.registeredOwner()))) {
                    throw new IllegalStateException("Immutable external exchange allocation changed");
                }
                return original;
            }
            byte[] bytes = GenerationExchangeEvidenceCodec.encode(registration);
            try (var statement = connection.prepareStatement("""
                    INSERT INTO artifact_exchange_registrations
                        (tenant_id, job_id, exchange_id, registration_bytes, registration_sha256)
                    VALUES (?, ?, ?, ?, ?)
                    """)) {
                bindKey(statement, job, registration.exchangeId());
                statement.setBytes(4, bytes);
                statement.setString(5, org.pangrova.common.crypto.Sha256Digests.sha256Hex(bytes));
                statement.executeUpdate();
            }
            return registration;
        });
    }

    private static GenerationExchangeRegistration withOriginalIssuer(GenerationExchangeRegistration requested,
            GenerationExecutionOwner originalIssuer) {
        return new GenerationExchangeRegistration(requested.exchangeId(), requested.executionId(),
                requested.shuffleId(), requested.producerStageId(), requested.planEpoch(), requested.graphSha256(),
                requested.stagePlanSha256(), requested.sourceSnapshot(), requested.producerCount(),
                requested.producerSetSha256(),
                requested.partitionCount(), requested.descriptor(), originalIssuer);
    }

    @Override
    public Optional<GenerationExchangeRegistration> findRegistration(GenerationJobKey job,
            GenerationExecutionOwner owner, String exchangeId) {
        return transaction(connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, job.tenantId(), job.jobId(), owner);
            return lockRegistration(connection, context, exchangeId);
        });
    }

    /** A terminal producer needs only its own immutable allocation on the authority connection. */
    public static Optional<GenerationExchangeRegistration> lockRegistration(Connection connection,
            PostgresCurrentGenerationAuthority context, String exchangeId) throws SQLException {
        return lockRegistration(connection, context, exchangeId, null);
    }

    private static Optional<GenerationExchangeRegistration> lockRegistration(Connection connection,
            PostgresCurrentGenerationAuthority context, String exchangeId, GenerationExchangeRegistration requested)
            throws SQLException {
        var spec = context.dispatchSpec();
        var graph = spec.stageGraph();
        var edge = graph.exchanges().stream().filter(value -> value.exchangeId().equals(exchangeId))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Exchange is outside the persisted graph"));
        String graphSha256 = context.graphSha256();
        try (var statement = connection.prepareStatement(LOCKED_REGISTRATION)) {
            bindKey(statement, new GenerationJobKey(spec.tenantId(), spec.jobId()), exchangeId);
            try (var row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("Exact exchange edge is missing from the immutable stage graph");
                }
                requireGraphEdge(row, edge, graphSha256);
                if (requested != null) {
                    var stage = graph.requireStage(edge.upstreamStageId());
                    requireRegistrationIdentity(context, requested, stage, edge, graphSha256,
                            physicalCount(row, stage));
                    requireSourceSnapshot(requested, row.getString("source_snapshot"));
                }
                return Optional.ofNullable(readExchangeEvidence(context, row, edge, graphSha256, null))
                        .map(ExchangeEvidence::registration);
            }
        }
    }

    /** The caller supplies the actual RPC response only after strict protocol validation, never page statistics. */
    @Override
    public void finalizeExchange(GenerationJobKey job, GenerationExecutionOwner owner,
            GenerationExchangeFinalization finalization, byte[] providerResponse) {
        Objects.requireNonNull(providerResponse, "providerResponse");
        transaction(connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, job.tenantId(), job.jobId(), owner);
            var registration = lockRegistration(connection, context, finalization.exchangeId()).orElseThrow(() ->
                    new IllegalStateException("Cannot finalize an unregistered durable exchange"));
            GenerationExchangeEvidenceCodec.requireRegistration(finalization, registration);
            if (!owner.equals(finalization.finalizingOwner())) {
                throw new IllegalArgumentException("External finalize response belongs to a different current owner");
            }
            if (!insertFinalization(connection, job, finalization, providerResponse)) {
                requireSameFinalizedData(connection, job, registration, finalization, providerResponse);
            }
            return null;
        });
    }

    /** Restores exact allocations; short-lived access grants are renewed at the real external service. */
    @Override
    public List<ExchangeEvidence> load(GenerationJobKey job, GenerationExecutionOwner owner) {
        return transaction(connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, job.tenantId(), job.jobId(), owner);
            return readLockedEvidence(connection, context, null, true);
        });
    }

    /** Stores the exact submitted graph's immutable consumer edges on the creating transaction. */
    public static void insertGraphEdges(Connection connection,
            org.pangrova.generation.api.job.GenerationDispatchSpec spec, String graphSha256)
            throws SQLException {
        try (var statement = connection.prepareStatement("INSERT INTO artifact_stage_exchanges "
                + "(tenant_id,job_id,exchange_id,producer_stage_id,consumer_stage_id,graph_sha256) "
                + "VALUES(?,?,?,?,?,?)")) {
            for (var edge : spec.stageGraph().exchanges()) {
                bindKey(statement, new GenerationJobKey(spec.tenantId(), spec.jobId()), edge.exchangeId());
                statement.setInt(4, edge.upstreamStageId());
                statement.setInt(5, edge.downstreamStageId());
                statement.setString(6, graphSha256);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    /** Called before job/page locks. A successful metadata page alone can never open an exchange consumer. */
    public static void lockValidatedBarriers(Connection connection, PostgresCurrentGenerationAuthority context)
            throws SQLException {
        readLockedEvidence(connection, context, null, false);
    }

    /** Locks each barrier and returns the same validated registration and finalization bytes. */
    public static List<ExchangeEvidence> lockAndLoadValidatedEvidence(Connection connection,
            PostgresCurrentGenerationAuthority context) throws SQLException {
        return readLockedEvidence(connection, context, null, true);
    }

    /** Locks and validates only the exchange edges incident to an exact stage claim. */
    public static List<ExchangeEvidence> lockAndLoadStageEvidence(Connection connection,
            PostgresCurrentGenerationAuthority context, int stage) throws SQLException {
        return readLockedEvidence(connection, context, stage, true);
    }

    private static List<ExchangeEvidence> readLockedEvidence(Connection connection,
            PostgresCurrentGenerationAuthority context, Integer stage, boolean retainEvidence)
            throws SQLException {
        var spec = context.dispatchSpec();
        var job = new GenerationJobKey(spec.tenantId(), spec.jobId());
        var expected = graphEdges(context, stage);
        String graphSha256 = context.graphSha256();
        var evidence = new ArrayList<ExchangeEvidence>();
        try (var statement = connection.prepareStatement(
                stage == null ? LOCKED_GRAPH_EVIDENCE : LOCKED_STAGE_EVIDENCE)) {
            // Each evidence column can carry 16 MiB. Keep the driver's raw fetch window bounded.
            statement.setFetchSize(4);
            statement.setString(1, job.tenantId());
            statement.setString(2, job.jobId());
            if (stage != null) {
                statement.setInt(3, stage);
                statement.setInt(4, stage);
            }
            try (var rows = statement.executeQuery()) {
                int count = 0;
                while (rows.next()) {
                    var edge = expected.get(rows.getString("exchange_id"));
                    requireGraphEdge(rows, edge, graphSha256);
                    count++;
                    byte[] finalized = rows.getBytes("finalization_bytes");
                    if (finalized == null && !retainEvidence) continue;
                    var value = readExchangeEvidence(context, rows, edge, graphSha256, finalized);
                    if (value == null) continue;
                    if (retainEvidence) evidence.add(value);
                }
                requireEdgeCount(expected.size(), count);
            }
        }
        return List.copyOf(evidence);
    }

    private static ExchangeEvidence readExchangeEvidence(PostgresCurrentGenerationAuthority context,
            ResultSet row, Exchange edge, String graphSha256, byte[] finalized) throws SQLException {
        byte[] registered = row.getBytes("registration_bytes");
        if (registered == null) {
            if (finalized != null) throw new IllegalStateException("Barrier has no allocation");
            return null;
        }
        String registrationDigest = row.getString("registration_sha256");
        var allocation = GenerationExchangeEvidenceCodec.decodeRegistration(registered, registrationDigest);
        var stage = context.dispatchSpec().stageGraph().requireStage(edge.upstreamStageId());
        requireRegistrationIdentity(context, allocation, stage, edge, graphSha256, physicalCount(row, stage));
        requireSourceSnapshot(allocation, row.getString("source_snapshot"));
        var finalization = Optional.<GenerationExchangeFinalization>empty();
        if (finalized != null) {
            var value = GenerationExchangeEvidenceCodec.decodeFinalization(
                    finalized, row.getString("finalization_sha256"));
            GenerationExchangeEvidenceCodec.requireRegistration(value, allocation, registrationDigest);
            finalization = Optional.of(value);
        }
        return new ExchangeEvidence(allocation, finalization);
    }

    private static void requireSourceSnapshot(GenerationExchangeRegistration registration, String snapshot) {
        if (!registration.sourceSnapshot().equals(snapshot)) {
            throw new IllegalArgumentException("External allocation changes the pinned source snapshot");
        }
    }

    private static int physicalCount(ResultSet row,
            org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan stage) throws SQLException {
        int count = row.getInt("physical_producer_count");
        if (row.wasNull()) return stage.parallelism();
        if (count <= 0 || (count >= stage.parallelism() && stage.splittableHashInputs().isEmpty())
                || count > GenerationExchangeFinalization.maximumAdaptiveTasks(stage.parallelism())) {
            throw new IllegalStateException("Frozen Shuffle producer count differs from its original stage");
        }
        return count;
    }

    private static void requireRegistrationIdentity(PostgresCurrentGenerationAuthority context,
            GenerationExchangeRegistration registration,
            org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan stage,
            org.pangrova.generation.api.graph.GenerationStageGraph.Exchange edge,
            String graphSha256, int physicalProducers) {
        if (!registration.executionId().equals(context.executionAnchor().executionId())
                || !registration.graphSha256().equals(graphSha256)
                || !registration.stagePlanSha256().equals(stage.planSha256())
                || registration.producerStageId() != stage.stageId()
                || registration.producerCount() != physicalProducers
                || registration.partitionCount() != edge.partitionCount()) {
            throw new IllegalArgumentException("External allocation differs from the persisted stage contract");
        }
    }

    /** Verifies the whole allocation index, including absent and unexpected rows, before using it as a gate. */
    private static void requireGraphEdges(Connection connection, PostgresCurrentGenerationAuthority context)
            throws SQLException {
        var spec = context.dispatchSpec();
        var expected = graphEdges(context, null);
        String graphSha256 = context.graphSha256();
        try (var statement = connection.prepareStatement("SELECT exchange_id,producer_stage_id,consumer_stage_id,"
                + "graph_sha256 FROM artifact_stage_exchanges WHERE tenant_id=? AND job_id=? "
                + "ORDER BY exchange_id FOR SHARE")) {
            statement.setString(1, spec.tenantId());
            statement.setString(2, spec.jobId());
            int count = 0;
            try (var row = statement.executeQuery()) {
                while (row.next()) {
                    requireGraphEdge(row, expected.get(row.getString("exchange_id")), graphSha256);
                    count++;
                }
            }
            requireEdgeCount(expected.size(), count);
        }
    }

    private static Map<String, Exchange> graphEdges(PostgresCurrentGenerationAuthority context, Integer stage) {
        return context.dispatchSpec().stageGraph().exchanges().stream()
                .filter(edge -> stage == null || edge.upstreamStageId() == stage || edge.downstreamStageId() == stage)
                .collect(Collectors.toMap(Exchange::exchangeId, value -> value));
    }

    private static void requireGraphEdge(ResultSet row, Exchange edge, String graphSha256) throws SQLException {
        if (edge == null || edge.upstreamStageId() != row.getInt("producer_stage_id")
                || edge.downstreamStageId() != row.getInt("consumer_stage_id")
                || !graphSha256.equals(row.getString("graph_sha256"))) {
            throw new IllegalStateException("Persisted exchange edges differ from the immutable stage graph");
        }
    }

    private static void requireEdgeCount(int expected, int actual) {
        if (actual != expected) {
            throw new IllegalStateException("Persisted exchange edges are missing from the immutable stage graph");
        }
    }

    private static void requireSameFinalizedData(Connection connection, GenerationJobKey job,
            GenerationExchangeRegistration registration, GenerationExchangeFinalization requested,
            byte[] providerResponse) throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT finalization_bytes, finalization_sha256, provider_response
                FROM artifact_exchange_finalizations
                WHERE tenant_id = ? AND job_id = ? AND exchange_id = ? FOR SHARE
                """)) {
            bindKey(statement, job, requested.exchangeId());
            try (var row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("Finalized exchange is missing after receipt conflict");
                }
                var existing = GenerationExchangeEvidenceCodec.decodeFinalization(row.getBytes(1), row.getString(2));
                GenerationExchangeEvidenceCodec.requireRegistration(existing, registration);
                if (!existing.partitions().equals(requested.partitions())
                        || !existing.responseSha256().equals(requested.responseSha256())
                        || !java.util.Arrays.equals(row.getBytes(3), providerResponse)) {
                    throw new IllegalStateException("Immutable external finalized data changed");
                }
            }
        }
    }

    private static boolean insertFinalization(Connection connection, GenerationJobKey job,
            GenerationExchangeFinalization finalization, byte[] providerResponse) throws SQLException {
        byte[] bytes = GenerationExchangeEvidenceCodec.encode(finalization);
        try (var statement = connection.prepareStatement("""
                INSERT INTO artifact_exchange_finalizations
                    (tenant_id, job_id, exchange_id, finalization_bytes, finalization_sha256, provider_response)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (tenant_id, job_id, exchange_id) DO NOTHING
                """)) {
            bindKey(statement, job, finalization.exchangeId());
            statement.setBytes(4, bytes);
            statement.setString(5, org.pangrova.common.crypto.Sha256Digests.sha256Hex(bytes));
            statement.setBytes(6, providerResponse);
            return statement.executeUpdate() == 1;
        }
    }

    private static void bindKey(java.sql.PreparedStatement statement, GenerationJobKey job, String exchange)
            throws SQLException {
        statement.setString(1, job.tenantId());
        statement.setString(2, job.jobId());
        statement.setString(3, exchange);
    }

    private <T> T transaction(Transaction<T> action) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = action.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException | Error failure) {
                try {
                    connection.rollback();
                } catch (SQLException | RuntimeException | Error rollbackFailure) {
                    if (rollbackFailure != failure) {
                        failure.addSuppressed(rollbackFailure);
                    }
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new GenerationStorageException(
                    "Cannot persist current-authorized external shuffle evidence", failure);
        }
    }

    private interface Transaction<T> { T run(Connection connection) throws SQLException; }
}
