package org.pangrova.persistence.generation.postgresql.exchange;

import org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentDirectRoleReader;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;

import org.pangrova.generation.codec.execution.GenerationExchangeEvidenceCodec;

import org.pangrova.generation.codec.execution.GenerationStageGraphCodec;

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
                SELECT tenant_id, job_id, exchange_id, producer_stage_id, consumer_stage_id, graph_sha256
                FROM artifact_stage_exchanges WHERE tenant_id = ? AND job_id = ?
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
            SELECT e.exchange_id, e.producer_stage_id, e.consumer_stage_id, e.graph_sha256,
                   j.source_snapshot, r.registration_bytes, r.registration_sha256,
                   f.finalization_bytes, f.finalization_sha256
            FROM locked_edges e
            JOIN artifact_build_jobs j USING (tenant_id, job_id)
            LEFT JOIN locked_registrations r ON r.exchange_id = e.exchange_id
            LEFT JOIN locked_finalizations f ON f.exchange_id = e.exchange_id
            ORDER BY e.exchange_id
            """;
    private final DataSource dataSource;
    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationExecutionTransactionFence executionFence;
    private final CurrentDirectRoleReader currentRoles;

    /** Uses the same PostgreSQL authority and owner transaction as durable stage claims. */
    public PostgresGenerationStageExchangeStore(
            DataSource dataSource, GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence, CurrentDirectRoleReader currentRoles) {
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
        this.currentRoles = java.util.Objects.requireNonNull(currentRoles, "currentRoles");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Persists the original real allocation without replacing an earlier registration. */
    @Override
    public void register(GenerationJobKey job, GenerationExecutionOwner owner,
            GenerationExchangeRegistration registration) {
        transaction(connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, job.tenantId(), job.jobId(), owner);
            validateRegistration(connection, context, registration, requireGraphEdges(connection, context));
            var existing = findRegistration(connection, job, registration.exchangeId(), true);
            if (existing.isPresent()) {
                if (!existing.orElseThrow().equals(registration)) {
                    throw new IllegalStateException("Immutable external exchange allocation changed");
                }
                return null;
            }
            if (!registration.registeredOwner().equals(owner)) {
                throw new IllegalArgumentException("New exchange registration must preserve its actual issued owner");
            }
            byte[] bytes = GenerationExchangeEvidenceCodec.encode(registration);
            insertEvidence(connection, job, registration.exchangeId(), bytes,
                    org.pangrova.common.util.DigestUtils.sha256Hex(bytes), false);
            return null;
        });
    }

    /** The caller supplies the actual RPC response only after strict protocol validation, never page statistics. */
    @Override
    public void finalizeExchange(GenerationJobKey job, GenerationExecutionOwner owner,
            GenerationExchangeFinalization finalization) {
        transaction(connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, job.tenantId(), job.jobId(), owner);
            var registration = findRegistration(connection, job, finalization.exchangeId(), true).orElseThrow(() ->
                    new IllegalStateException("Cannot finalize an unregistered durable exchange"));
            validateRegistration(connection, context, registration, requireGraphEdges(connection, context));
            GenerationExchangeEvidenceCodec.requireRegistration(finalization, registration);
            if (!owner.equals(finalization.finalizingOwner())) {
                throw new IllegalArgumentException("External finalize response belongs to a different current owner");
            }
            requireProducerWinners(connection, job, registration);
            var existing = findFinalization(connection, job, finalization.exchangeId(), true);
            if (existing.isPresent()) {
                requireSameFinalizedData(existing.orElseThrow(), finalization);
                return null;
            }
            byte[] bytes = GenerationExchangeEvidenceCodec.encode(finalization);
            insertEvidence(connection, job, finalization.exchangeId(), bytes,
                    org.pangrova.common.util.DigestUtils.sha256Hex(bytes), true);
            return null;
        });
    }

    /** Restores exact allocations; short-lived access grants are renewed at the real external service. */
    @Override
    public List<ExchangeEvidence> load(GenerationJobKey job, GenerationExecutionOwner owner) {
        return transaction(connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, job.tenantId(), job.jobId(), owner);
            return loadValidatedEvidence(connection, context);
        });
    }

    /** Same-connection immutable evidence for pure partition preparation; never access capabilities. */
    public static List<ExchangeEvidence> loadValidatedEvidence(Connection connection,
            PostgresCurrentGenerationAuthority context) throws SQLException {
        return readLockedEvidence(connection, context, false, true);
    }

    /** Stores the exact submitted graph's immutable consumer edges on the creating transaction. */
    public static void insertGraphEdges(Connection connection,
            org.pangrova.generation.api.job.GenerationDispatchSpec spec)
            throws SQLException {
        String graphSha256 = GenerationStageGraphCodec.sha256(spec.stageGraph());
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
        readLockedEvidence(connection, context, true, false);
    }

    /** Locks each barrier and returns the same validated registration and finalization bytes. */
    public static List<ExchangeEvidence> lockAndLoadValidatedEvidence(Connection connection,
            PostgresCurrentGenerationAuthority context) throws SQLException {
        return readLockedEvidence(connection, context, true, true);
    }

    private static List<ExchangeEvidence> readLockedEvidence(Connection connection,
            PostgresCurrentGenerationAuthority context, boolean verifyWinners, boolean retainEvidence)
            throws SQLException {
        var spec = context.dispatchSpec();
        var job = new GenerationJobKey(spec.tenantId(), spec.jobId());
        var expected = graphEdges(context);
        String graphSha256 = GenerationStageGraphCodec.sha256(spec.stageGraph());
        var evidence = new ArrayList<ExchangeEvidence>();
        try (var statement = connection.prepareStatement(LOCKED_EXCHANGE_EVIDENCE)) {
            // Each evidence column can carry 16 MiB. Keep the driver's raw fetch window bounded.
            statement.setFetchSize(4);
            statement.setString(1, job.tenantId());
            statement.setString(2, job.jobId());
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
                    if (verifyWinners && value.finalization().isPresent()) {
                        requireProducerWinners(connection, job, value.registration());
                    }
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
        var allocation = GenerationExchangeEvidenceCodec.decodeRegistration(
                registered, row.getString("registration_sha256"));
        var stage = context.dispatchSpec().stageGraph().requireStage(edge.upstreamStageId());
        requireRegistrationIdentity(context, allocation, stage, edge, graphSha256);
        requireSourceSnapshot(allocation, row.getString("source_snapshot"));
        var finalization = Optional.<GenerationExchangeFinalization>empty();
        if (finalized != null) {
            var value = GenerationExchangeEvidenceCodec.decodeFinalization(
                    finalized, row.getString("finalization_sha256"));
            GenerationExchangeEvidenceCodec.requireRegistration(value, allocation);
            finalization = Optional.of(value);
        }
        return new ExchangeEvidence(allocation, finalization);
    }

    private static void validateRegistration(Connection connection, PostgresCurrentGenerationAuthority context,
            GenerationExchangeRegistration registration, String graphSha256) throws SQLException {
        var spec = context.dispatchSpec();
        var graph = spec.stageGraph();
        var edge = graph.exchanges().stream().filter(value -> value.exchangeId().equals(registration.exchangeId()))
                .findFirst().orElseThrow(() ->
                        new IllegalArgumentException("Exchange is not part of the persisted graph"));
        var stage = graph.requireStage(edge.upstreamStageId());
        requireRegistrationIdentity(context, registration, stage, edge, graphSha256);
        try (var statement = connection.prepareStatement("SELECT source_snapshot FROM artifact_build_jobs "
                + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, spec.tenantId());
            statement.setString(2, spec.jobId());
            try (var row = statement.executeQuery()) {
                requireSourceSnapshot(registration, row.next() ? row.getString(1) : null);
            }
        }
    }

    private static void requireSourceSnapshot(GenerationExchangeRegistration registration, String snapshot) {
        if (!registration.sourceSnapshot().equals(snapshot)) {
            throw new IllegalArgumentException("External allocation changes the pinned source snapshot");
        }
    }

    private static void requireRegistrationIdentity(PostgresCurrentGenerationAuthority context,
            GenerationExchangeRegistration registration,
            org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan stage,
            org.pangrova.generation.api.graph.GenerationStageGraph.Exchange edge, String graphSha256) {
        if (!registration.executionId().equals(context.executionAnchor().executionId())
                || !registration.graphSha256().equals(graphSha256)
                || !registration.stagePlanSha256().equals(stage.planSha256())
                || registration.producerStageId() != stage.stageId()
                || registration.producerCount() != stage.parallelism()
                || registration.partitionCount() != edge.partitionCount()) {
            throw new IllegalArgumentException("External allocation differs from the persisted stage contract");
        }
    }

    /** Verifies the whole allocation index, including absent and unexpected rows, before using it as a gate. */
    private static String requireGraphEdges(Connection connection, PostgresCurrentGenerationAuthority context)
            throws SQLException {
        var spec = context.dispatchSpec();
        var expected = graphEdges(context);
        String graphSha256 = GenerationStageGraphCodec.sha256(spec.stageGraph());
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
        return graphSha256;
    }

    private static Map<String, Exchange> graphEdges(PostgresCurrentGenerationAuthority context) {
        return context.dispatchSpec().stageGraph().exchanges().stream()
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

    private static void requireProducerWinners(Connection connection, GenerationJobKey job,
            GenerationExchangeRegistration registration) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT partition_id,state,report_bytes,report_sha256,"
                + "execution_owner_token FROM artifact_build_pages WHERE tenant_id=? AND job_id=? AND stage_id=? "
                + "ORDER BY partition_id")) {
            statement.setString(1, job.tenantId());
            statement.setString(2, job.jobId());
            statement.setInt(3, registration.producerStageId());
            int partition = 0;
            try (var row = statement.executeQuery()) {
                while (row.next()) {
                    requireProducerWinner(row, partition++);
                }
            }
            if (partition != registration.producerCount()) {
                throw new IllegalStateException("External finalize producer cohort is incomplete");
            }
        }
    }

    private static void requireProducerWinner(java.sql.ResultSet row, int partition) throws SQLException {
        byte[] report = row.getBytes("report_bytes");
        if (row.getInt("partition_id") != partition || !"SUCCEEDED".equals(row.getString("state"))
                || report == null || report.length == 0 || row.getLong("execution_owner_token") <= 0
                || !org.pangrova.common.util.DigestUtils.matchesSha256(report, row.getString("report_sha256"))) {
            throw new IllegalStateException("External finalize lacks exact, immutable producer winners");
        }
    }

    private static void requireSameFinalizedData(GenerationExchangeFinalization existing,
            GenerationExchangeFinalization requested) {
        if (!existing.registrationSha256().equals(requested.registrationSha256())
                || !java.util.Arrays.equals(existing.responseBytes(), requested.responseBytes())) {
            throw new IllegalStateException("Immutable external finalized data changed");
        }
    }

    private static Optional<GenerationExchangeRegistration> findRegistration(Connection connection,
            GenerationJobKey job, String exchange, boolean lock) throws SQLException {
        var value = findEvidence(connection, job, exchange, false, lock);
        return value.map(bytes -> GenerationExchangeEvidenceCodec.decodeRegistration(bytes.bytes(), bytes.sha256()));
    }

    private static Optional<GenerationExchangeFinalization> findFinalization(Connection connection,
            GenerationJobKey job, String exchange, boolean lock) throws SQLException {
        var value = findEvidence(connection, job, exchange, true, lock);
        return value.map(bytes -> GenerationExchangeEvidenceCodec.decodeFinalization(bytes.bytes(), bytes.sha256()));
    }

    private static Optional<EvidenceBytes> findEvidence(Connection connection, GenerationJobKey job,
            String exchange, boolean finalized, boolean lock) throws SQLException {
        String name = finalized ? "finalization" : "registration";
        try (var statement = connection.prepareStatement("SELECT " + name + "_bytes," + name
                + "_sha256 FROM artifact_exchange_" + name + "s WHERE tenant_id=? AND job_id=? AND exchange_id=?"
                + (lock ? " FOR SHARE" : ""))) {
            bindKey(statement, job, exchange);
            try (var row = statement.executeQuery()) {
                return row.next()
                        ? Optional.of(new EvidenceBytes(row.getBytes(1), row.getString(2))) : Optional.empty();
            }
        }
    }

    private static void insertEvidence(Connection connection, GenerationJobKey job, String exchange,
            byte[] bytes, String sha256, boolean finalized) throws SQLException {
        String name = finalized ? "finalization" : "registration";
        try (var statement = connection.prepareStatement("INSERT INTO artifact_exchange_" + name + "s "
                + "(tenant_id,job_id,exchange_id," + name + "_bytes," + name + "_sha256) VALUES(?,?,?,?,?)")) {
            bindKey(statement, job, exchange);
            statement.setBytes(4, bytes);
            statement.setString(5, sha256);
            statement.executeUpdate();
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

    private record EvidenceBytes(byte[] bytes, String sha256) {}

    private interface Transaction<T> { T run(Connection connection) throws SQLException; }
}
