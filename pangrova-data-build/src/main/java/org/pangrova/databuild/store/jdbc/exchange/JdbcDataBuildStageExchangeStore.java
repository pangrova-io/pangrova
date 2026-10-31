package org.pangrova.databuild.store.jdbc.exchange;

import org.pangrova.databuild.store.jdbc.execution.CurrentDeferredExecutionContext;

import org.pangrova.databuild.spi.jdbc.DataBuildTransactionAuthority;
import org.pangrova.security.spi.jdbc.CurrentDirectRoleReader;
import org.pangrova.databuild.store.jdbc.execution.JdbcDataBuildExecutionFence;

import org.pangrova.databuild.protocol.DataBuildExchangeEvidenceCodec;

import org.pangrova.databuild.protocol.DataBuildStageGraphCodec;

import org.pangrova.databuild.api.build.DataBuildJobKey;
import org.pangrova.databuild.api.execution.DataBuildExchangeFinalization;
import org.pangrova.databuild.api.execution.DataBuildExchangeRegistration;
import org.pangrova.databuild.api.execution.DataBuildExecutionOwner;
import org.pangrova.databuild.api.DataBuildStorageException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable external exchange evidence, gated by current security and the actual execution anchor. */
@org.pangrova.common.api.annotation.Internal
public final class JdbcDataBuildStageExchangeStore {
    private final DataSource dataSource;
    private final DataBuildTransactionAuthority transactionAuthority;
    private final JdbcDataBuildExecutionFence executionFence;
    private final CurrentDirectRoleReader currentRoles;

    /** Uses the same PostgreSQL authority and owner transaction as durable stage claims. */
    public JdbcDataBuildStageExchangeStore(
            DataSource dataSource, DataBuildTransactionAuthority transactionAuthority,
            JdbcDataBuildExecutionFence executionFence, CurrentDirectRoleReader currentRoles) {
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
        this.currentRoles = java.util.Objects.requireNonNull(currentRoles, "currentRoles");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Persists the original real allocation without replacing an earlier registration. */
    public void register(DataBuildJobKey job, DataBuildExecutionOwner owner,
            DataBuildExchangeRegistration registration) {
        transaction(connection -> {
            var context = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, job.tenantId(), job.jobId(), owner);
            validateRegistration(connection, context, registration);
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
            byte[] bytes = DataBuildExchangeEvidenceCodec.encode(registration);
            insertEvidence(connection, job, registration.exchangeId(), bytes,
                    org.pangrova.common.util.DigestUtils.sha256Hex(bytes), false);
            return null;
        });
    }

    /** The caller supplies the actual RPC response only after strict protocol validation, never page statistics. */
    public void finalizeExchange(DataBuildJobKey job, DataBuildExecutionOwner owner,
            DataBuildExchangeFinalization finalization) {
        transaction(connection -> {
            var context = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, job.tenantId(), job.jobId(), owner);
            var registration = findRegistration(connection, job, finalization.exchangeId(), true).orElseThrow(() ->
                    new IllegalStateException("Cannot finalize an unregistered durable exchange"));
            validateRegistration(connection, context, registration);
            DataBuildExchangeEvidenceCodec.requireRegistration(finalization, registration);
            if (!owner.equals(finalization.finalizingOwner())) {
                throw new IllegalArgumentException("External finalize response belongs to a different current owner");
            }
            requireProducerWinners(connection, job, registration);
            var existing = findFinalization(connection, job, finalization.exchangeId(), true);
            if (existing.isPresent()) {
                requireSameFinalizedData(existing.orElseThrow(), finalization);
                return null;
            }
            byte[] bytes = DataBuildExchangeEvidenceCodec.encode(finalization);
            insertEvidence(connection, job, finalization.exchangeId(), bytes,
                    org.pangrova.common.util.DigestUtils.sha256Hex(bytes), true);
            return null;
        });
    }

    /** Restores exact allocations; short-lived access grants are renewed at the real external service. */
    public List<ExchangeEvidence> load(DataBuildJobKey job, DataBuildExecutionOwner owner) {
        return transaction(connection -> {
            var context = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, job.tenantId(), job.jobId(), owner);
            return loadValidatedEvidence(connection, context);
        });
    }

    /** Same-connection immutable evidence for pure partition preparation; never access capabilities. */
    public static List<ExchangeEvidence> loadValidatedEvidence(Connection connection,
            CurrentDeferredExecutionContext context) throws SQLException {
        requireGraphEdges(connection, context);
        var spec = context.dispatchSpec();
        var job = new DataBuildJobKey(spec.tenantId(), spec.jobId());
        var result = new ArrayList<ExchangeEvidence>();
        for (var edge : spec.stageGraph().exchanges()) {
            var registration = findRegistration(connection, job, edge.exchangeId(), true);
            if (registration.isPresent()) {
                validateRegistration(connection, context, registration.orElseThrow());
                var finalized = findFinalization(connection, job, edge.exchangeId(), true);
                finalized.ifPresent(value -> DataBuildExchangeEvidenceCodec.requireRegistration(
                        value, registration.orElseThrow()));
                result.add(new ExchangeEvidence(registration.orElseThrow(), finalized));
            }
        }
        return List.copyOf(result);
    }

    public record ExchangeEvidence(DataBuildExchangeRegistration registration,
            Optional<DataBuildExchangeFinalization> finalization) {}

    /** Stores the exact submitted graph's immutable consumer edges on the creating transaction. */
    public static void insertGraphEdges(Connection connection,
            org.pangrova.databuild.api.build.DataBuildDispatchSpec spec)
            throws SQLException {
        String graphSha256 = DataBuildStageGraphCodec.sha256(spec.stageGraph());
        try (var statement = connection.prepareStatement("INSERT INTO artifact_stage_exchanges "
                + "(tenant_id,job_id,exchange_id,producer_stage_id,consumer_stage_id,graph_sha256) "
                + "VALUES(?,?,?,?,?,?)")) {
            for (var edge : spec.stageGraph().exchanges()) {
                bindKey(statement, new DataBuildJobKey(spec.tenantId(), spec.jobId()), edge.exchangeId());
                statement.setInt(4, edge.upstreamStageId());
                statement.setInt(5, edge.downstreamStageId());
                statement.setString(6, graphSha256);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    /** Called before job/page locks. A successful metadata page alone can never open an exchange consumer. */
    public static void lockValidatedBarriers(Connection connection, CurrentDeferredExecutionContext context)
            throws SQLException {
        requireGraphEdges(connection, context);
        var spec = context.dispatchSpec();
        var job = new DataBuildJobKey(spec.tenantId(), spec.jobId());
        for (var edge : spec.stageGraph().exchanges()) {
            var registration = findRegistration(connection, job, edge.exchangeId(), true);
            var finalization = findFinalization(connection, job, edge.exchangeId(), true);
            if (finalization.isPresent()) {
                var allocated = registration.orElseThrow(() -> new IllegalStateException("Barrier has no allocation"));
                validateRegistration(connection, context, allocated);
                DataBuildExchangeEvidenceCodec.requireRegistration(finalization.orElseThrow(), allocated);
                requireProducerWinners(connection, job, allocated);
            }
        }
    }

    private static void validateRegistration(Connection connection, CurrentDeferredExecutionContext context,
            DataBuildExchangeRegistration registration) throws SQLException {
        requireGraphEdges(connection, context);
        var spec = context.dispatchSpec();
        var graph = spec.stageGraph();
        var edge = graph.exchanges().stream().filter(value -> value.exchangeId().equals(registration.exchangeId()))
                .findFirst().orElseThrow(() ->
                        new IllegalArgumentException("Exchange is not part of the persisted graph"));
        var stage = graph.requireStage(edge.upstreamStageId());
        requireRegistrationIdentity(context, registration, stage, edge);
        try (var statement = connection.prepareStatement("SELECT source_snapshot FROM artifact_build_jobs "
                + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, spec.tenantId());
            statement.setString(2, spec.jobId());
            try (var row = statement.executeQuery()) {
                if (!row.next() || !registration.sourceSnapshot().equals(row.getString(1))) {
                    throw new IllegalArgumentException("External allocation changes the pinned source snapshot");
                }
            }
        }
    }

    private static void requireRegistrationIdentity(CurrentDeferredExecutionContext context,
            DataBuildExchangeRegistration registration,
            org.pangrova.databuild.api.execution.DataBuildStageGraph.StagePlan stage,
            org.pangrova.databuild.api.execution.DataBuildStageGraph.Exchange edge) {
        if (!registration.executionId().equals(context.executionAnchor().executionId())
                || !registration.graphSha256().equals(
                        DataBuildStageGraphCodec.sha256(context.dispatchSpec().stageGraph()))
                || !registration.stagePlanSha256().equals(stage.planSha256())
                || registration.producerStageId() != stage.stageId()
                || registration.producerCount() != stage.parallelism()
                || registration.partitionCount() != edge.partitionCount()) {
            throw new IllegalArgumentException("External allocation differs from the persisted stage contract");
        }
    }

    /** Verifies the whole allocation index, including absent and unexpected rows, before using it as a gate. */
    private static void requireGraphEdges(Connection connection, CurrentDeferredExecutionContext context)
            throws SQLException {
        var spec = context.dispatchSpec();
        var expected = spec.stageGraph().exchanges().stream().collect(java.util.stream.Collectors.toMap(
                org.pangrova.databuild.api.execution.DataBuildStageGraph.Exchange::exchangeId, value -> value));
        String graphSha256 = DataBuildStageGraphCodec.sha256(spec.stageGraph());
        try (var statement = connection.prepareStatement("SELECT exchange_id,producer_stage_id,consumer_stage_id,"
                + "graph_sha256 FROM artifact_stage_exchanges WHERE tenant_id=? AND job_id=? "
                + "ORDER BY exchange_id FOR SHARE")) {
            statement.setString(1, spec.tenantId());
            statement.setString(2, spec.jobId());
            int count = 0;
            try (var row = statement.executeQuery()) {
                while (row.next()) {
                    var edge = expected.get(row.getString("exchange_id"));
                    if (edge == null || edge.upstreamStageId() != row.getInt("producer_stage_id")
                            || edge.downstreamStageId() != row.getInt("consumer_stage_id")
                            || !java.security.MessageDigest.isEqual(
                                    graphSha256.getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                                    row.getString("graph_sha256")
                                            .getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
                        throw new IllegalStateException(
                                "Persisted exchange edges differ from the immutable stage graph");
                    }
                    count++;
                }
            }
            if (count != expected.size()) {
                throw new IllegalStateException("Persisted exchange edges are missing from the immutable stage graph");
            }
        }
    }

    private static void requireProducerWinners(Connection connection, DataBuildJobKey job,
            DataBuildExchangeRegistration registration) throws SQLException {
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

    private static void requireSameFinalizedData(DataBuildExchangeFinalization existing,
            DataBuildExchangeFinalization requested) {
        if (!existing.registrationSha256().equals(requested.registrationSha256())
                || !java.util.Arrays.equals(existing.responseBytes(), requested.responseBytes())) {
            throw new IllegalStateException("Immutable external finalized data changed");
        }
    }

    private static Optional<DataBuildExchangeRegistration> findRegistration(Connection connection,
            DataBuildJobKey job, String exchange, boolean lock) throws SQLException {
        var value = findEvidence(connection, job, exchange, false, lock);
        return value.map(bytes -> DataBuildExchangeEvidenceCodec.decodeRegistration(bytes.bytes(), bytes.sha256()));
    }

    private static Optional<DataBuildExchangeFinalization> findFinalization(Connection connection,
            DataBuildJobKey job, String exchange, boolean lock) throws SQLException {
        var value = findEvidence(connection, job, exchange, true, lock);
        return value.map(bytes -> DataBuildExchangeEvidenceCodec.decodeFinalization(bytes.bytes(), bytes.sha256()));
    }

    private static Optional<EvidenceBytes> findEvidence(Connection connection, DataBuildJobKey job,
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

    private static void insertEvidence(Connection connection, DataBuildJobKey job, String exchange,
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

    private static void bindKey(java.sql.PreparedStatement statement, DataBuildJobKey job, String exchange)
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
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new DataBuildStorageException("Cannot persist current-authorized external shuffle evidence", failure);
        }
    }

    private record EvidenceBytes(byte[] bytes, String sha256) {}

    private interface Transaction<T> { T run(Connection connection) throws SQLException; }
}
