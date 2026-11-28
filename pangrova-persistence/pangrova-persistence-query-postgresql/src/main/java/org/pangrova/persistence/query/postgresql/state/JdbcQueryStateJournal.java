package org.pangrova.persistence.query.postgresql.state;

import java.util.function.Predicate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.common.lifecycle.ProcessIncarnationStore.Identity;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionRetirement;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.query.api.session.JournalSessionPayload;

/** PostgreSQL journal with one query-fair, per-query ordered commit owner and retryable actual SQL drain. */
@ThreadSafe
public final class JdbcQueryStateJournal implements QueryStateJournal, RecoveredTaskDrainJournal {
    private static final long DRAIN_MS = 5000;
    private static final int MAX_EXPIRED_ACCEPTANCES_PER_SWEEP = 1000;
    private final DataSource dataSource;
    private final Identity processIdentity;
    private final JournalPendingWrites pending;
    private final JournalFlushOwner flushOwner;

    /** Starts the sole writer; the external DataSource remains owned by the caller. */
    public JdbcQueryStateJournal(DataSource dataSource, StateJournalConfig config) {
        this(dataSource, config, null);
    }

    /** Starts a writer that can bind Coordinator-hosted work to an exclusive process identity. */
    public JdbcQueryStateJournal(DataSource dataSource, StateJournalConfig config, Identity processIdentity) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.processIdentity = processIdentity;
        Objects.requireNonNull(config, "config");
        pending = new JournalPendingWrites(config.maxResidentEvents());
        flushOwner = new JournalFlushOwner(dataSource, config, pending);
    }

    @Override public boolean commitsLineageFacts() { return true; }

    @Override public void appendQueryEvent(QueryStateEvent event) {
        pending.admit(new JournalPendingWrites.Write(pending, event), false);
    }
    @Override public boolean reserveCoordinatorStatement(ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor");
        if (processIdentity != null && !processIdentity.incarnationId().equals(anchor.ownerIncarnation())) {
            return false;
        }
        String domain = processIdentity == null ? null : processIdentity.ownershipDomainId();
        try {
            return pending.enterSql().perform(dataSource,
                    connection -> JournalTaskDrainSql.reserveCoordinatorStatement(connection, anchor, domain));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to fence Coordinator statement execution", failure);
        }
    }

    @Override public void appendOwnedTerminalSync(QueryStateEvent event, ExecutionAnchor anchor) {
        QueryStateJournal.requireOwnedTerminal(event, anchor);
        var owner = pending.admit(new JournalPendingWrites.Write(pending, event, anchor), true);
        await(owner.attempt());
    }

    @Override public void appendTaskEvent(TaskStateEvent event) {
        pending.admit(new JournalPendingWrites.Write(pending, event), false);
    }
    @Override public void appendQueryEventSync(QueryStateEvent event) {
        var owner = pending.admit(new JournalPendingWrites.Write(pending, event), true);
        await(owner.attempt());
    }
    @Override public void appendFencedAdmissionSync(QueryStateEvent event, ExecutionAnchor anchor) {
        var write = new JournalPendingWrites.Write(pending, event, requireGrant(event, anchor));
        await(pending.admit(write, true).attempt());
    }

    @Override public QueryAcceptanceAdmission acceptFencedQuerySync(QueryStateEvent event, ExecutionAnchor anchor) {
        return acceptQuerySync(event, requireGrant(event, anchor));
    }

    private static ExecutionAnchor requireGrant(QueryStateEvent event, ExecutionAnchor anchor) {
        Objects.requireNonNull(anchor, "anchor");
        if (!event.queryId().equals(anchor.executionId()) || !"QUEUED".equals(event.state())) {
            throw new IllegalArgumentException("Execution grant must match the initial query event");
        }
        return anchor;
    }

    @Override public QueryAcceptanceAdmission acceptQuerySync(QueryStateEvent event) {
        return acceptQuerySync(event, null);
    }

    private QueryAcceptanceAdmission acceptQuerySync(QueryStateEvent event, ExecutionAnchor anchor) {
        if (event.acceptance() == null) {
            throw new IllegalArgumentException("Durable acceptance requires an acceptance fact");
        }
        var owner = pending.admit(new JournalPendingWrites.Write(pending, event, anchor), true);
        await(owner.attempt());
        var admission = owner.acceptanceResult();
        if (admission == null) {
            throw JournalPendingWrites.failure("Durable acceptance committed without an admission result", null);
        }
        return admission;
    }
    @Override public void appendTaskEventSync(TaskStateEvent event) {
        var owner = pending.admit(new JournalPendingWrites.Write(pending, event), true);
        await(owner.attempt());
    }

    @Override public void appendTaskIssuancesSync(List<TaskStateEvent> events) {
        var cohort = QueryStateJournal.requireTaskIssuanceCohort(events);
        try {
            pending.enterIssuance(cohort.get(0).queryId()).perform(dataSource, connection -> {
                JournalTaskIssuanceSql.commit(connection, cohort);
                return null;
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to commit exact task issuance cohort", failure);
        }
    }

    private void await(CompletableFuture<Void> attempt) {
        try {
            flushOwner.awaitCommit(attempt);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw JournalPendingWrites.failure("Interrupted awaiting journal commit; event remains owned", interrupted);
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof JournalWriteException rejected) {
                throw rejected;
            }
            throw JournalPendingWrites.failure("Journal commit was not established", failure.getCause());
        }
    }

    @Override public Set<String> observedRecoveryExecutions(Set<String> claims) {
        var requested = Set.copyOf(claims);
        if (requested.isEmpty()) return Set.of();
        try {
            return pending.enterSql().perform(dataSource,
                    connection -> JournalTaskDrainSql.observed(connection, requested));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to classify claimed executions", failure);
        }
    }

    @Override public List<IssuedAttempt> pendingTaskDrains(ExecutionAnchor claim, long afterRowId) {
        if (afterRowId < 0) throw new IllegalArgumentException("Recovery cursor must not be negative");
        try {
            return pending.enterSql().perform(dataSource,
                    connection -> JournalTaskDrainSql.pending(connection, claim, afterRowId));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to read predecessor task issuances", failure);
        }
    }

    @Override public boolean retainTaskDrains(ExecutionAnchor claim, List<DrainReceipt> receipts) {
        var batch = List.copyOf(receipts);
        if (batch.isEmpty() || batch.size() > PAGE_SIZE
                || batch.stream().map(DrainReceipt::rowId).distinct().count() != batch.size()) {
            throw new IllegalArgumentException("Invalid recovered task drain batch");
        }
        try {
            return pending.enterSql().perform(dataSource,
                    connection -> JournalTaskDrainSql.retain(connection, claim, batch));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to retain recovered task drain receipts", failure);
        }
    }

    @Override public DrainOutcome confirmRecoveredTaskDrains(ExecutionAnchor claim) {
        try {
            return pending.enterSql().perform(dataSource, connection -> JournalTaskDrainSql.confirm(connection, claim));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to confirm predecessor exit", failure);
        }
    }

    @Override public Optional<QueryStateEvent> latestQueryEvent(String queryId) {
        Objects.requireNonNull(queryId, "queryId");
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT " + JournalEventSql.QUERY_COLUMNS
                        + " FROM query_state_journal WHERE query_id = ? ORDER BY id DESC LIMIT 1")) {
                    statement.setString(1, queryId);
                    try (ResultSet rows = statement.executeQuery()) {
                        return rows.next() ? Optional.of(JournalEventSql.readQuery(rows)) : Optional.empty();
                    }
                }
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to read latest query event", failure);
        }
    }

    @Override public Optional<ReceiptRecovery> recoverReceipt(String queryId, String principal, String ticket) {
        try {
            return pending.enterSql().perform(dataSource,
                    connection -> readReceiptRecovery(connection, queryId, principal, ticket));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to reconcile the query receipt", failure);
        }
    }

    private static Optional<ReceiptRecovery> readReceiptRecovery(
            Connection connection, String queryId, String principal, String ticket) throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT a.principal, a.idempotency_key_digest, a.request_fingerprint,
                       a.acceptance_receipt_json, a.retention_ms, t.state AS terminal_state, t.terminal_outcome,
                       (x.execution_id IS NULL) AS retired
                FROM query_state_journal a
                LEFT JOIN execution_anchor x ON x.execution_id = a.query_id
                LEFT JOIN query_state_journal t ON t.query_id = a.query_id AND t.terminal_outcome IS NOT NULL
                WHERE a.query_id = ? AND a.principal = ? AND a.state = 'QUEUED'
                      AND a.idempotency_key_digest IS NOT NULL
                      AND (t.state IS NULL
                           OR a.retention_ms > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint
                                                - GREATEST(a.timestamp_ms, t.timestamp_ms)
                           OR x.execution_id IS NOT NULL)
                """)) {
            statement.setString(1, queryId);
            statement.setString(2, principal);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                var owner = JournalEventSql.readAcceptance(rows).receipt().ownerEndpoint();
                if (owner == null || !ticket.equals(owner.routeTicket())) return Optional.empty();
                if (!rows.getBoolean("retired")) return Optional.of(new ReceiptRecovery(null, null));
                String state = rows.getString("terminal_state");
                if (state == null) return Optional.empty();
                var outcome = JournalEventSql.readTerminalOutcome(rows.getString("terminal_outcome"));
                return Optional.of(new ReceiptRecovery(state, outcome));
            }
        }
    }

    @Override public Optional<String> latestQueryStateForPrincipal(String queryId, String principal) {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(principal, "principal");
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                try (var statement = connection.prepareStatement("""
                        SELECT latest.state, (
                            SELECT admission.session_json FROM query_state_journal admission
                            WHERE admission.query_id = latest.query_id AND admission.state = 'QUEUED'
                            ORDER BY admission.id LIMIT 1
                        ) AS admission_payload
                        FROM query_state_journal latest WHERE latest.query_id = ?
                        ORDER BY latest.id DESC LIMIT 1
                        """)) {
                    statement.setString(1, queryId);
                    try (var rows = statement.executeQuery()) {
                        if (!rows.next()) return Optional.empty();
                        var admitted = JournalSessionPayload.admittedPrincipal(queryId,
                                rows.getString("admission_payload"));
                        return admitted.filter(principal::equals).isPresent()
                                ? Optional.of(rows.getString("state")) : Optional.empty();
                    }
                }
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to read principal-scoped query state", failure);
        }
    }

    @Override public Optional<QueryAcceptance> findAcceptance(String principal, String idempotencyKeyDigest) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(idempotencyKeyDigest, "idempotencyKeyDigest");
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                try (PreparedStatement statement = connection.prepareStatement("SELECT " + JournalEventSql.QUERY_COLUMNS
                        + """
                         FROM query_state_journal accepted
                         WHERE principal = ? AND idempotency_key_digest = ? AND state = 'QUEUED'
                           AND NOT EXISTS (
                               SELECT 1 FROM (
                                   SELECT MAX(terminal.timestamp_ms) AS finished_at
                                   FROM query_state_journal terminal
                                   WHERE terminal.query_id = accepted.query_id
                                     AND terminal.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                               ) timestamps
                               WHERE timestamps.finished_at IS NOT NULL
                                 AND accepted.retention_ms <= ?
                                     - GREATEST(accepted.timestamp_ms, timestamps.finished_at)
                                 AND NOT EXISTS (
                                     SELECT 1 FROM execution_anchor a WHERE a.execution_id = accepted.query_id
                                 )
                           )
                         ORDER BY id LIMIT 1
                        """)) {
                    statement.setString(1, principal);
                    statement.setString(2, idempotencyKeyDigest);
                    statement.setLong(3, System.currentTimeMillis());
                    try (ResultSet rows = statement.executeQuery()) {
                        return rows.next()
                                ? Optional.of(JournalEventSql.readQuery(rows).acceptance())
                                : Optional.empty();
                    }
                }
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to reconcile durable query acceptance", failure);
        }
    }

    @Override public void purgeExpiredAcceptances() {
        try {
            pending.enterSql().perform(dataSource, connection -> {
                connection.setAutoCommit(false);
                try (PreparedStatement statement = connection.prepareStatement("""
                        WITH expired AS (
                          SELECT a.query_id FROM query_state_journal a
                          WHERE a.idempotency_key_digest IS NOT NULL
                            AND NOT EXISTS (
                              SELECT 1 FROM execution_anchor e WHERE e.execution_id = a.query_id
                            )
                            AND EXISTS (
                              SELECT 1 FROM query_state_journal t
                              WHERE t.query_id = a.query_id
                                AND t.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                            )
                            AND a.retention_ms <= ? - GREATEST(a.timestamp_ms, (
                              SELECT MAX(t.timestamp_ms) FROM query_state_journal t
                              WHERE t.query_id = a.query_id
                                AND t.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                            ))
                          ORDER BY a.timestamp_ms LIMIT ?
                        ),
                        removed_tasks AS (
                          DELETE FROM task_state_journal t USING expired e
                          WHERE t.query_id = e.query_id
                        )
                        DELETE FROM query_state_journal q USING expired e
                        WHERE q.query_id = e.query_id
                        """)) {
                    statement.setLong(1, System.currentTimeMillis());
                    statement.setInt(2, MAX_EXPIRED_ACCEPTANCES_PER_SWEEP);
                    statement.executeUpdate();
                }
                connection.commit();
                return null;
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to sweep expired query acceptances", failure);
        }
    }

    @Override public void purgeQuery(String queryId) {
        retireQuery(queryId, null);
    }

    @Override public Optional<Predicate<ExecutionAnchor>> durableRetirement() {
        return Optional.of(this::retireOwnedQuery);
    }

    private boolean retireOwnedQuery(ExecutionAnchor anchor) {
        return retireQuery(anchor.executionId(), anchor);
    }

    private boolean retireQuery(String queryId, ExecutionAnchor anchor) {
        Objects.requireNonNull(queryId, "queryId");
        JournalSqlOperation operation = pending.enterPurge(queryId);
        try {
            pending.awaitQueryWrites(queryId, DRAIN_MS);
        } catch (InterruptedException interrupted) {
            operation.closeAfterFailure(interrupted);
            Thread.currentThread().interrupt();
            throw JournalPendingWrites.failure("Interrupted draining query journal before purge", interrupted);
        } catch (RuntimeException | Error failure) {
            operation.closeAfterFailure(failure);
            throw failure;
        }
        try {
            return operation.perform(dataSource, connection -> retireCommitted(connection, queryId, anchor));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to atomically purge query journal: " + queryId, failure);
        }
    }

    private boolean retireCommitted(Connection connection, String queryId, ExecutionAnchor anchor)
            throws SQLException {
        connection.setAutoCommit(false);
        try {
            boolean retired;
            if (anchor == null) {
                purgeLocalFacts(connection, queryId, System.currentTimeMillis());
                retired = true;
            } else {
                retired = JdbcExecutionRetirement.retireQuery(connection, anchor);
            }
            connection.commit();
            return retired;
        } catch (SQLException | RuntimeException | Error failure) {
            try {
                connection.rollback();
            } catch (SQLException rollback) {
                if (rollback != failure) failure.addSuppressed(rollback);
            }
            throw failure;
        }
    }

    private static final String RETIRE_QUERY_PREFIX = """
            WITH target(query_id) AS (VALUES (CAST(? AS text))),
            tasks AS (
                DELETE FROM task_state_journal j USING target t WHERE j.query_id = t.query_id
            )
            """;
    private static final String RETIRE_QUERY_FACTS = """
            DELETE FROM query_state_journal q USING target target_query
            WHERE q.query_id = target_query.query_id
              AND NOT EXISTS (
                SELECT 1 FROM query_state_journal a
                WHERE a.query_id = q.query_id AND a.idempotency_key_digest IS NOT NULL
                  AND (
                    NOT EXISTS (
                      SELECT 1 FROM query_state_journal t
                      WHERE t.query_id = a.query_id
                        AND t.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                    )
                    OR a.retention_ms > ? - GREATEST(a.timestamp_ms, (
                      SELECT MAX(t.timestamp_ms) FROM query_state_journal t
                      WHERE t.query_id = a.query_id
                        AND t.state IN ('FINISHED', 'FAILED', 'CANCELLED')
                    ))
                  )
              )
            """;
    private static final String RETIRE_LOCAL_QUERY = RETIRE_QUERY_PREFIX + RETIRE_QUERY_FACTS;

    private static void purgeLocalFacts(Connection connection, String queryId, long now) throws SQLException {
        try (var statement = connection.prepareStatement(RETIRE_LOCAL_QUERY)) {
            statement.setString(1, queryId);
            statement.setLong(2, now);
            statement.executeUpdate();
        }
    }

    @Override public void close() { flushOwner.close(); }
    @Override public boolean isDrained() { return flushOwner.isDrained(); }
}
