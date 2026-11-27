package org.pangrova.persistence.query.postgresql.state;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.lifecycle.ProcessIncarnationStore.Identity;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.persistence.query.api.exception.JournalWriteException;
import org.pangrova.persistence.query.api.state.QueryStateJournal;
import org.pangrova.persistence.query.api.state.RecoveredTaskDrainJournal;
import org.pangrova.persistence.query.api.state.StateJournalConfig;
import org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.query.api.session.JournalSessionPayload;

/** PostgreSQL journal with one query-fair, per-query ordered commit owner and retryable actual SQL drain. */
@ThreadSafe
public final class JdbcQueryStateJournal implements QueryStateJournal, RecoveredTaskDrainJournal {
    private static final long DRAIN_MS = 5000;
    private static final int MAX_EXPIRED_ACCEPTANCES_PER_SWEEP = 1000;
    private static final String ACTIVE = "query_id NOT IN (SELECT query_id FROM query_state_journal "
            + "WHERE state IN ('FINISHED','FAILED','CANCELLED'))";
    private static final String REPLAY_CLAIMED = """
            WITH claimed AS MATERIALIZED (
                SELECT query_id FROM unnest(?::varchar[]) AS ids(query_id)
            ), claimed_events AS MATERIALIZED (
                SELECT q.* FROM claimed JOIN query_state_journal q USING (query_id)
            ), query_facts AS MATERIALIZED (
                SELECT query_id, bool_or(state IN ('FINISHED','FAILED','CANCELLED')) AS terminal
                FROM claimed_events GROUP BY query_id
            )
            SELECT 0 AS event_kind, q.id, q.event_id, q.query_id, q.state, q.timestamp_ms, q.coordinator_id,
                   q.sql_text, q.session_json, q.lineage_payload, q.principal, q.idempotency_key_digest,
                   q.request_fingerprint, q.acceptance_receipt_json, q.retention_ms, q.terminal_outcome,
                   NULL::varchar AS task_id, NULL::varchar AS worker_id, NULL::integer AS stage_id,
                   NULL::bigint AS report_sequence, NULL::text AS worker_host, NULL::integer AS worker_http_port,
                   NULL::text AS owner_incarnation, NULL::bigint AS owner_fencing_token, NULL::bytea AS admission_seal
            FROM query_facts JOIN claimed_events q USING (query_id) WHERE NOT terminal
            UNION ALL
            SELECT 1, t.id, t.event_id, t.query_id, t.state, t.timestamp_ms, t.coordinator_id,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   t.task_id, t.worker_id, t.stage_id, t.report_sequence,
                   t.worker_host, t.worker_http_port, t.owner_incarnation, t.owner_fencing_token, t.admission_seal
            FROM query_facts JOIN task_state_journal t USING (query_id) WHERE NOT terminal
            UNION ALL
            SELECT 2, NULL, NULL, claimed.query_id, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM claimed LEFT JOIN query_facts USING (query_id)
            WHERE CASE WHEN query_facts.query_id IS NOT NULL THEN query_facts.terminal
                  ELSE (SELECT true FROM task_state_journal t WHERE t.query_id = claimed.query_id LIMIT 1) IS TRUE
                  END
            ORDER BY event_kind, id
            """;

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

    @Override public List<QueryStateEvent> replayActiveQueries() {
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                List<QueryStateEvent> events = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement("SELECT " + JournalEventSql.QUERY_COLUMNS
                        + " FROM query_state_journal WHERE " + ACTIVE + " ORDER BY id ASC");
                        ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) events.add(JournalEventSql.readQuery(rows));
                }
                return List.copyOf(events);
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to replay active queries", failure);
        }
    }

    @Override public List<TaskStateEvent> replayActiveTasks() {
        try {
            return pending.enterSql().perform(dataSource, connection -> {
                List<TaskStateEvent> events = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement("SELECT " + JournalEventSql.TASK_COLUMNS
                        + " FROM task_state_journal WHERE query_id IN (SELECT query_id FROM query_state_journal WHERE "
                        + ACTIVE + ") ORDER BY id ASC"); ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) events.add(JournalEventSql.readTask(rows));
                }
                return List.copyOf(events);
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to replay active tasks", failure);
        }
    }

    @Override public RecoveryBatch replayClaimedQueries(Set<String> queryIds) {
        Set<String> claimed = Set.copyOf(queryIds);
        if (claimed.isEmpty()) return new RecoveryBatch(List.of(), List.of(), Set.of());
        try {
            return pending.enterSql().perform(dataSource, connection -> readClaimed(connection, claimed));
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to replay claimed queries", failure);
        }
    }

    private static RecoveryBatch readClaimed(Connection connection, Set<String> claimed) throws SQLException {
        List<QueryStateEvent> queries = new ArrayList<>();
        List<TaskStateEvent> tasks = new ArrayList<>();
        Set<String> observed = new HashSet<>();
        var ids = connection.createArrayOf("varchar", claimed.toArray(String[]::new));
        try (var statement = connection.prepareStatement(REPLAY_CLAIMED)) {
            statement.setArray(1, ids);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    switch (rows.getInt("event_kind")) {
                        case 0 -> queries.add(JournalEventSql.readQuery(rows));
                        case 1 -> tasks.add(JournalEventSql.readTask(rows));
                        case 2 -> { }
                        default -> throw new SQLException("Unknown recovery row kind");
                    }
                    observed.add(rows.getString("query_id"));
                }
            }
        } finally {
            ids.free();
        }
        return new RecoveryBatch(queries, tasks, observed);
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
                        + " FROM query_state_journal WHERE principal = ? AND idempotency_key_digest = ? "
                        + "AND state = 'QUEUED' ORDER BY id LIMIT 1")) {
                    statement.setString(1, principal);
                    statement.setString(2, idempotencyKeyDigest);
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
        purgeQuery(queryId, null);
    }

    @Override public void purgeOwnedQuery(ExecutionAnchor anchor) {
        purgeQuery(anchor.executionId(), anchor);
    }

    private void purgeQuery(String queryId, ExecutionAnchor anchor) {
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
            operation.perform(dataSource, connection -> {
                purgeCommitted(connection, queryId, anchor);
                return null;
            });
        } catch (SQLException failure) {
            throw JournalPendingWrites.failure("Failed to atomically purge query journal: " + queryId, failure);
        }
    }

    private void purgeCommitted(Connection connection, String queryId, ExecutionAnchor anchor) throws SQLException {
        connection.setAutoCommit(false);
        try {
            if (anchor != null && !JournalEventSql.lockActiveGrant(connection, anchor)) {
                connection.rollback();
                return;
            }
            retireQueryFacts(connection, queryId, System.currentTimeMillis(), anchor != null);
            connection.commit();
        } catch (SQLException failure) {
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
    private static final String RETIRE_OWNED_QUERY = RETIRE_QUERY_PREFIX + """
            , writes AS (
                DELETE FROM write_commit_journal w USING target t WHERE w.query_id = t.query_id AND
            """ + JdbcWriteCommitJournal.RETIRABLE_WRITE + """
            )
            """ + RETIRE_QUERY_FACTS;

    private static void retireQueryFacts(Connection connection, String queryId, long now, boolean owned)
            throws SQLException {
        try (var statement = owned ? connection.prepareStatement(RETIRE_OWNED_QUERY)
                : connection.prepareStatement(RETIRE_LOCAL_QUERY)) {
            statement.setString(1, queryId);
            statement.setLong(2, now);
            statement.executeUpdate();
        }
    }

    @Override public void close() { flushOwner.close(); }
    @Override public boolean isDrained() { return flushOwner.isDrained(); }
}
