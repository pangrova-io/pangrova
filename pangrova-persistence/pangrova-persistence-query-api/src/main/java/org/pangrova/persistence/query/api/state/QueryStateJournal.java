package org.pangrova.persistence.query.api.state;

import java.util.function.Predicate;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.journal.QueryAcceptance;
import org.pangrova.query.api.journal.QueryAcceptanceAdmission;
import org.pangrova.query.api.journal.QueryStateEvent;
import org.pangrova.query.api.journal.QueryStateJournalReader;
import org.pangrova.query.api.journal.TaskStateEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.pangrova.common.api.annotation.Stable;

/**
 * One owner for immutable query/task journal facts.
 * Sync methods wait for actual commit; async methods transfer an admitted event to
 * the same bounded owner and reject closed/full admission. NONE is process-local,
 * not a durable issued-attempt or HA authority.
 */
@Stable(since = "1.0")
public interface QueryStateJournal extends QueryStateJournalReader, AutoCloseable {
    /** Admits a query event or throws without accepting it; accepted failures remain owned. */
    void appendQueryEvent(QueryStateEvent event);
    /** Commits the exact frozen query event or reports an unestablished outcome for retry. */
    void appendQueryEventSync(QueryStateEvent event);
    /** Fences the start of Coordinator-owned statement work before its first side effect. */
    default boolean reserveCoordinatorStatement(ExecutionAnchor anchor) {
        throw new UnsupportedOperationException("The journal does not support durable Coordinator statement authority");
    }

    /** Commits the terminal fact under the exact originally admitted execution grant. */
    void appendOwnedTerminalSync(QueryStateEvent event, ExecutionAnchor anchor);

    /** The owned journal retains only outcomes after admission. */
    static void requireOwnedTerminal(QueryStateEvent event, ExecutionAnchor anchor) {
        boolean terminal = switch (event.state()) {
            case "FINISHED", "FAILED", "CANCELLED" -> true;
            default -> false;
        };
        if (!terminal || !event.queryId().equals(anchor.executionId())
                || !anchor.ownerId().equals(event.context().coordinatorId()) || event.acceptance() != null) {
            throw new IllegalArgumentException("Execution grant must match the terminal query event");
        }
    }

    /** Atomically accepts or replays the initial QUEUED event carrying idempotency authority. */
    QueryAcceptanceAdmission acceptQuerySync(QueryStateEvent event);

    /** Commits an initial unkeyed admission under the exact execution grant. */
    void appendFencedAdmissionSync(QueryStateEvent event, ExecutionAnchor anchor);

    /** Commits or replays an initial keyed admission under the exact execution grant. */
    QueryAcceptanceAdmission acceptFencedQuerySync(QueryStateEvent event, ExecutionAnchor anchor);
    /** Reads one principal-scoped acceptance only for conflict or uncertain-outcome reconciliation. */
    Optional<QueryAcceptance> findAcceptance(String principal, String idempotencyKeyDigest);
    /** Bounded sweep of terminal acceptances whose retention has elapsed. */
    void purgeExpiredAcceptances();
    /** Admits a task event or throws without accepting it; accepted failures remain owned. */
    void appendTaskEvent(TaskStateEvent event);
    /** Commits the exact frozen task event or reports an unestablished outcome for retry. */
    void appendTaskEventSync(TaskStateEvent event);
    /** Upper bound on one dispatch cohort of exact issuance facts. */
    int MAX_TASK_ISSUANCE_BATCH = 16;

    /**
     * Commits one same-owner cohort before Worker HTTP admission. A rejected cohort must never be sent.
     */
    void appendTaskIssuancesSync(List<TaskStateEvent> events);

    /** Validates the shared execution fence before any backend accepts a cohort. */
    static List<TaskStateEvent> requireTaskIssuanceCohort(List<TaskStateEvent> events) {
        var cohort = List.copyOf(events);
        if (cohort.isEmpty() || cohort.size() > MAX_TASK_ISSUANCE_BATCH || cohort.get(0).issuance() == null) {
            throw new IllegalArgumentException("Invalid task issuance cohort");
        }
        var first = cohort.get(0);
        var identities = new HashSet<java.util.UUID>();
        for (var event : cohort) {
            if (event.issuance() == null || !first.queryId().equals(event.queryId())
                    || !first.context().coordinatorId().equals(event.context().coordinatorId())
                    || !first.issuance().ownerIncarnation().equals(event.issuance().ownerIncarnation())
                    || first.issuance().fencingToken() != event.issuance().fencingToken()
                    || !identities.add(event.context().eventId())) {
                throw new IllegalArgumentException(
                        "Task issuance cohort changed its execution fence or event identity");
            }
        }
        return cohort;
    }

    /** Excludes concurrent query admission, drains admitted writes and atomically deletes its facts. */
    void purgeQuery(String queryId);
    /** Atomic durable retirement for backends that share execution authority and query facts. */
    default Optional<Predicate<ExecutionAnchor>> durableRetirement() {
        return Optional.empty();
    }
    /** Seals admission and waits for actual owned operations to exit; failure is retryable. */
    @Override void close();
    /**
     * True only after admission is sealed and every actual writer, entered operation and connection has exited.
     * This is resource evidence, not commit success; a sticky semantic failure still makes every close throw.
     */
    boolean isDrained();
}
