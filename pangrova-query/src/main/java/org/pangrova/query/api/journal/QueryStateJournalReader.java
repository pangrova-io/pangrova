package org.pangrova.query.api.journal;

import java.util.Optional;
import org.pangrova.common.api.annotation.Stable;

/** Point lookups for query state and exact persisted receipts. */
@Stable(since = "1.0")
public interface QueryStateJournalReader {
    /** Returns the last committed query fact by journal row/insertion order. */
    Optional<QueryStateEvent> latestQueryEvent(String queryId);

    /**
     * Returns the exact persisted receipt and retirement result. An empty optional means the receipt
     * cannot be established, and a pending snapshot means there is no proven retired outcome yet.
     */
    default Optional<ReceiptRecovery> recoverReceipt(String queryId, String principal, String ticket) {
        return Optional.empty();
    }

    /** An immutable receipt is known, with a terminal state only after physical retirement. */
    record ReceiptRecovery(String terminalState, QueryTerminalOutcome outcome) {
        public ReceiptRecovery {
            if (terminalState == null ? outcome != null : outcome == null || !outcome.matchesState(terminalState)) {
                throw new IllegalArgumentException("Recovery requires a coherent terminal outcome or remains pending");
            }
        }

        public boolean pending() { return terminalState == null; }
    }

    /** Returns the latest state when the original admission belongs to this principal. */
    Optional<String> latestQueryStateForPrincipal(String queryId, String principal);
}
