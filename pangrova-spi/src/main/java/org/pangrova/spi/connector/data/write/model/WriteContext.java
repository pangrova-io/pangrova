package org.pangrova.spi.connector.data.write.model;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.session.SessionContext;

import java.util.Map;
import java.util.Optional;

/**
 * Write context with transaction mode and optional commit properties.
 *
 * <p>Commit properties are key-value pairs embedded atomically in the table commit
 * (e.g., Iceberg snapshot summary properties). Used by Dynamic Table refresh to
 * embed source checkpoints for idempotent recovery.</p>
 */
@Experimental
public record WriteContext(
        TransactionMode transactionMode,
        Optional<SessionContext> sessionContext,
        Map<String, String> commitProperties
) {
    public enum TransactionMode { COORDINATOR_MANAGED, SELF_MANAGED }

    public WriteContext {
        commitProperties = commitProperties != null ? Map.copyOf(commitProperties) : Map.of();
    }

    public static WriteContext selfManaged() {
        return new WriteContext(TransactionMode.SELF_MANAGED, Optional.empty(), Map.of());
    }

    public static WriteContext selfManagedWithProperties(Map<String, String> commitProperties) {
        return new WriteContext(TransactionMode.SELF_MANAGED, Optional.empty(), commitProperties);
    }

    public static WriteContext coordinatorManaged(SessionContext session) {
        return new WriteContext(TransactionMode.COORDINATOR_MANAGED, Optional.of(session), Map.of());
    }
}
