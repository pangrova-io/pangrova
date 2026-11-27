package org.pangrova.query.lifecycle;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.connector.api.metadata.ConnectorMetadataScope;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.query.api.lifecycle.QueryExecutionCleanup;
import org.pangrova.query.api.lifecycle.QueryOperation;
import org.pangrova.query.api.session.QuerySession;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/** Execution-owner operations borrowed by the query lifecycle owner. */
@Experimental
public interface QueryExecutionPort extends QueryExecutionCleanup {

    /** Enters one query operation and reserves its execution lifetime. */
    QueryOperation enterQueryOperation(String queryId);

    /** Enters one metadata operation without opening a result or data lifetime. */
    QueryOperation enterQueryMetadataOperation(String queryId);

    /** Dispatches an admitted query and returns its closed control-plane outcome. */
    QueryDispatchOutcome dispatch(QueryStateMachine state, String sql, QuerySession session);

    /** Starts an admitted query and observes its closed outcome without retaining the submit thread. */
    CompletionStage<QueryDispatchOutcome> dispatchAsync(QueryStateMachine state, String sql, QuerySession session);

    /** Retires exact output column names after response construction. */
    List<String> removeColumnNames(String queryId);

    /** Acquires durable execution ownership and samples the authenticated tenant's publication revision. */
    Ownership acquireQueryOwnership(String queryId, String tenantId);

    /** Atomically acquires fresh SQL execution ownership and reserves one authorized tenant slot. */
    default Ownership acquireLimitedQueryOwnership(String queryId, String tenantId, int maxConcurrentQueries) {
        throw new UnsupportedOperationException("Atomic durable query admission is unavailable");
    }

    /** Exact grant and publication revision for the submitted query, or a process-local grant. */
    record Ownership(boolean acquired, OptionalLong publicationRevision, ExecutionAnchor anchor) {
        public Ownership(boolean acquired, OptionalLong publicationRevision) {
            this(acquired, publicationRevision, null);
        }
    }

    /** Releases the execution grant after its local or recovered dependencies have drained. */
    void releaseQueryOwnership(String queryId);

    /** Releases the immutable original grant after this SQL execution has physically drained. */
    default void releaseIssuedQueryOwnership(ExecutionAnchor anchor) {
        throw new UnsupportedOperationException("Exact query ownership release is unavailable");
    }

    /** Binds the exact connector authority for the query lifetime. */
    ConnectorMetadataScope bindConnectorAuthority(QueryStateMachine state, String queryId);

    /** Opens the exact connector query issuance for a recovered query. */
    ConnectorQueryAuthority openQueryIssuance(String queryId);

    /** Marks a recovered issuance unresolved; it must not be treated as complete. */
    void markRecoveredIssuanceUnresolved(String queryId);

    /** Retires the exact query issuance after its terminal publication. */
    void retireQueryIssuance(String queryId);

    /** Retries pending task cancellations under the execution owner. */
    void retryPendingCancels();

    /** Registers a listener for local operation exit and exact task drain progress. */
    void addQueryDrainProgressListener(Consumer<String> listener);
}
