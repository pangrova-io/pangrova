package org.pangrova.coordinator.query.execution.phase;

import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;

/**
 * Phase 1: DDL/DML statement dispatch.
 * Short-circuits the pipeline if the SQL is a DDL/DML statement.
 * Falls through (Continue) for SELECT queries.
 */
public final class StatementDispatchPhase implements QueryExecutionPhase {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(StatementDispatchPhase.class);

    @FunctionalInterface
    public interface MetadataListingResolver {
        org.pangrova.planner.MetadataTableQuery resolve(java.util.List<String> parts,
                org.pangrova.connector.spi.metadata.MetadataTableKind kind,
                org.pangrova.coordinator.query.session.CoordinatorSessionContext session);
    }

    private final StatementDispatcher statementDispatcher;
    private final MetadataListingResolver listingResolver;

    public StatementDispatchPhase(StatementDispatcher statementDispatcher) {
        this(statementDispatcher, null);
    }

    public StatementDispatchPhase(StatementDispatcher statementDispatcher, MetadataListingResolver listingResolver) {
        this.statementDispatcher = statementDispatcher;
        this.listingResolver = listingResolver;
    }

    @Override
    public PhaseResult execute(QueryExecutionContext context) {
        if (statementDispatcher == null) {
            LOG.debug("event=ddl_dml_skip reason=metadata_disabled");
            return new PhaseResult.Continue(context);
        }
        org.pangrova.parser.ast.Statement parsed = null;
        try {
            parsed = context.compiler().parseToAst(context.sql());
        } catch (RuntimeException parseFailure) {
            // Preserve the existing dispatch fallback for SQL handled directly
            // by Calcite; CompilationPhase will report unsupported syntax.
        }
        var metadataQuery = ShowMetadataQueryExpander.expand(parsed,
                (name, kind) -> {
                    if (listingResolver == null) {
                        throw new IllegalStateException("SHOW metadata requires the query listing resolver");
                    }
                    return listingResolver.resolve(name, kind, context.session());
                });
        if (metadataQuery != null) {
            LOG.debug("event=show_metadata_expanded queryId={}", context.session().queryId());
            return new PhaseResult.Continue(context.withMetadataQuery(metadataQuery));
        }
        DispatchResult result = statementDispatcher.tryDispatch(
                context.stateMachine(),
                context.sql(),
                context.session().queryId(),
                context.session());
        if (result != null) {
            return new PhaseResult.Terminal(result);
        }
        // DDL/DML may complete without a DispatchResult (no resultService).
        // If the state machine already reached a terminal state, stop here.
        var state = context.stateMachine().getState();
        if (state == org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED
                || state == org.pangrova.coordinator.query.QueryStateMachine.State.FAILED) {
            return new PhaseResult.Terminal(null);
        }
        return new PhaseResult.Continue(context);
    }

    @Override
    public String name() {
        return "statement-dispatch";
    }

}
