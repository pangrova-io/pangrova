package org.pangrova.coordinator.query.execution.phase;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.parser.api.ast.Statement;

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
        org.pangrova.planner.analysis.MetadataTableQuery resolve(java.util.List<String> parts,
                org.pangrova.connector.api.enums.MetadataTableKind kind,
                org.pangrova.query.api.session.QuerySession session);
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
        Statement parsed = context.parsedSql().statement();
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
        var completion = statementDispatcher.tryDispatch(
                context.stateMachine(),
                parsed,
                context.session().queryId(),
                context.session());
        if (completion != null) {
            return new PhaseResult.Terminal(completion);
        }
        var state = context.stateMachine().getState();
        if (state == QueryState.FINISHED
                || state == QueryState.FAILED) {
            return PhaseResult.Terminal.completed(null);
        }
        return new PhaseResult.Continue(context);
    }

    @Override
    public String name() {
        return "statement-dispatch";
    }

}
