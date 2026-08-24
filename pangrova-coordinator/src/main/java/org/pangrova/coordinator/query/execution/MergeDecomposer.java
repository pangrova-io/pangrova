package org.pangrova.coordinator.query.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.TableNameExtractor;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.coordinator.query.task.NativeWriteCommitTracker;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement.MergeClause;
import org.pangrova.planner.SqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decomposes complex MERGE statements into sequential native operations.
 * Handles Complete MERGE (UPDATE+DELETE+INSERT) and MERGE (UPDATE+INSERT)
 * by splitting into multiple phases with write-ahead logging for consistency.
 *
 * <p>Thread safety: relies on thread-safe collaborators (MergeWriteAheadLog,
 * NativeWriteCommitTracker). The volatile writeCommitTracker reference is
 * safe for read-only access from concurrent threads.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
final class MergeDecomposer {

    private static final Logger LOG = LoggerFactory.getLogger(MergeDecomposer.class);

    private final MergeWriteAheadLog mergeWriteAheadLog;
    private final CatalogRegistry catalogRegistry;
    private final DmlExecutionCallback dmlCallback;
    private volatile NativeWriteCommitTracker writeCommitTracker;

    /**
     * Callback for DML SQL execution, avoiding circular dependency
     * between MergeDecomposer and QueryPipelineExecutor.
     */
    @FunctionalInterface
    interface DmlExecutionCallback {
        DispatchResult executeDmlSql(QueryStateMachine sm, String sql,
                                     CoordinatorSessionContext session,
                                     String writeModeOverride);
    }

    MergeDecomposer(MergeWriteAheadLog mergeWriteAheadLog,
                    CatalogRegistry catalogRegistry,
                    DmlExecutionCallback dmlCallback) {
        this.mergeWriteAheadLog = mergeWriteAheadLog;
        this.catalogRegistry = catalogRegistry;
        this.dmlCallback = dmlCallback;
    }

    void setWriteCommitTracker(NativeWriteCommitTracker tracker) {
        this.writeCommitTracker = tracker;
    }

    // ── Complete MERGE (UPDATE + DELETE + INSERT) ──

    /**
     * Decomposes a Complete MERGE into three sequential operations:
     * 1. INSERT: unmatched source rows
     * 2. DELETE: matched rows meeting the delete condition
     * 3. UPDATE: remaining matched rows
     *
     * <p>Consistency guarantee: if any phase fails, uncommitted subsequent
     * phases have their write contexts cleared to prevent partial commits.</p>
     */
    DispatchResult executeCompleteMerge(QueryStateMachine sm,
                                         MergeStatement merge,
                                         CoordinatorSessionContext session,
                                         SqlCompiler compiler) {
        var printer = new AstPrinter();
        List<String> phaseQueryIds = new ArrayList<>();
        String targetTable = TableNameExtractor.extract(merge);
        mergeWriteAheadLog.begin(session.queryId(),
                targetTable != null ? targetTable : "unknown");
        try {
            executeInsertPhase(merge, session, printer, phaseQueryIds);
            executeDeletePhase(merge, session, printer, phaseQueryIds);
            DispatchResult result = executeUpdatePhase(
                    merge, session, printer, phaseQueryIds);
            mergeWriteAheadLog.complete(session.queryId());
            return result;
        } catch (Exception e) {
            mergeWriteAheadLog.recordFailure(session.queryId(), null, e);
            cleanupMergePhases(phaseQueryIds, session.queryId(), e);
            throw e;
        }
    }

    private void executeInsertPhase(MergeStatement merge,
                                    CoordinatorSessionContext session,
                                    AstPrinter printer,
                                    List<String> phaseQueryIds) {
        var insertClauses = merge.clauses().stream()
                .filter(c -> c instanceof MergeClause.WhenNotMatchedInsert)
                .toList();
        if (insertClauses.isEmpty()) {
            return;
        }
        var insertMerge = new MergeStatement(
                merge.targetTable(), merge.targetAlias(), merge.sourceTable(),
                merge.onCondition(), insertClauses);
        String insertSql = printer.print(insertMerge);
        LOG.info("event=complete_merge_phase1_insert sql={}", insertSql);
        QueryStateMachine phase1Sm = new QueryStateMachine();
        phase1Sm.tryTransition(QueryStateMachine.State.PLANNING);
        String phase1QueryId = UUID.randomUUID().toString();
        phaseQueryIds.add(phase1QueryId);
        mergeWriteAheadLog.recordPhaseStarted(
                session.queryId(), phase1QueryId, "INSERT", insertSql);
        registerMergePhaseWriteContext(phase1QueryId, merge);
        dmlCallback.executeDmlSql(
                phase1Sm, insertSql, session.withQueryId(phase1QueryId), "INSERT");
        mergeWriteAheadLog.recordPhaseCommitted(session.queryId(), phase1QueryId);
    }

    private void executeDeletePhase(MergeStatement merge,
                                    CoordinatorSessionContext session,
                                    AstPrinter printer,
                                    List<String> phaseQueryIds) {
        var deleteClause = merge.clauses().stream()
                .filter(c -> c instanceof MergeClause.WhenMatchedDelete)
                .map(c -> (MergeClause.WhenMatchedDelete) c)
                .findFirst()
                .orElseThrow();
        Expression deleteOnCondition = merge.onCondition();
        if (deleteClause.condition().isPresent()) {
            deleteOnCondition = new BinaryExpression(
                    merge.onCondition(),
                    BinaryExpression.Operator.AND,
                    deleteClause.condition().get());
        }
        var unconditionalDelete = new MergeClause.WhenMatchedDelete(
                Optional.empty());
        var deleteMerge = new MergeStatement(
                merge.targetTable(), merge.targetAlias(), merge.sourceTable(),
                deleteOnCondition, List.of(unconditionalDelete));
        String deleteSql = printer.print(deleteMerge);
        LOG.info("event=complete_merge_phase2_delete sql={}", deleteSql);
        QueryStateMachine phase2Sm = new QueryStateMachine();
        phase2Sm.tryTransition(QueryStateMachine.State.PLANNING);
        String phase2QueryId = UUID.randomUUID().toString();
        phaseQueryIds.add(phase2QueryId);
        mergeWriteAheadLog.recordPhaseStarted(
                session.queryId(), phase2QueryId, "DELETE", deleteSql);
        registerMergePhaseWriteContext(phase2QueryId, merge);
        dmlCallback.executeDmlSql(
                phase2Sm, deleteSql, session.withQueryId(phase2QueryId), "OVERWRITE");
        mergeWriteAheadLog.recordPhaseCommitted(session.queryId(), phase2QueryId);
    }

    private DispatchResult executeUpdatePhase(MergeStatement merge,
                                              CoordinatorSessionContext session,
                                              AstPrinter printer,
                                              List<String> phaseQueryIds) {
        var updateClauses = merge.clauses().stream()
                .filter(c -> c instanceof MergeClause.WhenMatchedUpdate)
                .toList();
        var updateMerge = new MergeStatement(
                merge.targetTable(), merge.targetAlias(), merge.sourceTable(),
                merge.onCondition(), updateClauses);
        String updateSql = printer.print(updateMerge);
        LOG.info("event=complete_merge_phase3_update sql={}", updateSql);
        QueryStateMachine phase3Sm = new QueryStateMachine();
        phase3Sm.tryTransition(QueryStateMachine.State.PLANNING);
        String phase3QueryId = UUID.randomUUID().toString();
        phaseQueryIds.add(phase3QueryId);
        mergeWriteAheadLog.recordPhaseStarted(
                session.queryId(), phase3QueryId, "UPDATE", updateSql);
        registerMergePhaseWriteContext(phase3QueryId, merge);
        return dmlCallback.executeDmlSql(
                phase3Sm, updateSql, session.withQueryId(phase3QueryId), "OVERWRITE");
    }

    // ── MERGE (UPDATE + INSERT, no DELETE) ──

    /**
     * Decomposes MERGE (UPDATE + INSERT, no DELETE) into two sequential
     * operations. Phase 1: UPDATE matched rows. Phase 2: INSERT unmatched.
     */
    DispatchResult executeMergeUpdateInsert(QueryStateMachine sm,
                                            MergeStatement merge,
                                            CoordinatorSessionContext session,
                                            SqlCompiler compiler) {
        var printer = new AstPrinter();
        List<String> phaseQueryIds = new ArrayList<>();
        String targetTable = TableNameExtractor.extract(merge);
        mergeWriteAheadLog.begin(session.queryId(),
                targetTable != null ? targetTable : "unknown");
        try {
            executeMergeUpdatePhase(merge, session, printer, phaseQueryIds);
            DispatchResult result = executeMergeInsertPhase(
                    sm, merge, session, printer);
            mergeWriteAheadLog.complete(session.queryId());
            return result;
        } catch (Exception e) {
            mergeWriteAheadLog.recordFailure(session.queryId(), null, e);
            cleanupMergePhases(phaseQueryIds, session.queryId(), e);
            throw e;
        }
    }

    private void executeMergeUpdatePhase(MergeStatement merge,
                                         CoordinatorSessionContext session,
                                         AstPrinter printer,
                                         List<String> phaseQueryIds) {
        var updateClauses = merge.clauses().stream()
                .filter(c -> c instanceof MergeClause.WhenMatchedUpdate)
                .toList();
        var updateMerge = new MergeStatement(
                merge.targetTable(), merge.targetAlias(), merge.sourceTable(),
                merge.onCondition(), updateClauses);
        String updateSql = printer.print(updateMerge);
        LOG.info("event=merge_update_insert_phase1_update sql={}", updateSql);
        QueryStateMachine phase1Sm = new QueryStateMachine();
        phase1Sm.tryTransition(QueryStateMachine.State.PLANNING);
        String phase1QueryId = UUID.randomUUID().toString();
        phaseQueryIds.add(phase1QueryId);
        mergeWriteAheadLog.recordPhaseStarted(
                session.queryId(), phase1QueryId, "UPDATE", updateSql);
        registerMergePhaseWriteContext(phase1QueryId, merge);
        dmlCallback.executeDmlSql(
                phase1Sm, updateSql, session.withQueryId(phase1QueryId), "OVERWRITE");
        mergeWriteAheadLog.recordPhaseCommitted(
                session.queryId(), phase1QueryId);
    }

    private DispatchResult executeMergeInsertPhase(QueryStateMachine sm,
                                                   MergeStatement merge,
                                                   CoordinatorSessionContext session,
                                                   AstPrinter printer) {
        var insertClauses = merge.clauses().stream()
                .filter(c -> c instanceof MergeClause.WhenNotMatchedInsert)
                .toList();
        var insertMerge = new MergeStatement(
                merge.targetTable(), merge.targetAlias(), merge.sourceTable(),
                merge.onCondition(), insertClauses);
        String insertSql = printer.print(insertMerge);
        LOG.info("event=merge_update_insert_phase2_insert sql={}", insertSql);
        return dmlCallback.executeDmlSql(sm, insertSql, session, "INSERT");
    }

    // ── Internal helpers ──

    private void registerMergePhaseWriteContext(String queryId,
                                                MergeStatement merge) {
        var tracker = this.writeCommitTracker;
        if (tracker == null || catalogRegistry == null) {
            return;
        }
        var ref = TableNameExtractor.extractQualified(merge);
        if (ref == null) {
            return;
        }
        var connectorSession = TableResolver.buildSession(ref.catalog());
        var catalogHandle = catalogRegistry.getCatalog(ref.catalog());
        var metadata = catalogHandle.getMetadata(connectorSession);
        var tableHandle = TableResolver.requireTableHandle(
                metadata, connectorSession, ref);
        var outputHandle = metadata.beginOverwrite(connectorSession, tableHandle);
        tracker.registerWriteContext(
                queryId, ref.catalog(), outputHandle, connectorSession);
        LOG.info("event=merge_phase_write_context_registered queryId={} table={}",
                queryId, ref);
    }

    private void cleanupMergePhases(List<String> phaseQueryIds,
                                    String parentQueryId, Exception cause) {
        var tracker = this.writeCommitTracker;
        if (tracker == null || phaseQueryIds.isEmpty()) {
            LOG.error("event=complete_merge_failed queryId={} error={}",
                    parentQueryId, cause.getMessage());
            return;
        }
        int cleared = tracker.clearWriteStates(phaseQueryIds);
        LOG.error("event=complete_merge_failed queryId={} phaseQueryIds={}"
                        + " clearedPendingPhases={} error={}"
                        + " WARNING: earlier committed phases cannot be rolled back;"
                        + " table may be in partially-modified state",
                parentQueryId, phaseQueryIds, cleared, cause.getMessage());
    }
}
