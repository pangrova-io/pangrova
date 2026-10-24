package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Executes data quality rules by running SQL against actual table data
 * via the internal query pipeline.
 *
 * <p>For each rule, constructs a SQL query that counts total rows and
 * violating rows, then collects a bounded sample of failing rows for
 * diagnostic purposes. Results are reported to the governance audit log.</p>
 *
 * <p>The adapter parses the rule's {@code checkExpression} as one expression
 * before constructing the count or sample query. A row passes if the expression evaluates to true; it fails
 * otherwise, including SQL UNKNOWN (NULL). The executor runs two queries:</p>
 * <ol>
 *   <li>Count query: total rows and failing rows</li>
 *   <li>Sample query: up to {@link QualityQueryResult#MAX_SAMPLES} failing rows</li>
 * </ol>
 */
public final class SqlQualityRuleExecutor implements QualityRuleExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SqlQualityRuleExecutor.class);

    /** Nanoseconds per millisecond, for elapsed-time conversion. */
    private static final long NANOS_PER_MILLI = 1_000_000;

    private final GovernanceQueryRunner queryRunner;

    /**
     * Creates an executor that delegates SQL execution to the provided runner.
     *
     * @param queryRunner executes SQL against the query engine
     */
    public SqlQualityRuleExecutor(GovernanceQueryRunner queryRunner) {
        this.queryRunner = checkNotNull(queryRunner, "queryRunner must not be null");
    }

    @Override
    public QualityCheckResult execute(DataQualityRule rule) {
        checkNotNull(rule, "rule must not be null");
        if (!rule.enabled() || rule.executionIdentity() == null) {
            throw new IllegalArgumentException("Quality rule is disabled or has no execution identity");
        }
        validateExpression(rule);

        long startNanos = System.nanoTime();
        try {
            QualityQueryResult queryResult = runCountQuery(rule);
            List<List<String>> samples = queryResult.failedRows() > 0
                    ? runSampleQuery(rule)
                    : List.of();

            long elapsedMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI;
            boolean passed = queryResult.failedRows() == 0;

            logAuditResult(rule, passed, queryResult.totalRows(),
                    queryResult.failedRows(), elapsedMs);

            return new QualityCheckResult(
                    0, rule.ruleId(), rule.ruleName(), rule.tableName(),
                    rule.severity(), Instant.now(), passed,
                    queryResult.totalRows(), queryResult.failedRows(),
                    elapsedMs, passed ? null : buildErrorMessage(rule, queryResult, samples));
        } catch (GovernanceQueryException e) {
            long elapsedMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI;
            LOG.error("event=quality_rule_sql_failed ruleId={} ruleName={} sql={} error={}",
                    rule.ruleId(), rule.ruleName(), e.sql(), e.getMessage());
            return new QualityCheckResult(
                    0, rule.ruleId(), rule.ruleName(), rule.tableName(),
                    rule.severity(), Instant.now(), false, 0, 0, elapsedMs,
                    e.getMessage());
        }
    }

    private void validateExpression(DataQualityRule rule) {
        if (rule.checkExpression() == null || rule.checkExpression().isBlank()) {
            throw new GovernanceQueryException("",
                    "Rule '" + rule.ruleName() + "' has blank check expression");
        }
    }

    private QualityQueryResult runCountQuery(DataQualityRule rule) {
        return queryRunner.execute(request(rule, GovernanceQueryRunner.QueryType.COUNTS), rule.executionIdentity());
    }

    private List<List<String>> runSampleQuery(DataQualityRule rule) {
        return queryRunner.execute(request(rule, GovernanceQueryRunner.QueryType.SAMPLES),
                rule.executionIdentity()).failingSamples();
    }

    private static QualityReadRequest request(DataQualityRule rule, GovernanceQueryRunner.QueryType type) {
        return new QualityReadRequest(rule.catalogName(), rule.schemaName(), rule.tableName(),
                rule.checkExpression(), type);
    }

    private String buildErrorMessage(DataQualityRule rule, QualityQueryResult result,
                                     List<List<String>> samples) {
        StringBuilder sb = new StringBuilder();
        sb.append("Rule '").append(rule.ruleName()).append("' failed: ");
        sb.append(result.failedRows()).append(" of ").append(result.totalRows());
        sb.append(" rows violate '").append(rule.checkExpression()).append("'");
        if (!samples.isEmpty()) {
            sb.append(". Sample violations: ").append(samples.size()).append(" rows");
        }
        return sb.toString();
    }

    private void logAuditResult(DataQualityRule rule, boolean passed,
                                long totalRows, long failedRows, long elapsedMs) {
        LOG.info("event=quality_rule_audit ruleId={} ruleName={} tableName={} "
                        + "passed={} totalRows={} failedRows={} executionTimeMs={}",
                rule.ruleId(), rule.ruleName(), rule.tableName(),
                passed, totalRows, failedRows, elapsedMs);
    }
}
