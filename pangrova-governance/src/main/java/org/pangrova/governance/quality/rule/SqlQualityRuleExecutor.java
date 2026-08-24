package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Executes data quality rules by running SQL against actual table data
 * via the internal query pipeline.
 *
 * <p>For each rule, constructs a SQL query that counts total rows and
 * violating rows, then collects a bounded sample of failing rows for
 * diagnostic purposes. Results are reported to the governance audit log.</p>
 *
 * <p>SQL construction uses the rule's {@code checkExpression} as a WHERE
 * predicate. A row passes if the expression evaluates to true; it fails
 * otherwise. The executor runs two queries:</p>
 * <ol>
 *   <li>Count query: total rows and failing rows</li>
 *   <li>Sample query: up to {@link QualityQueryResult#MAX_SAMPLES} failing rows</li>
 * </ol>
 */
public final class SqlQualityRuleExecutor implements QualityRuleExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SqlQualityRuleExecutor.class);

    /** Whitelist pattern for SQL identifiers: letters, digits, underscores, hyphens. */
    private static final Pattern SAFE_IDENTIFIER_PATTERN =
            Pattern.compile("[a-zA-Z_][a-zA-Z0-9_\\-]*");

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
        String countSql = buildCountSql(rule);
        return queryRunner.execute(countSql);
    }

    private List<List<String>> runSampleQuery(DataQualityRule rule) {
        String sampleSql = buildSampleSql(rule);
        QualityQueryResult sampleResult = queryRunner.execute(sampleSql);
        return sampleResult.failingSamples();
    }

    /**
     * Builds SQL that counts total rows and rows violating the rule expression.
     *
     * <p>Result schema: total_rows (BIGINT), failed_rows (BIGINT)</p>
     *
     * <p>Table identifiers are quoted to prevent injection. The check expression
     * is validated against disallowed patterns as a defense-in-depth measure.</p>
     */
    String buildCountSql(DataQualityRule rule) {
        String fqn = safeQualifiedTableName(rule);
        String expression = validateCheckExpression(rule.checkExpression());
        return String.format(Locale.ROOT,
                "SELECT COUNT(*) AS total_rows, "
                        + "COUNT(CASE WHEN NOT (%s) THEN 1 END) AS failed_rows "
                        + "FROM %s",
                expression, fqn);
    }

    /**
     * Builds SQL that retrieves a sample of rows violating the rule expression.
     */
    String buildSampleSql(DataQualityRule rule) {
        String fqn = safeQualifiedTableName(rule);
        String expression = validateCheckExpression(rule.checkExpression());
        return String.format(Locale.ROOT,
                "SELECT * FROM %s WHERE NOT (%s) LIMIT %d",
                fqn, expression, QualityQueryResult.MAX_SAMPLES);
    }

    /**
     * Constructs a fully-qualified table name from quoted identifier components.
     * Each component is double-quoted to prevent injection via crafted names.
     */
    private String safeQualifiedTableName(DataQualityRule rule) {
        return quoteIdentifier(rule.catalogName()) + "."
                + quoteIdentifier(rule.schemaName()) + "."
                + quoteIdentifier(rule.tableName());
    }

    /**
     * Quotes a SQL identifier using double-quotes, escaping any embedded double-quotes.
     * Rejects identifiers containing characters outside the safe set.
     */
    private static String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new GovernanceQueryException("",
                    "Identifier must not be null or blank");
        }
        if (!SAFE_IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new GovernanceQueryException("",
                    "Identifier contains unsafe characters: " + identifier);
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /** Keywords that indicate advanced injection attempts beyond simple comments/semicolons. */
    private static final Set<String> BLOCKED_KEYWORDS = Set.of(
            "union", "into", "exec", "execute", "xp_", "sp_",
            "drop", "alter", "create", "truncate", "grant", "revoke",
            "insert", "update", "delete", "merge"
    );

    /**
     * Validates that a check expression does not contain obvious injection patterns.
     * Rejects semicolons, line comments, block comments, UNION SELECT, subqueries,
     * and DDL/DML keywords as defense-in-depth.
     */
    private static String validateCheckExpression(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new GovernanceQueryException("",
                    "Check expression must not be null or blank");
        }
        String lower = expression.toLowerCase(Locale.ROOT);
        if (lower.contains(";") || lower.contains("--") || lower.contains("/*")) {
            throw new GovernanceQueryException(expression,
                    "Check expression contains disallowed characters: " + expression);
        }
        // Detect advanced injection: UNION SELECT, subqueries, DDL/DML keywords
        for (String keyword : BLOCKED_KEYWORDS) {
            // Match whole-word boundaries to avoid false positives (e.g., "updated_at")
            if (lower.matches(".*\\b" + keyword + "\\b.*")) {
                throw new GovernanceQueryException(expression,
                        "Check expression contains blocked keyword '" + keyword + "': " + expression);
            }
        }
        return expression;
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
