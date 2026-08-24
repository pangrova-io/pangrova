package org.pangrova.metadata.governance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.governance.quality.rule.QualityRuleStore;
import org.pangrova.metadata.exception.MetastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-backed implementation of {@link QualityRuleStore}.
 *
 * <p>Persists data quality rules and check results to the
 * {@code quality_rules} and {@code quality_check_results} tables.
 * Uses PostgreSQL {@code INSERT ... ON CONFLICT} for dialect-aware upsert.</p>
 *
 * <p>Concurrency strategy: relies on database-level row locking.
 * No in-process mutable state.</p>
 */
@ThreadSafe
public final class JdbcQualityRuleStore implements QualityRuleStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcQualityRuleStore.class);

    // ── DDL ──────────────────────────────────────────────────────────

    static final String DDL_QUALITY_RULES = """
            CREATE TABLE IF NOT EXISTS quality_rules (
                rule_id            BIGINT        NOT NULL PRIMARY KEY,
                rule_name          VARCHAR(512)  NOT NULL,
                catalog_name       VARCHAR(255)  NOT NULL,
                schema_name        VARCHAR(255)  NOT NULL,
                table_name         VARCHAR(255)  NOT NULL,
                check_expression   TEXT          NOT NULL,
                severity           VARCHAR(32)   NOT NULL,
                message            TEXT,
                enabled            BOOLEAN       NOT NULL DEFAULT TRUE,
                schedule           VARCHAR(255),
                created_at         TIMESTAMP     NOT NULL,
                updated_at         TIMESTAMP     NOT NULL
            )
            """;

    static final String DDL_QUALITY_CHECK_RESULTS = """
            CREATE TABLE IF NOT EXISTS quality_check_results (
                result_id          BIGINT        NOT NULL PRIMARY KEY,
                rule_id            BIGINT        NOT NULL,
                rule_name          VARCHAR(512)  NOT NULL,
                table_name         VARCHAR(255)  NOT NULL,
                severity           VARCHAR(32)   NOT NULL,
                check_time         TIMESTAMP     NOT NULL,
                passed             BOOLEAN       NOT NULL,
                total_rows         BIGINT        NOT NULL DEFAULT 0,
                failed_rows        BIGINT        NOT NULL DEFAULT 0,
                execution_time_ms  BIGINT        NOT NULL DEFAULT 0,
                error_message      TEXT
            )
            """;

    // ── SQL Constants ────────────────────────────────────────────────

    private static final String SQL_MERGE_RULE =
            "INSERT INTO quality_rules (rule_id, rule_name, catalog_name, schema_name, "
                    + "table_name, check_expression, severity, message, enabled, schedule, "
                    + "created_at, updated_at) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (rule_id) DO UPDATE SET "
                    + "rule_name=EXCLUDED.rule_name, catalog_name=EXCLUDED.catalog_name, "
                    + "schema_name=EXCLUDED.schema_name, table_name=EXCLUDED.table_name, "
                    + "check_expression=EXCLUDED.check_expression, severity=EXCLUDED.severity, "
                    + "message=EXCLUDED.message, enabled=EXCLUDED.enabled, "
                    + "schedule=EXCLUDED.schedule, updated_at=EXCLUDED.updated_at";

    private static final String SQL_GET_RULE =
            "SELECT * FROM quality_rules WHERE rule_id = ?";

    private static final String SQL_GET_RULES_FOR_TABLE =
            "SELECT * FROM quality_rules "
                    + "WHERE catalog_name = ? AND schema_name = ? AND table_name = ?";

    private static final String SQL_GET_ALL_ENABLED =
            "SELECT * FROM quality_rules WHERE enabled = TRUE";

    private static final String SQL_DELETE_RULE =
            "DELETE FROM quality_rules WHERE rule_id = ?";

    private static final String SQL_INSERT_RESULT =
            "INSERT INTO quality_check_results (result_id, rule_id, rule_name, table_name, "
                    + "severity, check_time, passed, total_rows, failed_rows, "
                    + "execution_time_ms, error_message) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (result_id) DO UPDATE SET "
                    + "rule_id=EXCLUDED.rule_id, rule_name=EXCLUDED.rule_name, "
                    + "table_name=EXCLUDED.table_name, severity=EXCLUDED.severity, "
                    + "check_time=EXCLUDED.check_time, passed=EXCLUDED.passed, "
                    + "total_rows=EXCLUDED.total_rows, failed_rows=EXCLUDED.failed_rows, "
                    + "execution_time_ms=EXCLUDED.execution_time_ms, "
                    + "error_message=EXCLUDED.error_message";

    // ── Fields ───────────────────────────────────────────────────────

    private final DataSource dataSource;

    public JdbcQualityRuleStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /**
     * Initializes the quality_rules and quality_check_results tables if they do not exist.
     * Call once during bootstrap.
     */
    public void initSchema() {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute(DDL_QUALITY_RULES);
            stmt.execute(DDL_QUALITY_CHECK_RESULTS);
            LOG.info("event=quality_rule_schema_initialized");
        } catch (SQLException e) {
            throw new MetastoreException("Failed to initialize quality rule schema", e);
        }
    }

    // ── QualityRuleStore Implementation ──────────────────────────────

    @Override
    public void saveRule(DataQualityRule rule) {
        Objects.requireNonNull(rule, "rule must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_MERGE_RULE)) {
            bindRule(ps, rule);
            ps.executeUpdate();
            LOG.info("event=quality_rule_saved ruleId={} ruleName={}", rule.ruleId(), rule.ruleName());
        } catch (SQLException e) {
            throw new MetastoreException("Failed to save quality rule ruleId=" + rule.ruleId(), e);
        }
    }

    @Override
    public Optional<DataQualityRule> getRule(long ruleId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_RULE)) {
            ps.setLong(1, ruleId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRule(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new MetastoreException("Failed to get quality rule ruleId=" + ruleId, e);
        }
    }

    @Override
    public List<DataQualityRule> getRulesForTable(String catalogName, String schemaName,
                                                   String tableName) {
        Objects.requireNonNull(catalogName, "catalogName must not be null");
        Objects.requireNonNull(schemaName, "schemaName must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_RULES_FOR_TABLE)) {
            ps.setString(1, catalogName);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            return collectRules(ps);
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to get rules for table=" + catalogName + "." + schemaName + "." + tableName, e);
        }
    }

    @Override
    public List<DataQualityRule> getAllEnabledRules() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_ALL_ENABLED)) {
            return collectRules(ps);
        } catch (SQLException e) {
            throw new MetastoreException("Failed to list enabled quality rules", e);
        }
    }

    @Override
    public void saveResult(QualityCheckResult result) {
        Objects.requireNonNull(result, "result must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_INSERT_RESULT)) {
            bindResult(ps, result);
            ps.executeUpdate();
            LOG.debug("event=quality_result_saved resultId={} ruleId={} passed={}",
                    result.resultId(), result.ruleId(), result.passed());
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to save quality check result resultId=" + result.resultId(), e);
        }
    }

    /**
     * Deletes a quality rule by its ID.
     *
     * @param ruleId the rule identifier
     * @return {@code true} if a rule was deleted, {@code false} if no rule existed
     */
    public boolean deleteRule(long ruleId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_DELETE_RULE)) {
            ps.setLong(1, ruleId);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=quality_rule_deleted ruleId={}", ruleId);
            }
            return deleted > 0;
        } catch (SQLException e) {
            throw new MetastoreException("Failed to delete quality rule ruleId=" + ruleId, e);
        }
    }

    // ── Private Helpers ──────────────────────────────────────────────

    // PreparedStatement parameter indices beyond the single-digit range.
    private static final int RULE_CREATED_AT_PARAM = 11;
    private static final int RULE_UPDATED_AT_PARAM = 12;
    private static final int RESULT_ERROR_MESSAGE_PARAM = 11;

    private static List<DataQualityRule> collectRules(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<DataQualityRule> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapRule(rs));
            }
            return Collections.unmodifiableList(result);
        }
    }

    private static void bindRule(PreparedStatement ps, DataQualityRule rule) throws SQLException {
        ps.setLong(1, rule.ruleId());
        ps.setString(2, rule.ruleName());
        ps.setString(3, rule.catalogName());
        ps.setString(4, rule.schemaName());
        ps.setString(5, rule.tableName());
        ps.setString(6, rule.checkExpression());
        ps.setString(7, rule.severity().name());
        ps.setString(8, rule.message());
        ps.setBoolean(9, rule.enabled());
        ps.setString(10, rule.schedule());
        ps.setTimestamp(RULE_CREATED_AT_PARAM, rule.createdAt() != null
                ? Timestamp.from(rule.createdAt()) : Timestamp.from(Instant.now()));
        ps.setTimestamp(RULE_UPDATED_AT_PARAM, rule.updatedAt() != null
                ? Timestamp.from(rule.updatedAt()) : Timestamp.from(Instant.now()));
    }

    private static void bindResult(PreparedStatement ps, QualityCheckResult result)
            throws SQLException {
        ps.setLong(1, result.resultId());
        ps.setLong(2, result.ruleId());
        ps.setString(3, result.ruleName());
        ps.setString(4, result.tableName());
        ps.setString(5, result.severity().name());
        ps.setTimestamp(6, result.checkTime() != null
                ? Timestamp.from(result.checkTime()) : Timestamp.from(Instant.now()));
        ps.setBoolean(7, result.passed());
        ps.setLong(8, result.totalRows());
        ps.setLong(9, result.failedRows());
        ps.setLong(10, result.executionTimeMs());
        ps.setString(RESULT_ERROR_MESSAGE_PARAM, result.errorMessage());
    }

    private static DataQualityRule mapRule(ResultSet rs) throws SQLException {
        Timestamp createdAt = rs.getTimestamp("created_at");
        Timestamp updatedAt = rs.getTimestamp("updated_at");
        return new DataQualityRule(
                rs.getLong("rule_id"),
                rs.getString("rule_name"),
                rs.getString("catalog_name"),
                rs.getString("schema_name"),
                rs.getString("table_name"),
                rs.getString("check_expression"),
                Severity.valueOf(rs.getString("severity")),
                rs.getString("message"),
                rs.getBoolean("enabled"),
                rs.getString("schedule"),
                createdAt != null ? createdAt.toInstant() : null,
                updatedAt != null ? updatedAt.toInstant() : null);
    }
}
