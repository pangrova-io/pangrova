package org.pangrova.governance.quality.rule;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * JDBC-backed {@link QualityRuleStore} for persistent quality rule storage (F-GOV-04).
 *
 * <p>Persists rules and results to a shared JDBC state store, ensuring rules survive
 * coordinator restarts and are visible across all coordinators in a multi-node cluster.</p>
 *
 * <p>Concurrency strategy: relies on database-level row locking for atomicity;
 * each operation uses a short-lived connection from the pool.</p>
 */
@ThreadSafe
public final class JdbcQualityRuleStore implements QualityRuleStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcQualityRuleStore.class);

    private static final String CREATE_RULES_TABLE = """
            CREATE TABLE IF NOT EXISTS cdc_quality_rules (
                rule_id BIGINT PRIMARY KEY,
                rule_name VARCHAR(255) NOT NULL,
                catalog_name VARCHAR(255) NOT NULL,
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                check_expression TEXT NOT NULL,
                severity VARCHAR(32) NOT NULL,
                message TEXT,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                schedule VARCHAR(255),
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )""";

    private static final String CREATE_RESULTS_TABLE = """
            CREATE TABLE IF NOT EXISTS cdc_quality_results (
                result_id BIGINT PRIMARY KEY,
                rule_id BIGINT NOT NULL,
                rule_name VARCHAR(255),
                table_name VARCHAR(255),
                severity VARCHAR(32),
                check_time TIMESTAMP NOT NULL,
                passed BOOLEAN NOT NULL,
                total_rows BIGINT,
                failed_rows BIGINT,
                execution_time_ms BIGINT,
                error_message TEXT
            )""";

    private static final String UPSERT_RULE = """
            INSERT INTO cdc_quality_rules (rule_id, rule_name, catalog_name, schema_name,
            table_name, check_expression, severity, message, enabled, schedule, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (rule_id) DO UPDATE SET
                rule_name = EXCLUDED.rule_name,
                catalog_name = EXCLUDED.catalog_name,
                schema_name = EXCLUDED.schema_name,
                table_name = EXCLUDED.table_name,
                check_expression = EXCLUDED.check_expression,
                severity = EXCLUDED.severity,
                message = EXCLUDED.message,
                enabled = EXCLUDED.enabled,
                schedule = EXCLUDED.schedule,
                updated_at = EXCLUDED.updated_at""";

    private static final String SELECT_RULE = "SELECT * FROM cdc_quality_rules WHERE rule_id = ?";
    private static final String SELECT_RULES_FOR_TABLE =
            "SELECT * FROM cdc_quality_rules WHERE catalog_name = ? AND schema_name = ? AND table_name = ?";
    private static final String SELECT_ENABLED_RULES = "SELECT * FROM cdc_quality_rules WHERE enabled = TRUE";
    private static final String INSERT_RESULT = """
            INSERT INTO cdc_quality_results (result_id, rule_id, rule_name, table_name,
            severity, check_time, passed, total_rows, failed_rows, execution_time_ms, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";

    /** Parameter index of {@code created_at} in {@link #UPSERT_RULE}. */
    private static final int RULE_CREATED_AT_INDEX = 11;
    /** Parameter index of {@code updated_at} in {@link #UPSERT_RULE}. */
    private static final int RULE_UPDATED_AT_INDEX = 12;
    /** Parameter index of {@code error_message} in {@link #INSERT_RESULT}. */
    private static final int RESULT_ERROR_MESSAGE_INDEX = 11;

    private final DataSource dataSource;

    public JdbcQualityRuleStore(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(CREATE_RULES_TABLE);
            statement.execute(CREATE_RESULTS_TABLE);
            LOG.info("event=jdbc_quality_rule_store_initialized");
        } catch (SQLException e) {
            throw qualityStoreFailure("initialize quality rule store", e);
        }
    }

    @Override
    public void saveRule(DataQualityRule rule) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_RULE)) {
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
            ps.setTimestamp(RULE_CREATED_AT_INDEX, Timestamp.from(rule.createdAt()));
            ps.setTimestamp(RULE_UPDATED_AT_INDEX, Timestamp.from(rule.updatedAt()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw qualityStoreFailure("save quality rule " + rule.ruleId(), e);
        }
    }

    @Override
    public Optional<DataQualityRule> getRule(long ruleId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_RULE)) {
            ps.setLong(1, ruleId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRule(rs));
                }
            }
        } catch (SQLException e) {
            throw qualityStoreFailure("read quality rule " + ruleId, e);
        }
        return Optional.empty();
    }

    @Override
    public List<DataQualityRule> getRulesForTable(String catalogName, String schemaName, String tableName) {
        List<DataQualityRule> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_RULES_FOR_TABLE)) {
            ps.setString(1, catalogName);
            ps.setString(2, schemaName);
            ps.setString(3, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRule(rs));
                }
            }
        } catch (SQLException e) {
            throw qualityStoreFailure("read quality rules for table", e);
        }
        return result;
    }

    @Override
    public List<DataQualityRule> getAllEnabledRules() {
        List<DataQualityRule> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ENABLED_RULES);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(mapRule(rs));
            }
        } catch (SQLException e) {
            throw qualityStoreFailure("read enabled quality rules", e);
        }
        return result;
    }

    @Override
    public void saveResult(QualityCheckResult result) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_RESULT)) {
            ps.setLong(1, result.resultId());
            ps.setLong(2, result.ruleId());
            ps.setString(3, result.ruleName());
            ps.setString(4, result.tableName());
            ps.setString(5, result.severity() != null ? result.severity().name() : null);
            ps.setTimestamp(6, Timestamp.from(result.checkTime()));
            ps.setBoolean(7, result.passed());
            ps.setLong(8, result.totalRows());
            ps.setLong(9, result.failedRows());
            ps.setLong(10, result.executionTimeMs());
            ps.setString(RESULT_ERROR_MESSAGE_INDEX, result.errorMessage());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw qualityStoreFailure("save quality check result " + result.resultId(), e);
        }
    }

    private static PangrovaException qualityStoreFailure(String operation, SQLException cause) {
        return new PangrovaException(PangrovaErrorCode.GOVERNANCE_QUALITY_CHECK_FAILED,
                "Failed to " + operation, cause);
    }

    private static DataQualityRule mapRule(ResultSet rs) throws SQLException {
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
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant()
        );
    }
}
