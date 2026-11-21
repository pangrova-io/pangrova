-- Converge the obsolete Governance tables into the existing Metadata authority.
-- PostgreSQL/Flyway owns one transaction. Conflicts fail without dropping either fact.
DO $$
BEGIN
    IF to_regclass(format('%I.cdc_quality_rules', current_schema())) IS NOT NULL THEN
        LOCK TABLE cdc_quality_rules, quality_rules IN ACCESS EXCLUSIVE MODE;
        IF EXISTS (
            SELECT 1 FROM cdc_quality_rules legacy JOIN quality_rules canonical USING (rule_id)
            WHERE ROW(legacy.rule_name, legacy.catalog_name, legacy.schema_name, legacy.table_name,
                      legacy.check_expression, legacy.severity, legacy.message, legacy.enabled,
                      legacy.schedule, legacy.created_at, legacy.updated_at)
                IS DISTINCT FROM
                  ROW(canonical.rule_name, canonical.catalog_name, canonical.schema_name, canonical.table_name,
                      canonical.check_expression, canonical.severity, canonical.message, canonical.enabled,
                      canonical.schedule, canonical.created_at, canonical.updated_at)
        ) THEN
            RAISE EXCEPTION 'Quality rule identities conflict between legacy and canonical metadata';
        END IF;
        INSERT INTO quality_rules (rule_id, rule_name, catalog_name, schema_name, table_name,
                                   check_expression, severity, message, enabled, schedule, created_at, updated_at)
        SELECT rule_id, rule_name, catalog_name, schema_name, table_name,
               check_expression, severity, message, enabled, schedule, created_at, updated_at
        FROM cdc_quality_rules ON CONFLICT (rule_id) DO NOTHING;
    END IF;
    IF to_regclass(format('%I.cdc_quality_results', current_schema())) IS NOT NULL THEN
        LOCK TABLE cdc_quality_results, quality_check_results IN ACCESS EXCLUSIVE MODE;
        IF EXISTS (
            SELECT 1 FROM cdc_quality_results legacy JOIN quality_check_results canonical USING (result_id)
            WHERE ROW(legacy.rule_id, legacy.rule_name, legacy.table_name, legacy.severity, legacy.check_time,
                      legacy.passed, legacy.total_rows, legacy.failed_rows, legacy.execution_time_ms,
                      legacy.error_message)
                IS DISTINCT FROM
                  ROW(canonical.rule_id, canonical.rule_name, canonical.table_name, canonical.severity,
                      canonical.check_time, canonical.passed, canonical.total_rows, canonical.failed_rows,
                      canonical.execution_time_ms, canonical.error_message)
        ) THEN
            RAISE EXCEPTION 'Quality result identities conflict between legacy and canonical metadata';
        END IF;
        INSERT INTO quality_check_results (result_id, rule_id, rule_name, table_name, severity,
                                          check_time, passed, total_rows, failed_rows, execution_time_ms, error_message)
        SELECT result_id, rule_id, rule_name, table_name, severity,
               check_time, passed, total_rows, failed_rows, execution_time_ms, error_message
        FROM cdc_quality_results ON CONFLICT (result_id) DO NOTHING;
    END IF;
    DROP TABLE IF EXISTS cdc_quality_results;
    DROP TABLE IF EXISTS cdc_quality_rules;
END $$;
