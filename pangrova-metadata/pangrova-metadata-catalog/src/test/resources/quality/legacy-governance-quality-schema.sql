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
);

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
);
