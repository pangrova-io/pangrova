-- Names cannot prove which historical physical table a quality rule evaluated.
-- Preserve old rules/results unbound; production admission requires explicit stable ownership.
ALTER TABLE quality_rules ADD COLUMN IF NOT EXISTS table_id UUID;
ALTER TABLE quality_check_results ADD COLUMN IF NOT EXISTS table_id UUID;
CREATE INDEX IF NOT EXISTS quality_rule_table_identity ON quality_rules(table_id);
CREATE INDEX IF NOT EXISTS quality_result_table_identity ON quality_check_results(table_id,rule_id);

-- Keep the original owner after rule deletion so delayed audit results remain attributable
-- and the same numeric rule ID can never be reassigned to another table incarnation.
CREATE TABLE quality_rule_identity (
    rule_id BIGINT PRIMARY KEY,
    table_id UUID NOT NULL,
    UNIQUE(rule_id,table_id)
);
ALTER TABLE quality_rules ADD CONSTRAINT quality_rule_owner
    FOREIGN KEY(rule_id,table_id) REFERENCES quality_rule_identity(rule_id,table_id);
ALTER TABLE quality_check_results ADD CONSTRAINT quality_result_owner
    FOREIGN KEY(rule_id,table_id) REFERENCES quality_rule_identity(rule_id,table_id);
