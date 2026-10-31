-- Existing rules have no verifiable owner or tenant. Preserve the definitions,
-- but require explicit reassignment and authorization before enabling execution.
ALTER TABLE quality_rules
    ADD COLUMN execution_principal VARCHAR(512),
    ADD COLUMN execution_tenant VARCHAR(255);

UPDATE quality_rules SET enabled = FALSE WHERE execution_principal IS NULL;

ALTER TABLE quality_rules ADD CONSTRAINT quality_rule_execution_identity CHECK (
    (execution_principal IS NULL AND execution_tenant IS NULL AND NOT enabled)
    OR (execution_principal IS NOT NULL AND length(trim(execution_principal)) > 0
        AND execution_tenant IS NOT NULL AND length(trim(execution_tenant)) > 0)
);
