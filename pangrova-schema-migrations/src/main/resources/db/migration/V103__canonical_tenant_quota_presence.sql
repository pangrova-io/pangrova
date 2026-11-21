-- The canonical Tenant owner is tenants + tenant_user_memberships (V1/V4).
-- Keep absent quota distinct from an explicitly configured all-zero quota.
ALTER TABLE tenants ADD COLUMN quota_present BOOLEAN NOT NULL DEFAULT TRUE;

-- Tenant facts must fence current-authority checks even before optional security stores are used.
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON tenants
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
CREATE OR REPLACE TRIGGER pangrova_authority_gate
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON tenant_user_memberships
FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate();
