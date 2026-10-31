-- Historical models have no implicit invocation policy. Explicitly provision their current roles.
ALTER TABLE pangrova_embedding_models ADD COLUMN admin_role VARCHAR(256);
ALTER TABLE pangrova_embedding_models ADD COLUMN usage_role VARCHAR(256);
