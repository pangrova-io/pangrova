-- Keep the submitting subject and exact raw-read scope with the scalar index
-- descriptor. Historical descriptors remain unscoped and cannot be replayed.
ALTER TABLE index_descriptors
    ADD COLUMN build_principal VARCHAR(255),
    ADD COLUMN build_authorization_scope BYTEA;

ALTER TABLE index_descriptors
    ADD CONSTRAINT ck_index_build_authority_pair
    CHECK ((build_principal IS NULL) = (build_authorization_scope IS NULL));
