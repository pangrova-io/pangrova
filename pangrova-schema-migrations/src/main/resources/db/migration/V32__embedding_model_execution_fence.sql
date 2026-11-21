-- Exact Worker deployment selected when an embedding model catalog revision is created.
-- ALTER clears both fields so changed catalog metadata cannot execute against an old Worker.
ALTER TABLE pangrova_embedding_models
    ADD COLUMN IF NOT EXISTS deployment_version VARCHAR(256) NOT NULL DEFAULT '';

ALTER TABLE pangrova_embedding_models
    ADD COLUMN IF NOT EXISTS deployment_signature_sha256 VARCHAR(64) NOT NULL DEFAULT '';
