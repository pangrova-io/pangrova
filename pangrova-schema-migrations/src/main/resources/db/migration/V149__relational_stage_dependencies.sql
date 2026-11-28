-- The immutable stage exchange is the dependency for every relational consumer.
-- Remove the repeated per-page edges without changing artifact reduction inputs.
DELETE FROM artifact_build_page_dependencies d
USING artifact_build_pages p
WHERE p.tenant_id = d.tenant_id AND p.job_id = d.job_id AND p.page_id = d.page_id
  AND p.page_kind IN ('RELATIONAL_COMPUTE', 'RELATIONAL_PARTITION');
