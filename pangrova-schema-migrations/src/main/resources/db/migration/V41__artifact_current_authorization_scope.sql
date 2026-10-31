-- Exact permissions and actual compilation-time policy observations.
-- Older unscoped jobs deliberately cannot replay; resubmit them with current authorization.
ALTER TABLE artifact_build_dispatch_specs ADD COLUMN authorization_scope_bytes BYTEA;
