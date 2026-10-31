-- Evaluation runs may pin a published Feature View artifact generation.
-- The generic artifact ID column supports 1024 characters; keep the evaluation
-- subject key equally wide so a valid published export remains addressable.
ALTER TABLE evaluation_runs
    ALTER COLUMN subject_id TYPE VARCHAR(1024);

ALTER TABLE evaluation_runs
    DROP CONSTRAINT chk_evaluation_subject_type;

ALTER TABLE evaluation_runs
    ADD CONSTRAINT chk_evaluation_subject_type CHECK
        (subject_type IN ('MODEL', 'EMBEDDING_MODEL', 'SEARCH_SERVICE',
                          'SEMANTIC_VIEW', 'FEATURE_VIEW', 'INDEX', 'CAPABILITY'));

-- The completion transaction is the only writer allowed to add metric rows.
-- This permits JdbcEvaluationStore's terminal UPDATE followed by INSERTs in
-- that same transaction, while rejecting later SQL INSERTs after commit.
ALTER TABLE evaluation_runs
    ADD COLUMN terminal_txid BIGINT;

ALTER TABLE evaluation_runs
    ADD CONSTRAINT chk_evaluation_run_time_order CHECK
        (completed_at IS NULL OR completed_at >= started_at);

CREATE OR REPLACE FUNCTION evaluation_run_metric_policy_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'RUNNING' OR NOT EXISTS (
                SELECT 1 FROM evaluation_metric_policies
                WHERE tenant_id = NEW.tenant_id
                  AND policy_version = NEW.policy_version
                  AND sealed = TRUE) THEN
            RAISE EXCEPTION 'evaluation run requires RUNNING state and a sealed metric policy';
        END IF;
        NEW.metric_policy_pinned := TRUE;
        NEW.terminal_txid := NULL;
        RETURN NEW;
    END IF;
    IF ROW(NEW.tenant_id, NEW.run_id, NEW.dataset_id, NEW.dataset_version,
           NEW.subject_type, NEW.subject_id, NEW.subject_version,
           NEW.baseline_run_id, NEW.policy_version, NEW.started_at,
           NEW.metric_policy_pinned)
       IS DISTINCT FROM
       ROW(OLD.tenant_id, OLD.run_id, OLD.dataset_id, OLD.dataset_version,
           OLD.subject_type, OLD.subject_id, OLD.subject_version,
           OLD.baseline_run_id, OLD.policy_version, OLD.started_at,
           OLD.metric_policy_pinned) THEN
        RAISE EXCEPTION 'evaluation run identity and metric policy pin are immutable';
    END IF;
    IF OLD.state <> 'RUNNING' OR NEW.state = 'RUNNING' THEN
        RAISE EXCEPTION 'evaluation run permits only one terminal transition';
    END IF;
    NEW.terminal_txid := txid_current();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION evaluation_metric_write_guard() RETURNS trigger AS $$
DECLARE
    run_state VARCHAR(16);
    completion_txid BIGINT;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'evaluation metrics are immutable';
    END IF;
    SELECT state, terminal_txid INTO run_state, completion_txid
        FROM evaluation_runs
        WHERE tenant_id = NEW.tenant_id AND run_id = NEW.run_id;
    IF NOT FOUND OR run_state = 'RUNNING'
            OR completion_txid IS DISTINCT FROM txid_current()
            OR EXISTS (SELECT 1 FROM evaluation_release_decisions
                       WHERE tenant_id = NEW.tenant_id AND run_id = NEW.run_id) THEN
        RAISE EXCEPTION 'evaluation metrics require the run completion transaction';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evaluation_metrics_completion_only
    BEFORE INSERT OR UPDATE OR DELETE ON evaluation_metrics
    FOR EACH ROW EXECUTE FUNCTION evaluation_metric_write_guard();

CREATE TRIGGER evaluation_metrics_no_truncate
    BEFORE TRUNCATE ON evaluation_metrics
    FOR EACH STATEMENT EXECUTE FUNCTION evaluation_metric_write_guard();

CREATE FUNCTION evaluation_evidence_immutable_guard() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'published evaluation evidence is immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evaluation_dataset_versions_no_mutation
    BEFORE UPDATE OR DELETE ON evaluation_dataset_versions
    FOR EACH ROW EXECUTE FUNCTION evaluation_evidence_immutable_guard();

CREATE TRIGGER evaluation_dataset_versions_no_truncate
    BEFORE TRUNCATE ON evaluation_dataset_versions
    FOR EACH STATEMENT EXECUTE FUNCTION evaluation_evidence_immutable_guard();

CREATE TRIGGER evaluation_release_decisions_no_mutation
    BEFORE UPDATE OR DELETE ON evaluation_release_decisions
    FOR EACH ROW EXECUTE FUNCTION evaluation_evidence_immutable_guard();

CREATE TRIGGER evaluation_release_decisions_no_truncate
    BEFORE TRUNCATE ON evaluation_release_decisions
    FOR EACH STATEMENT EXECUTE FUNCTION evaluation_evidence_immutable_guard();

CREATE FUNCTION evaluation_release_decision_time_guard() RETURNS trigger AS $$
DECLARE
    run_state VARCHAR(16);
    run_policy_version VARCHAR(255);
    run_completed_at TIMESTAMPTZ(6);
BEGIN
    SELECT state, policy_version, completed_at
        INTO run_state, run_policy_version, run_completed_at
        FROM evaluation_runs
        WHERE tenant_id = NEW.tenant_id AND run_id = NEW.run_id
        FOR SHARE;
    IF NOT FOUND OR run_state = 'RUNNING'
            OR NEW.policy_version <> run_policy_version
            OR NEW.decided_at < run_completed_at
            OR (NEW.approved AND run_state <> 'SUCCEEDED') THEN
        RAISE EXCEPTION 'release decision requires a terminal matching run and decided_at not before completed_at';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evaluation_release_decisions_time_order
    BEFORE INSERT ON evaluation_release_decisions
    FOR EACH ROW EXECUTE FUNCTION evaluation_release_decision_time_guard();

-- A RUNNING or failed run may have no metric or decision children. Preserve that
-- audit record as well; foreign keys alone cannot protect it from deletion.
CREATE TRIGGER evaluation_runs_no_delete
    BEFORE DELETE ON evaluation_runs
    FOR EACH ROW EXECUTE FUNCTION evaluation_evidence_immutable_guard();

CREATE TRIGGER evaluation_runs_no_truncate
    BEFORE TRUNCATE ON evaluation_runs
    FOR EACH STATEMENT EXECUTE FUNCTION evaluation_evidence_immutable_guard();
