-- A run pins one immutable, tenant-scoped numerical release policy version.
-- Rules are inserted transactionally with their parent policy by EvaluationStore.
CREATE TABLE evaluation_metric_policies (
    tenant_id       VARCHAR(255) NOT NULL,
    policy_version  VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sealed          BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (tenant_id, policy_version)
);

CREATE TABLE evaluation_metric_policy_rules (
    tenant_id                   VARCHAR(255) NOT NULL,
    policy_version              VARCHAR(255) NOT NULL,
    metric_name                 VARCHAR(255) NOT NULL,
    metric_unit                 VARCHAR(64) NOT NULL,
    direction                   VARCHAR(32) NOT NULL,
    threshold_value             DOUBLE PRECISION NOT NULL,
    maximum_baseline_regression DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (tenant_id, policy_version, metric_name),
    CONSTRAINT fk_evaluation_metric_policy FOREIGN KEY (tenant_id, policy_version)
        REFERENCES evaluation_metric_policies (tenant_id, policy_version),
    CONSTRAINT chk_evaluation_metric_policy_direction CHECK
        (direction IN ('HIGHER_IS_BETTER', 'LOWER_IS_BETTER')),
    CONSTRAINT chk_evaluation_metric_policy_finite CHECK
        (threshold_value NOT IN ('NaN'::DOUBLE PRECISION,
                                 'Infinity'::DOUBLE PRECISION,
                                 '-Infinity'::DOUBLE PRECISION)
         AND maximum_baseline_regression >= 0
         AND maximum_baseline_regression NOT IN ('NaN'::DOUBLE PRECISION,
                 'Infinity'::DOUBLE PRECISION, '-Infinity'::DOUBLE PRECISION))
);

CREATE FUNCTION evaluation_metric_policy_header_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF OLD.sealed = FALSE AND NEW.sealed = TRUE
                AND NEW.tenant_id = OLD.tenant_id
                AND NEW.policy_version = OLD.policy_version
                AND NEW.created_at = OLD.created_at
                AND EXISTS (SELECT 1 FROM evaluation_metric_policy_rules
                            WHERE tenant_id = OLD.tenant_id
                              AND policy_version = OLD.policy_version) THEN
            RETURN NEW;
        END IF;
    END IF;
    RAISE EXCEPTION 'evaluation metric policies are immutable after sealing';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evaluation_metric_policies_no_mutation
    BEFORE UPDATE OR DELETE ON evaluation_metric_policies
    FOR EACH ROW EXECUTE FUNCTION evaluation_metric_policy_header_guard();

CREATE TRIGGER evaluation_metric_policies_no_truncate
    BEFORE TRUNCATE ON evaluation_metric_policies
    FOR EACH STATEMENT EXECUTE FUNCTION evaluation_metric_policy_header_guard();

CREATE FUNCTION evaluation_metric_policy_rule_guard() RETURNS trigger AS $$
DECLARE
    policy_sealed BOOLEAN;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'evaluation metric policy rules are immutable';
    END IF;
    SELECT sealed INTO policy_sealed FROM evaluation_metric_policies
        WHERE tenant_id = NEW.tenant_id AND policy_version = NEW.policy_version
        FOR UPDATE;
    IF policy_sealed IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION 'evaluation metric policy is sealed';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evaluation_metric_policy_rules_no_mutation
    BEFORE INSERT OR UPDATE OR DELETE ON evaluation_metric_policy_rules
    FOR EACH ROW EXECUTE FUNCTION evaluation_metric_policy_rule_guard();

CREATE TRIGGER evaluation_metric_policy_rules_no_truncate
    BEFORE TRUNCATE ON evaluation_metric_policy_rules
    FOR EACH STATEMENT EXECUTE FUNCTION evaluation_metric_policy_rule_guard();

-- Existing runs have no provable policy-registration ordering and fail closed
-- for approval. New runs are marked only by this database trigger after it
-- confirms that their policy version was already sealed.
ALTER TABLE evaluation_runs
    ADD COLUMN metric_policy_pinned BOOLEAN NOT NULL DEFAULT FALSE;

CREATE FUNCTION evaluation_run_metric_policy_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NOT EXISTS (SELECT 1 FROM evaluation_metric_policies
                       WHERE tenant_id = NEW.tenant_id
                         AND policy_version = NEW.policy_version
                         AND sealed = TRUE) THEN
            RAISE EXCEPTION 'evaluation run requires a sealed metric policy';
        END IF;
        NEW.metric_policy_pinned := TRUE;
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
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evaluation_runs_pin_metric_policy
    BEFORE INSERT ON evaluation_runs
    FOR EACH ROW EXECUTE FUNCTION evaluation_run_metric_policy_guard();

CREATE TRIGGER evaluation_runs_immutable_identity_and_terminal
    BEFORE UPDATE ON evaluation_runs
    FOR EACH ROW EXECUTE FUNCTION evaluation_run_metric_policy_guard();
