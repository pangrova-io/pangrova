-- ai baseline: single migration owning the ai durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: evaluation dataset/run/metric/release, feature views, training datasets,
-- embedding models, catalog/tabular export generations.

;
;
;
;
;
;

-- PostgreSQL database dump

-- Name: catalog_export_requests; Type: TABLE; Schema: public; Owner: -

CREATE TABLE catalog_export_requests (
    tenant_id character varying(255) NOT NULL,
    object_kind character varying(32) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    object_name character varying(255) NOT NULL,
    object_version bigint NOT NULL,
    generation character varying(128) NOT NULL,
    artifact_id character varying(1024) NOT NULL,
    job_id character varying(2048) NOT NULL,
    dependency_sha256 character varying(64) NOT NULL,
    requested_by character varying(512) NOT NULL,
    requested_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_catalog_export_dependency_sha CHECK (((dependency_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_catalog_export_kind CHECK (((object_kind)::text = ANY ((ARRAY['FEATURE_VIEW'::character varying, 'TRAINING_DATASET'::character varying])::text[]))),
    CONSTRAINT chk_catalog_export_version CHECK ((object_version > 0))
);

-- Name: evaluation_dataset_versions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE evaluation_dataset_versions (
    tenant_id character varying(255) NOT NULL,
    dataset_id character varying(255) NOT NULL,
    version bigint NOT NULL,
    owner_principal character varying(512) NOT NULL,
    source_snapshot character varying(1024) NOT NULL,
    manifest_uri character varying(2048) NOT NULL,
    manifest_sha256 character varying(64) NOT NULL,
    item_count bigint NOT NULL,
    schema_json jsonb NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_evaluation_dataset_items CHECK ((item_count >= 0)),
    CONSTRAINT chk_evaluation_dataset_sha256 CHECK (((manifest_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_evaluation_dataset_version CHECK ((version > 0))
);

-- Name: evaluation_metric_policies; Type: TABLE; Schema: public; Owner: -

CREATE TABLE evaluation_metric_policies (
    tenant_id character varying(255) NOT NULL,
    policy_version character varying(255) NOT NULL,
    created_at timestamp(6) with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    sealed boolean DEFAULT false NOT NULL
);

-- Name: evaluation_metric_policy_rules; Type: TABLE; Schema: public; Owner: -

CREATE TABLE evaluation_metric_policy_rules (
    tenant_id character varying(255) NOT NULL,
    policy_version character varying(255) NOT NULL,
    metric_name character varying(255) NOT NULL,
    metric_unit character varying(64) NOT NULL,
    direction character varying(32) NOT NULL,
    threshold_value double precision NOT NULL,
    maximum_baseline_regression double precision NOT NULL,
    CONSTRAINT chk_evaluation_metric_policy_direction CHECK (((direction)::text = ANY ((ARRAY['HIGHER_IS_BETTER'::character varying, 'LOWER_IS_BETTER'::character varying])::text[]))),
    CONSTRAINT chk_evaluation_metric_policy_finite CHECK (((threshold_value <> ALL (ARRAY['NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision])) AND (maximum_baseline_regression >= (0)::double precision) AND (maximum_baseline_regression <> ALL (ARRAY['NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision]))))
);

-- Name: evaluation_metrics; Type: TABLE; Schema: public; Owner: -

CREATE TABLE evaluation_metrics (
    tenant_id character varying(255) NOT NULL,
    run_id character varying(255) NOT NULL,
    metric_name character varying(255) NOT NULL,
    metric_value double precision NOT NULL,
    metric_unit character varying(64) NOT NULL,
    passed boolean NOT NULL,
    evidence_ref character varying(2048),
    CONSTRAINT chk_evaluation_metric_finite CHECK ((metric_value <> ALL (ARRAY['NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision])))
);

-- Name: evaluation_release_decisions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE evaluation_release_decisions (
    tenant_id character varying(255) NOT NULL,
    run_id character varying(255) NOT NULL,
    approved boolean NOT NULL,
    policy_version character varying(255) NOT NULL,
    reason character varying(2048) NOT NULL,
    decided_by character varying(512) NOT NULL,
    decided_at timestamp(6) with time zone NOT NULL
);

-- Name: evaluation_runs; Type: TABLE; Schema: public; Owner: -

CREATE TABLE evaluation_runs (
    tenant_id character varying(255) NOT NULL,
    run_id character varying(255) NOT NULL,
    dataset_id character varying(255) NOT NULL,
    dataset_version bigint NOT NULL,
    subject_type character varying(32) NOT NULL,
    subject_id character varying(1024) NOT NULL,
    subject_version character varying(255) NOT NULL,
    baseline_run_id character varying(255),
    policy_version character varying(255) NOT NULL,
    state character varying(16) NOT NULL,
    started_at timestamp(6) with time zone NOT NULL,
    completed_at timestamp(6) with time zone,
    trace_reference character varying(2048),
    failure_class character varying(255),
    metric_policy_pinned boolean DEFAULT false NOT NULL,
    terminal_txid bigint,
    CONSTRAINT chk_evaluation_run_state CHECK (((state)::text = ANY ((ARRAY['RUNNING'::character varying, 'SUCCEEDED'::character varying, 'FAILED'::character varying, 'CANCELLED'::character varying])::text[]))),
    CONSTRAINT chk_evaluation_run_terminal CHECK (((((state)::text = 'RUNNING'::text) AND (completed_at IS NULL) AND (failure_class IS NULL)) OR (((state)::text = 'SUCCEEDED'::text) AND (completed_at IS NOT NULL) AND (failure_class IS NULL)) OR (((state)::text = 'FAILED'::text) AND (completed_at IS NOT NULL) AND (failure_class IS NOT NULL)) OR (((state)::text = 'CANCELLED'::text) AND (completed_at IS NOT NULL)))),
    CONSTRAINT chk_evaluation_run_time_order CHECK (((completed_at IS NULL) OR (completed_at >= started_at))),
    CONSTRAINT chk_evaluation_subject_type CHECK (((subject_type)::text = ANY ((ARRAY['MODEL'::character varying, 'EMBEDDING_MODEL'::character varying, 'SEARCH_SERVICE'::character varying, 'SEMANTIC_VIEW'::character varying, 'FEATURE_VIEW'::character varying, 'INDEX'::character varying, 'CAPABILITY'::character varying])::text[])))
);

-- Name: feature_view_active_versions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE feature_view_active_versions (
    tenant_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    feature_view_name character varying(255) NOT NULL,
    active_version bigint NOT NULL,
    publication_epoch bigint NOT NULL,
    published_by character varying(512) NOT NULL,
    published_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_feature_view_publication_epoch CHECK ((publication_epoch > 0))
);

-- Name: feature_view_versions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE feature_view_versions (
    tenant_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    feature_view_name character varying(255) NOT NULL,
    version bigint NOT NULL,
    owner_principal character varying(512) NOT NULL,
    definition_json jsonb NOT NULL,
    definition_sha256 character varying(64) NOT NULL,
    source_plan bytea NOT NULL,
    source_plan_sha256 character varying(64) NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_feature_view_definition_sha CHECK (((definition_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_feature_view_plan_nonempty CHECK ((octet_length(source_plan) > 0)),
    CONSTRAINT chk_feature_view_plan_sha CHECK (((source_plan_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_feature_view_version CHECK ((version > 0))
);

-- Name: pangrova_embedding_models; Type: TABLE; Schema: public; Owner: -

CREATE TABLE pangrova_embedding_models (
    model_name character varying(256) NOT NULL,
    provider character varying(128) NOT NULL,
    endpoint character varying(512) NOT NULL,
    dimension integer NOT NULL,
    api_key_secret character varying(512) NOT NULL,
    max_batch_size integer NOT NULL,
    max_rps integer NOT NULL,
    query_prefix text NOT NULL,
    document_prefix text NOT NULL,
    version integer DEFAULT 0 NOT NULL,
    has_dependents boolean DEFAULT false NOT NULL,
    deployment_version character varying(256) DEFAULT ''::character varying NOT NULL,
    deployment_signature_sha256 character varying(64) DEFAULT ''::character varying NOT NULL,
    incarnation_id character varying(36) DEFAULT '00000000-0000-0000-0000-000000000000'::character varying NOT NULL,
    admin_role character varying(256),
    usage_role character varying(256)
);

-- Name: tabular_export_generations; Type: TABLE; Schema: public; Owner: -

CREATE TABLE tabular_export_generations (
    tenant_id character varying(255) NOT NULL,
    artifact_id character varying(1024) NOT NULL,
    generation character varying(128) NOT NULL,
    job_id character varying(2048) NOT NULL,
    source_id character varying(2048) NOT NULL,
    source_snapshot character varying(1024) NOT NULL,
    format_id character varying(255) NOT NULL,
    format_major integer NOT NULL,
    format_minor integer NOT NULL,
    schema_sha256 character varying(64) NOT NULL,
    writer_build_id character varying(255) NOT NULL,
    manifest_uri character varying(2048) NOT NULL,
    manifest_sha256 character varying(64) NOT NULL,
    object_count bigint NOT NULL,
    row_count bigint NOT NULL,
    total_bytes bigint NOT NULL,
    publication_epoch bigint NOT NULL,
    state character varying(16) NOT NULL,
    published_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_tabular_export_counts CHECK (((object_count >= 0) AND (row_count >= 0) AND (total_bytes >= 0))),
    CONSTRAINT chk_tabular_export_epoch CHECK ((publication_epoch > 0)),
    CONSTRAINT chk_tabular_export_manifest_sha CHECK (((manifest_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_tabular_export_schema_sha CHECK (((schema_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_tabular_export_state CHECK (((state)::text = ANY ((ARRAY['ACTIVE'::character varying, 'RETIRED'::character varying])::text[]))),
    CONSTRAINT chk_tabular_export_versions CHECK (((format_major > 0) AND (format_minor >= 0)))
);

-- Name: training_dataset_versions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE training_dataset_versions (
    tenant_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    dataset_name character varying(255) NOT NULL,
    version bigint NOT NULL,
    owner_principal character varying(512) NOT NULL,
    definition_json jsonb NOT NULL,
    definition_sha256 character varying(64) NOT NULL,
    normalized_plan bytea NOT NULL,
    normalized_plan_sha256 character varying(64) NOT NULL,
    split_kind character varying(16) NOT NULL,
    split_seed bigint NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_training_dataset_definition_sha CHECK (((definition_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_training_dataset_plan_nonempty CHECK ((octet_length(normalized_plan) > 0)),
    CONSTRAINT chk_training_dataset_plan_sha CHECK (((normalized_plan_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_training_dataset_split_kind CHECK (((split_kind)::text = ANY ((ARRAY['RANDOM'::character varying, 'TEMPORAL'::character varying, 'ENTITY'::character varying])::text[]))),
    CONSTRAINT chk_training_dataset_version CHECK ((version > 0))
);

-- Name: catalog_export_requests catalog_export_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY catalog_export_requests
    ADD CONSTRAINT catalog_export_requests_pkey PRIMARY KEY (tenant_id, object_kind, catalog_name, schema_name, object_name, object_version, generation);

-- Name: evaluation_dataset_versions evaluation_dataset_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_dataset_versions
    ADD CONSTRAINT evaluation_dataset_versions_pkey PRIMARY KEY (tenant_id, dataset_id, version);

-- Name: evaluation_metric_policies evaluation_metric_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_metric_policies
    ADD CONSTRAINT evaluation_metric_policies_pkey PRIMARY KEY (tenant_id, policy_version);

-- Name: evaluation_metric_policy_rules evaluation_metric_policy_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_metric_policy_rules
    ADD CONSTRAINT evaluation_metric_policy_rules_pkey PRIMARY KEY (tenant_id, policy_version, metric_name);

-- Name: evaluation_metrics evaluation_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_metrics
    ADD CONSTRAINT evaluation_metrics_pkey PRIMARY KEY (tenant_id, run_id, metric_name);

-- Name: evaluation_release_decisions evaluation_release_decisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_release_decisions
    ADD CONSTRAINT evaluation_release_decisions_pkey PRIMARY KEY (tenant_id, run_id);

-- Name: evaluation_runs evaluation_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_runs
    ADD CONSTRAINT evaluation_runs_pkey PRIMARY KEY (tenant_id, run_id);

-- Name: feature_view_active_versions feature_view_active_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY feature_view_active_versions
    ADD CONSTRAINT feature_view_active_versions_pkey PRIMARY KEY (tenant_id, catalog_name, schema_name, feature_view_name);

-- Name: feature_view_versions feature_view_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY feature_view_versions
    ADD CONSTRAINT feature_view_versions_pkey PRIMARY KEY (tenant_id, catalog_name, schema_name, feature_view_name, version);

-- Name: pangrova_embedding_models pangrova_embedding_models_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY pangrova_embedding_models
    ADD CONSTRAINT pangrova_embedding_models_pkey PRIMARY KEY (model_name);

-- Name: tabular_export_generations tabular_export_generations_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY tabular_export_generations
    ADD CONSTRAINT tabular_export_generations_pkey PRIMARY KEY (tenant_id, artifact_id, generation);

-- Name: training_dataset_versions training_dataset_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY training_dataset_versions
    ADD CONSTRAINT training_dataset_versions_pkey PRIMARY KEY (tenant_id, catalog_name, schema_name, dataset_name, version);

-- Name: catalog_export_requests uq_catalog_export_job; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY catalog_export_requests
    ADD CONSTRAINT uq_catalog_export_job UNIQUE (tenant_id, job_id);

-- Name: evaluation_dataset_versions uq_evaluation_dataset_manifest; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_dataset_versions
    ADD CONSTRAINT uq_evaluation_dataset_manifest UNIQUE (tenant_id, dataset_id, manifest_sha256);

-- Name: feature_view_versions uq_feature_view_definition; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY feature_view_versions
    ADD CONSTRAINT uq_feature_view_definition UNIQUE (tenant_id, catalog_name, schema_name, feature_view_name, definition_sha256);

-- Name: tabular_export_generations uq_tabular_export_job; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY tabular_export_generations
    ADD CONSTRAINT uq_tabular_export_job UNIQUE (tenant_id, job_id);

-- Name: training_dataset_versions uq_training_dataset_definition; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY training_dataset_versions
    ADD CONSTRAINT uq_training_dataset_definition UNIQUE (tenant_id, catalog_name, schema_name, dataset_name, definition_sha256);

-- Name: idx_catalog_export_object; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_catalog_export_object ON catalog_export_requests USING btree (tenant_id, object_kind, catalog_name, schema_name, object_name, object_version, requested_at DESC);

-- Name: idx_emb_models_dependents; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_emb_models_dependents ON pangrova_embedding_models USING btree (model_name, has_dependents);

-- Name: idx_evaluation_dataset_versions; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_evaluation_dataset_versions ON evaluation_dataset_versions USING btree (tenant_id, dataset_id, version DESC);

-- Name: idx_evaluation_runs_subject; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_evaluation_runs_subject ON evaluation_runs USING btree (tenant_id, subject_type, subject_id, subject_version, started_at DESC);

-- Name: uq_tabular_export_active; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_tabular_export_active ON tabular_export_generations USING btree (tenant_id, artifact_id) WHERE ((state)::text = 'ACTIVE'::text);

-- Name: evaluation_dataset_versions evaluation_dataset_versions_no_mutation; Type: TRIGGER; Schema: public; Owner: -

CREATE OR REPLACE FUNCTION evaluation_evidence_immutable_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'published evaluation evidence is immutable';
END;
$function$;
CREATE OR REPLACE FUNCTION evaluation_metric_policy_header_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
$function$;
CREATE OR REPLACE FUNCTION evaluation_metric_policy_rule_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
$function$;
CREATE OR REPLACE FUNCTION evaluation_metric_write_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
$function$;
CREATE OR REPLACE FUNCTION evaluation_release_decision_time_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
$function$;
CREATE OR REPLACE FUNCTION evaluation_run_metric_policy_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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
$function$;
CREATE TRIGGER evaluation_dataset_versions_no_mutation BEFORE DELETE OR UPDATE ON evaluation_dataset_versions FOR EACH ROW EXECUTE FUNCTION evaluation_evidence_immutable_guard();

-- Name: evaluation_dataset_versions evaluation_dataset_versions_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_dataset_versions_no_truncate BEFORE TRUNCATE ON evaluation_dataset_versions FOR EACH STATEMENT EXECUTE FUNCTION evaluation_evidence_immutable_guard();

-- Name: evaluation_metric_policies evaluation_metric_policies_no_mutation; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_metric_policies_no_mutation BEFORE DELETE OR UPDATE ON evaluation_metric_policies FOR EACH ROW EXECUTE FUNCTION evaluation_metric_policy_header_guard();

-- Name: evaluation_metric_policies evaluation_metric_policies_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_metric_policies_no_truncate BEFORE TRUNCATE ON evaluation_metric_policies FOR EACH STATEMENT EXECUTE FUNCTION evaluation_metric_policy_header_guard();

-- Name: evaluation_metric_policy_rules evaluation_metric_policy_rules_no_mutation; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_metric_policy_rules_no_mutation BEFORE INSERT OR DELETE OR UPDATE ON evaluation_metric_policy_rules FOR EACH ROW EXECUTE FUNCTION evaluation_metric_policy_rule_guard();

-- Name: evaluation_metric_policy_rules evaluation_metric_policy_rules_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_metric_policy_rules_no_truncate BEFORE TRUNCATE ON evaluation_metric_policy_rules FOR EACH STATEMENT EXECUTE FUNCTION evaluation_metric_policy_rule_guard();

-- Name: evaluation_metrics evaluation_metrics_completion_only; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_metrics_completion_only BEFORE INSERT OR DELETE OR UPDATE ON evaluation_metrics FOR EACH ROW EXECUTE FUNCTION evaluation_metric_write_guard();

-- Name: evaluation_metrics evaluation_metrics_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_metrics_no_truncate BEFORE TRUNCATE ON evaluation_metrics FOR EACH STATEMENT EXECUTE FUNCTION evaluation_metric_write_guard();

-- Name: evaluation_release_decisions evaluation_release_decisions_no_mutation; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_release_decisions_no_mutation BEFORE DELETE OR UPDATE ON evaluation_release_decisions FOR EACH ROW EXECUTE FUNCTION evaluation_evidence_immutable_guard();

-- Name: evaluation_release_decisions evaluation_release_decisions_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_release_decisions_no_truncate BEFORE TRUNCATE ON evaluation_release_decisions FOR EACH STATEMENT EXECUTE FUNCTION evaluation_evidence_immutable_guard();

-- Name: evaluation_release_decisions evaluation_release_decisions_time_order; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_release_decisions_time_order BEFORE INSERT ON evaluation_release_decisions FOR EACH ROW EXECUTE FUNCTION evaluation_release_decision_time_guard();

-- Name: evaluation_runs evaluation_runs_immutable_identity_and_terminal; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_runs_immutable_identity_and_terminal BEFORE UPDATE ON evaluation_runs FOR EACH ROW EXECUTE FUNCTION evaluation_run_metric_policy_guard();

-- Name: evaluation_runs evaluation_runs_no_delete; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_runs_no_delete BEFORE DELETE ON evaluation_runs FOR EACH ROW EXECUTE FUNCTION evaluation_evidence_immutable_guard();

-- Name: evaluation_runs evaluation_runs_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_runs_no_truncate BEFORE TRUNCATE ON evaluation_runs FOR EACH STATEMENT EXECUTE FUNCTION evaluation_evidence_immutable_guard();

-- Name: evaluation_runs evaluation_runs_pin_metric_policy; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER evaluation_runs_pin_metric_policy BEFORE INSERT ON evaluation_runs FOR EACH ROW EXECUTE FUNCTION evaluation_run_metric_policy_guard();

-- Name: evaluation_metric_policy_rules fk_evaluation_metric_policy; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_metric_policy_rules
    ADD CONSTRAINT fk_evaluation_metric_policy FOREIGN KEY (tenant_id, policy_version) REFERENCES evaluation_metric_policies(tenant_id, policy_version);

-- Name: evaluation_metrics fk_evaluation_metric_run; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_metrics
    ADD CONSTRAINT fk_evaluation_metric_run FOREIGN KEY (tenant_id, run_id) REFERENCES evaluation_runs(tenant_id, run_id) ON DELETE CASCADE;

-- Name: evaluation_release_decisions fk_evaluation_release_run; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_release_decisions
    ADD CONSTRAINT fk_evaluation_release_run FOREIGN KEY (tenant_id, run_id) REFERENCES evaluation_runs(tenant_id, run_id) ON DELETE CASCADE;

-- Name: evaluation_runs fk_evaluation_run_baseline; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_runs
    ADD CONSTRAINT fk_evaluation_run_baseline FOREIGN KEY (tenant_id, baseline_run_id) REFERENCES evaluation_runs(tenant_id, run_id);

-- Name: evaluation_runs fk_evaluation_run_dataset; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY evaluation_runs
    ADD CONSTRAINT fk_evaluation_run_dataset FOREIGN KEY (tenant_id, dataset_id, dataset_version) REFERENCES evaluation_dataset_versions(tenant_id, dataset_id, version);

-- Name: feature_view_active_versions fk_feature_view_active_version; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY feature_view_active_versions
    ADD CONSTRAINT fk_feature_view_active_version FOREIGN KEY (tenant_id, catalog_name, schema_name, feature_view_name, active_version) REFERENCES feature_view_versions(tenant_id, catalog_name, schema_name, feature_view_name, version);

