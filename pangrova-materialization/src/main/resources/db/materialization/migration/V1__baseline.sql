-- materialization baseline: single migration owning the materialization durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Owns the materialization schema (definitions, generations, dependencies, events, ...).
-- Depends on the query baseline (execution_anchor, write_commit_journal trigger) and the
-- coordinator baseline (distributed_lease read by validate_attempt()).

-- PostgreSQL database dump

-- Name: materialization; Type: SCHEMA; Schema: -; Owner: -

CREATE SCHEMA IF NOT EXISTS materialization;

-- Name: capture_native_commit(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: is_publication_reference(text); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: reject_append_only_mutation(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: reject_definition_mutation(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: source_vector_hash(jsonb); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_attempt(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_command_mutation(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_commit_evidence(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_dependency(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_event_mutation(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_generation(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_materialization_drop(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_object_header(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_reader_pin(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_schedule(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_source_pin(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: validate_write_capability(); Type: FUNCTION; Schema: materialization; Owner: -


-- Name: attempts; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.attempts (
    attempt_id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    object_name character varying(512) NOT NULL,
    incarnation_id uuid NOT NULL,
    definition_revision bigint NOT NULL,
    base_object_revision bigint NOT NULL,
    base_generation_id uuid,
    refresh_kind character varying(64) NOT NULL,
    source_vector jsonb NOT NULL,
    source_vector_hash character(64) NOT NULL,
    trigger_kind character varying(32) NOT NULL,
    requesting_principal character varying(512) NOT NULL,
    definer_principal character varying(512) NOT NULL,
    execution_anchor character varying(128) NOT NULL,
    control_epoch bigint NOT NULL,
    lease_expires_at timestamp(6) with time zone NOT NULL,
    candidate_catalog character varying(255) NOT NULL,
    candidate_schema character varying(255) NOT NULL,
    candidate_table character varying(255) NOT NULL,
    state character varying(32) NOT NULL,
    error_code character varying(128),
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT attempts_base_object_revision_check CHECK ((base_object_revision >= 0)),
    CONSTRAINT attempts_candidate_schema_check CHECK (((candidate_schema)::text = '__pangrova_materializations'::text)),
    CONSTRAINT attempts_candidate_table_check CHECK (((candidate_table)::text ~ '^__pg_mat_[0-9a-f]{32}$'::text)),
    CONSTRAINT attempts_check CHECK ((updated_at >= created_at)),
    CONSTRAINT attempts_check1 CHECK ((((refresh_kind)::text = 'FULL'::text) OR (base_generation_id IS NOT NULL))),
    CONSTRAINT attempts_check2 CHECK ((((refresh_kind)::text <> 'FULL'::text) OR (base_generation_id IS NULL))),
    CONSTRAINT attempts_check3 CHECK ((((state)::text <> 'FAILED'::text) OR (error_code IS NOT NULL))),
    CONSTRAINT attempts_control_epoch_check CHECK ((control_epoch > 0)),
    CONSTRAINT attempts_definer_principal_check CHECK ((btrim((definer_principal)::text) <> ''::text)),
    CONSTRAINT attempts_definition_revision_check CHECK ((definition_revision >= 1)),
    CONSTRAINT attempts_refresh_kind_check CHECK (((refresh_kind)::text = ANY ((ARRAY['FULL'::character varying, 'INCREMENTAL_APPEND_ONLY'::character varying, 'INCREMENTAL_PARTITION_REPLACE'::character varying, 'INCREMENTAL_AFFECTED_KEY'::character varying, 'INCREMENTAL_CDC'::character varying])::text[]))),
    CONSTRAINT attempts_requesting_principal_check CHECK ((btrim((requesting_principal)::text) <> ''::text)),
    CONSTRAINT attempts_source_vector_hash_check CHECK ((source_vector_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT attempts_state_check CHECK (((state)::text = ANY ((ARRAY['PREPARING'::character varying, 'BUILDING'::character varying, 'COMMITTED'::character varying, 'PUBLISHED'::character varying, 'FAILED'::character varying, 'ABANDONED'::character varying, 'QUARANTINED'::character varying, 'SUPERSEDED'::character varying])::text[]))),
    CONSTRAINT attempts_trigger_kind_check CHECK (((trigger_kind)::text = ANY ((ARRAY['MANUAL'::character varying, 'SCHEDULED'::character varying, 'CREATION'::character varying, 'REPAIR'::character varying])::text[])))
);

-- Name: commands; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.commands (
    tenant_id character varying(255) NOT NULL,
    command_id character varying(128) NOT NULL,
    result_reference text NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT commands_result_reference_check CHECK ((btrim(result_reference) <> ''::text))
);

-- Name: commit_evidence; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.commit_evidence (
    attempt_id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    connector_id character varying(64) NOT NULL,
    commit_fingerprint character varying(255) NOT NULL,
    manifest_hash character varying(255) NOT NULL,
    format_version character varying(64) NOT NULL,
    committed_at timestamp(6) with time zone NOT NULL,
    recorded_at timestamp(6) with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT commit_evidence_commit_fingerprint_check CHECK ((btrim((commit_fingerprint)::text) <> ''::text)),
    CONSTRAINT commit_evidence_connector_id_check CHECK ((btrim((connector_id)::text) <> ''::text)),
    CONSTRAINT commit_evidence_format_version_check CHECK ((btrim((format_version)::text) <> ''::text)),
    CONSTRAINT commit_evidence_manifest_hash_check CHECK ((btrim((manifest_hash)::text) <> ''::text))
);

-- Name: definition_revisions; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.definition_revisions (
    tenant_id character varying(255) NOT NULL,
    object_name character varying(512) NOT NULL,
    revision bigint NOT NULL,
    definition_sql text NOT NULL,
    canonical_sql text NOT NULL,
    plan_fingerprint character(64) NOT NULL,
    resolved boolean NOT NULL,
    output_schema jsonb,
    output_schema_hash character(64),
    sources jsonb NOT NULL,
    policy_snapshot jsonb NOT NULL,
    security_mode character varying(32) NOT NULL,
    definer_principal character varying(512) NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    session_properties jsonb DEFAULT '{}'::jsonb NOT NULL,
    definer_groups jsonb DEFAULT '[]'::jsonb NOT NULL,
    CONSTRAINT definition_revisions_canonical_sql_check CHECK ((btrim(canonical_sql) <> ''::text)),
    CONSTRAINT definition_revisions_check CHECK (((resolved AND (output_schema IS NOT NULL) AND (output_schema_hash IS NOT NULL)) OR ((NOT resolved) AND (output_schema IS NULL) AND (output_schema_hash IS NULL)))),
    CONSTRAINT definition_revisions_definer_principal_check CHECK ((btrim((definer_principal)::text) <> ''::text)),
    CONSTRAINT definition_revisions_definition_sql_check CHECK ((btrim(definition_sql) <> ''::text)),
    CONSTRAINT definition_revisions_plan_fingerprint_check CHECK ((plan_fingerprint ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT definition_revisions_policy_snapshot_check CHECK ((jsonb_typeof(policy_snapshot) = 'object'::text)),
    CONSTRAINT definition_revisions_revision_check CHECK ((revision >= 1)),
    CONSTRAINT definition_revisions_security_mode_check CHECK (((security_mode)::text = 'DEFINER'::text)),
    CONSTRAINT definition_revisions_sources_check CHECK ((jsonb_typeof(sources) = 'array'::text))
);

-- Name: dependencies; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.dependencies (
    tenant_id character varying(255) NOT NULL,
    downstream_object character varying(512) NOT NULL,
    upstream_object character varying(512) NOT NULL,
    downstream_definition_revision bigint NOT NULL,
    dependency_kind character varying(32) NOT NULL,
    CONSTRAINT dependencies_check CHECK (((downstream_object)::text <> (upstream_object)::text)),
    CONSTRAINT dependencies_dependency_kind_check CHECK (((dependency_kind)::text = ANY ((ARRAY['DIRECT'::character varying, 'INDIRECT'::character varying])::text[]))),
    CONSTRAINT dependencies_downstream_definition_revision_check CHECK ((downstream_definition_revision >= 1))
);

-- Name: events; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.events (
    event_id uuid NOT NULL,
    event_type character varying(64) NOT NULL,
    tenant_id character varying(255) NOT NULL,
    object_name character varying(512) NOT NULL,
    command_id character varying(128),
    attempt_id uuid,
    generation_id uuid,
    query_id character varying(128),
    requesting_principal character varying(512) NOT NULL,
    effective_principal character varying(512) NOT NULL,
    detail text DEFAULT ''::text NOT NULL,
    occurred_at timestamp(6) with time zone NOT NULL,
    published_at timestamp(6) with time zone
);

-- Name: gc_ledger; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.gc_ledger (
    entry_id uuid NOT NULL,
    generation_id uuid NOT NULL,
    decision character varying(32) NOT NULL,
    evidence text NOT NULL,
    state character varying(32) NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT gc_ledger_check CHECK ((updated_at >= created_at)),
    CONSTRAINT gc_ledger_decision_check CHECK (((decision)::text = ANY ((ARRAY['RETAIN'::character varying, 'COLLECT'::character varying, 'DEFER'::character varying])::text[]))),
    CONSTRAINT gc_ledger_state_check CHECK (((state)::text = ANY ((ARRAY['PENDING'::character varying, 'APPLIED'::character varying, 'FAILED'::character varying])::text[])))
);

-- Name: generations; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.generations (
    generation_id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    object_name character varying(512) NOT NULL,
    incarnation_id uuid NOT NULL,
    definition_revision bigint NOT NULL,
    producing_attempt_id uuid NOT NULL,
    base_generation_id uuid,
    physical_catalog character varying(255) NOT NULL,
    physical_schema character varying(255) NOT NULL,
    physical_table character varying(255) NOT NULL,
    refresh_kind character varying(64) NOT NULL,
    source_vector jsonb NOT NULL,
    source_vector_hash character(64) NOT NULL,
    state character varying(32) NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    published_at timestamp(6) with time zone,
    CONSTRAINT generations_check CHECK ((((refresh_kind)::text = 'FULL'::text) OR (base_generation_id IS NOT NULL))),
    CONSTRAINT generations_check1 CHECK ((((refresh_kind)::text <> 'FULL'::text) OR (base_generation_id IS NULL))),
    CONSTRAINT generations_check2 CHECK (((published_at IS NULL) OR ((state)::text = ANY ((ARRAY['PUBLISHED'::character varying, 'RETIRED'::character varying, 'COLLECTED'::character varying])::text[])))),
    CONSTRAINT generations_definition_revision_check CHECK ((definition_revision >= 1)),
    CONSTRAINT generations_physical_schema_check CHECK (((physical_schema)::text = '__pangrova_materializations'::text)),
    CONSTRAINT generations_physical_table_check CHECK (((physical_table)::text ~ '^__pg_mat_[0-9a-f]{32}$'::text)),
    CONSTRAINT generations_refresh_kind_check CHECK (((refresh_kind)::text = ANY ((ARRAY['FULL'::character varying, 'INCREMENTAL_APPEND_ONLY'::character varying, 'INCREMENTAL_PARTITION_REPLACE'::character varying, 'INCREMENTAL_AFFECTED_KEY'::character varying, 'INCREMENTAL_CDC'::character varying])::text[]))),
    CONSTRAINT generations_source_vector_hash_check CHECK ((source_vector_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT generations_state_check CHECK (((state)::text = ANY ((ARRAY['CANDIDATE'::character varying, 'COMMITTED'::character varying, 'PUBLISHED'::character varying, 'RETIRED'::character varying, 'QUARANTINED'::character varying, 'COLLECTED'::character varying])::text[])))
);

-- Name: native_commit_capture; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.native_commit_capture (
    query_id character varying(255) NOT NULL,
    tenant_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    table_name character varying(1024) NOT NULL,
    commit_fingerprint character varying(64) NOT NULL,
    principal character varying(255),
    owner_id character varying(255),
    owner_incarnation character varying(255),
    execution_fencing_token bigint NOT NULL,
    published_version bigint NOT NULL,
    manifest_sha256 bytea NOT NULL,
    committed_at timestamp(6) with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT native_commit_capture_commit_fingerprint_check CHECK (((commit_fingerprint)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT native_commit_capture_manifest_sha256_check CHECK ((octet_length(manifest_sha256) = 32)),
    CONSTRAINT native_commit_capture_published_version_check CHECK ((published_version > 0))
);

-- Name: objects; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.objects (
    tenant_id character varying(255) NOT NULL,
    object_name character varying(512) NOT NULL,
    object_kind character varying(32) NOT NULL,
    incarnation_id uuid NOT NULL,
    definition_revision bigint NOT NULL,
    revision bigint NOT NULL,
    published_generation_id uuid,
    state character varying(32) NOT NULL,
    definer_principal character varying(512) NOT NULL,
    security_mode character varying(32) NOT NULL,
    refresh_mode character varying(32) NOT NULL,
    incremental_strategy character varying(64),
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT objects_check CHECK ((updated_at >= created_at)),
    CONSTRAINT objects_check1 CHECK ((((state)::text <> 'ACTIVE'::text) OR (published_generation_id IS NOT NULL))),
    CONSTRAINT objects_check2 CHECK ((((refresh_mode)::text <> 'FULL'::text) OR (incremental_strategy IS NULL))),
    CONSTRAINT objects_definer_principal_check CHECK ((btrim((definer_principal)::text) <> ''::text)),
    CONSTRAINT objects_definition_revision_check CHECK ((definition_revision >= 1)),
    CONSTRAINT objects_object_kind_check CHECK (((object_kind)::text = ANY ((ARRAY['DYNAMIC_TABLE'::character varying, 'MATERIALIZED_VIEW'::character varying])::text[]))),
    CONSTRAINT objects_refresh_mode_check CHECK (((refresh_mode)::text = ANY ((ARRAY['FULL'::character varying, 'INCREMENTAL'::character varying])::text[]))),
    CONSTRAINT objects_revision_check CHECK ((revision >= 0)),
    CONSTRAINT objects_security_mode_check CHECK (((security_mode)::text = ANY ((ARRAY['DEFINER'::character varying, 'INVOKER'::character varying])::text[]))),
    CONSTRAINT objects_state_check CHECK (((state)::text = ANY ((ARRAY['CREATING'::character varying, 'ACTIVE'::character varying, 'SUSPENDED'::character varying, 'FAILED'::character varying, 'DROPPING'::character varying, 'DROPPED'::character varying])::text[])))
);

-- Name: reader_pins; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.reader_pins (
    generation_id uuid NOT NULL,
    query_id character varying(128) NOT NULL,
    owner character varying(255) NOT NULL,
    expires_at timestamp(6) with time zone NOT NULL,
    heartbeat_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT reader_pins_check CHECK ((expires_at >= heartbeat_at)),
    CONSTRAINT reader_pins_owner_check CHECK ((btrim((owner)::text) <> ''::text))
);

-- Name: registry_epochs; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.registry_epochs (
    tenant_id character varying(255) NOT NULL,
    epoch bigint DEFAULT 0 NOT NULL,
    CONSTRAINT registry_epochs_epoch_check CHECK ((epoch >= 0))
);

-- Name: schedules; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.schedules (
    tenant_id character varying(255) NOT NULL,
    object_name character varying(512) NOT NULL,
    target_lag_ms bigint,
    next_due_at timestamp(6) with time zone,
    last_success_at timestamp(6) with time zone,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    state character varying(32) NOT NULL,
    lease_owner character varying(255),
    lease_expires_at timestamp(6) with time zone,
    version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT schedules_check CHECK (((lease_owner IS NULL) = (lease_expires_at IS NULL))),
    CONSTRAINT schedules_check1 CHECK ((((state)::text <> 'ACTIVE'::text) OR ((target_lag_ms IS NOT NULL) AND (next_due_at IS NOT NULL)))),
    CONSTRAINT schedules_check2 CHECK ((((state)::text <> 'DISABLED'::text) OR (target_lag_ms IS NULL))),
    CONSTRAINT schedules_check3 CHECK ((((state)::text <> 'SUSPENDED'::text) OR (target_lag_ms IS NOT NULL))),
    CONSTRAINT schedules_consecutive_failures_check CHECK ((consecutive_failures >= 0)),
    CONSTRAINT schedules_state_check CHECK (((state)::text = ANY ((ARRAY['ACTIVE'::character varying, 'SUSPENDED'::character varying, 'DISABLED'::character varying])::text[]))),
    CONSTRAINT schedules_target_lag_ms_check CHECK (((target_lag_ms IS NULL) OR (target_lag_ms > 0))),
    CONSTRAINT schedules_version_check CHECK ((version >= 0))
);

-- Name: schema_version; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.schema_version (
    version integer NOT NULL,
    applied_at timestamp(6) with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT schema_version_version_check CHECK ((version > 0))
);

-- Name: source_pins; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.source_pins (
    attempt_id uuid NOT NULL,
    connector_id character varying(64) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    snapshot_token character varying(512) NOT NULL,
    schema_fingerprint character varying(255) NOT NULL,
    policy_fingerprint character varying(255) NOT NULL,
    observed_at timestamp(6) with time zone NOT NULL,
    expires_at timestamp(6) with time zone NOT NULL,
    state character varying(32) NOT NULL,
    CONSTRAINT source_pins_state_check CHECK (((state)::text = ANY ((ARRAY['ACQUIRED'::character varying, 'RELEASED'::character varying, 'EXPIRED'::character varying])::text[])))
);

-- Name: write_capabilities; Type: TABLE; Schema: materialization; Owner: -

CREATE TABLE IF NOT EXISTS materialization.write_capabilities (
    attempt_id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    object_name character varying(512) NOT NULL,
    incarnation_id uuid NOT NULL,
    definition_revision bigint NOT NULL,
    generation_id uuid NOT NULL,
    candidate_catalog character varying(255) NOT NULL,
    candidate_schema character varying(255) NOT NULL,
    candidate_table character varying(255) NOT NULL,
    source_vector_hash character(64) NOT NULL,
    definer_principal character varying(512) NOT NULL,
    control_epoch bigint NOT NULL,
    issued_at timestamp(6) with time zone NOT NULL,
    expires_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT write_capabilities_check CHECK ((expires_at > issued_at)),
    CONSTRAINT write_capabilities_control_epoch_check CHECK ((control_epoch > 0)),
    CONSTRAINT write_capabilities_definer_principal_check CHECK ((btrim((definer_principal)::text) <> ''::text)),
    CONSTRAINT write_capabilities_definition_revision_check CHECK ((definition_revision >= 1)),
    CONSTRAINT write_capabilities_source_vector_hash_check CHECK ((source_vector_hash ~ '^[0-9a-f]{64}$'::text))
);

-- Name: attempts attempts_candidate_catalog_candidate_schema_candidate_table_key; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.attempts
        ADD CONSTRAINT attempts_candidate_catalog_candidate_schema_candidate_table_key UNIQUE (candidate_catalog, candidate_schema, candidate_table);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: attempts attempts_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.attempts
        ADD CONSTRAINT attempts_pkey PRIMARY KEY (attempt_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: commands commands_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.commands
        ADD CONSTRAINT commands_pkey PRIMARY KEY (tenant_id, command_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: commit_evidence commit_evidence_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.commit_evidence
        ADD CONSTRAINT commit_evidence_pkey PRIMARY KEY (attempt_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: definition_revisions definition_revisions_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.definition_revisions
        ADD CONSTRAINT definition_revisions_pkey PRIMARY KEY (tenant_id, object_name, revision);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: dependencies dependencies_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.dependencies
        ADD CONSTRAINT dependencies_pkey PRIMARY KEY (tenant_id, downstream_object, downstream_definition_revision, upstream_object);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: events events_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.events
        ADD CONSTRAINT events_pkey PRIMARY KEY (event_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: gc_ledger gc_ledger_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.gc_ledger
        ADD CONSTRAINT gc_ledger_pkey PRIMARY KEY (entry_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: generations generations_physical_catalog_physical_schema_physical_table_key; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.generations
        ADD CONSTRAINT generations_physical_catalog_physical_schema_physical_table_key UNIQUE (physical_catalog, physical_schema, physical_table);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: generations generations_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.generations
        ADD CONSTRAINT generations_pkey PRIMARY KEY (generation_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: generations generations_producing_attempt_id_key; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.generations
        ADD CONSTRAINT generations_producing_attempt_id_key UNIQUE (producing_attempt_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: native_commit_capture native_commit_capture_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.native_commit_capture
        ADD CONSTRAINT native_commit_capture_pkey PRIMARY KEY (query_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: objects objects_incarnation_id_key; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.objects
        ADD CONSTRAINT objects_incarnation_id_key UNIQUE (incarnation_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: objects objects_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.objects
        ADD CONSTRAINT objects_pkey PRIMARY KEY (tenant_id, object_name);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: reader_pins reader_pins_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.reader_pins
        ADD CONSTRAINT reader_pins_pkey PRIMARY KEY (generation_id, query_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: registry_epochs registry_epochs_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.registry_epochs
        ADD CONSTRAINT registry_epochs_pkey PRIMARY KEY (tenant_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: schedules schedules_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.schedules
        ADD CONSTRAINT schedules_pkey PRIMARY KEY (tenant_id, object_name);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: schema_version schema_version_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.schema_version
        ADD CONSTRAINT schema_version_pkey PRIMARY KEY (version);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: source_pins source_pins_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.source_pins
        ADD CONSTRAINT source_pins_pkey PRIMARY KEY (attempt_id, connector_id, catalog_name, schema_name, table_name);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: write_capabilities write_capabilities_pkey; Type: CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.write_capabilities
        ADD CONSTRAINT write_capabilities_pkey PRIMARY KEY (attempt_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: materialization_attempts_active; Type: INDEX; Schema: materialization; Owner: -

CREATE UNIQUE INDEX IF NOT EXISTS materialization_attempts_active ON materialization.attempts USING btree (tenant_id, object_name) WHERE ((state)::text = ANY ((ARRAY['PREPARING'::character varying, 'BUILDING'::character varying, 'COMMITTED'::character varying, 'QUARANTINED'::character varying])::text[]));

-- Name: materialization_attempts_unsettled; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_attempts_unsettled ON materialization.attempts USING btree (state, updated_at) WHERE ((state)::text = ANY ((ARRAY['PREPARING'::character varying, 'BUILDING'::character varying, 'COMMITTED'::character varying, 'QUARANTINED'::character varying])::text[]));

-- Name: materialization_definitions_sources; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_definitions_sources ON materialization.definition_revisions USING gin (sources);

-- Name: materialization_dependencies_upstream; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_dependencies_upstream ON materialization.dependencies USING btree (tenant_id, upstream_object);

-- Name: materialization_events_object; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_events_object ON materialization.events USING btree (tenant_id, object_name, occurred_at);

-- Name: materialization_events_outbox; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_events_outbox ON materialization.events USING btree (occurred_at) WHERE (published_at IS NULL);

-- Name: materialization_generations_object; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_generations_object ON materialization.generations USING btree (tenant_id, object_name, created_at);

-- Name: materialization_generations_published; Type: INDEX; Schema: materialization; Owner: -

CREATE UNIQUE INDEX IF NOT EXISTS materialization_generations_published ON materialization.generations USING btree (tenant_id, object_name, incarnation_id) WHERE ((state)::text = 'PUBLISHED'::text);

-- Name: materialization_generations_retirable; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_generations_retirable ON materialization.generations USING btree (state, published_at) WHERE ((state)::text = 'RETIRED'::text);

-- Name: materialization_objects_kind_state; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_objects_kind_state ON materialization.objects USING btree (tenant_id, object_kind, state);

-- Name: materialization_reader_pins_expiry; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_reader_pins_expiry ON materialization.reader_pins USING btree (expires_at);

-- Name: materialization_reader_pins_query; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_reader_pins_query ON materialization.reader_pins USING btree (query_id);

-- Name: materialization_schedules_due; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_schedules_due ON materialization.schedules USING btree (next_due_at) WHERE ((state)::text = 'ACTIVE'::text);

-- Name: materialization_source_pins_expiry; Type: INDEX; Schema: materialization; Owner: -

CREATE INDEX IF NOT EXISTS materialization_source_pins_expiry ON materialization.source_pins USING btree (state, expires_at);

-- Name: attempts materialization_attempt_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE FUNCTION materialization.capture_native_commit() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM materialization.attempts a
            WHERE a.execution_anchor = NEW.query_id) THEN
        RETURN NEW;
    END IF;
    IF NEW.published_version IS NULL OR NEW.manifest_sha256 IS NULL
            OR NEW.tenant_id IS NULL OR NEW.principal IS NULL THEN
        RAISE EXCEPTION 'materialization commit requires authenticated publication evidence';
    END IF;
    INSERT INTO materialization.native_commit_capture (query_id, tenant_id, catalog_name,
        table_name, commit_fingerprint, principal, owner_id, owner_incarnation,
        execution_fencing_token, published_version, manifest_sha256, committed_at)
    VALUES (NEW.query_id, NEW.tenant_id, NEW.catalog_name, NEW.table_name,
        NEW.commit_fingerprint, NEW.principal, NEW.owner_id, NEW.owner_incarnation,
        NEW.execution_fencing_token, NEW.published_version, NEW.manifest_sha256,
        clock_timestamp())
    ON CONFLICT (query_id) DO NOTHING;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.is_publication_reference(reference text) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    IF reference IS NULL OR btrim(reference) NOT LIKE '{%' THEN
        RETURN FALSE;
    END IF;
    RETURN (reference::jsonb) ? 'generation';
EXCEPTION WHEN others THEN
    RETURN FALSE;
END $$;
CREATE OR REPLACE FUNCTION materialization.reject_append_only_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'materialization row is append-only';
END $$;
CREATE OR REPLACE FUNCTION materialization.reject_definition_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE'
            AND current_setting('pangrova.materialization_reclaim', true) = 'on' THEN
        RETURN OLD;
    END IF;
    IF TG_OP = 'UPDATE'
            AND current_setting('pangrova.materialization_rename', true) = 'on'
            AND NEW.tenant_id = OLD.tenant_id
            AND NEW.revision = OLD.revision
            AND NEW.definition_sql = OLD.definition_sql
            AND NEW.canonical_sql = OLD.canonical_sql
            AND NEW.plan_fingerprint = OLD.plan_fingerprint
            AND NEW.resolved = OLD.resolved
            AND NEW.output_schema IS NOT DISTINCT FROM OLD.output_schema
            AND NEW.output_schema_hash IS NOT DISTINCT FROM OLD.output_schema_hash
            AND NEW.sources IS NOT DISTINCT FROM OLD.sources
            AND NEW.policy_snapshot IS NOT DISTINCT FROM OLD.policy_snapshot
            AND NEW.security_mode = OLD.security_mode
            AND NEW.definer_principal = OLD.definer_principal
            AND NEW.session_properties IS NOT DISTINCT FROM OLD.session_properties
            AND NEW.created_at = OLD.created_at THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'materialization definitions are immutable';
END $$;
CREATE OR REPLACE FUNCTION materialization.source_vector_hash(vector jsonb) RETURNS character
    LANGUAGE plpgsql IMMUTABLE
    AS $_$
DECLARE
    canonical TEXT;
    entry JSONB;
    connector TEXT;
    source_name TEXT;
    entry_count INTEGER;
    distinct_count INTEGER;
BEGIN
    IF vector IS NULL OR jsonb_typeof(vector) <> 'object'
            OR jsonb_typeof(vector->'entries') <> 'array'
            OR (vector->>'canonicalVersion') IS NULL
            OR (vector->>'canonicalVersion') !~ '^[0-9]+$' THEN
        RAISE EXCEPTION 'source vector must carry canonicalVersion and an entries array';
    END IF;
    FOR entry IN SELECT value FROM jsonb_array_elements(vector->'entries') LOOP
        connector := entry->'source'->>'connectorId';
        source_name := connector || ':' || (entry->'source'->>'catalog') || '.'
            || (entry->'source'->>'schema') || '.' || (entry->'source'->>'table');
        IF connector IS NULL OR connector = '' OR (entry->'source'->>'catalog') IS NULL
                OR (entry->'source'->>'schema') IS NULL OR (entry->'source'->>'table') IS NULL
                OR (entry->'snapshot'->>'snapshotToken') IS NULL
                OR (entry->'snapshot'->>'schemaFingerprint') IS NULL
                OR (entry->'snapshot'->>'policyFingerprint') IS NULL THEN
            RAISE EXCEPTION 'source vector entry is incomplete';
        END IF;
        IF (entry->'snapshot'->>'connectorId') IS DISTINCT FROM connector
                OR COALESCE((entry->'snapshot'->>'pinnable')::BOOLEAN, FALSE) IS NOT TRUE THEN
            RAISE EXCEPTION 'source vector snapshot must match its connector and be pinnable';
        END IF;
    END LOOP;
    SELECT count(*), count(DISTINCT src.name) INTO entry_count, distinct_count
    FROM (
        SELECT (e->'source'->>'connectorId') || ':' || (e->'source'->>'catalog') || '.'
            || (e->'source'->>'schema') || '.' || (e->'source'->>'table') AS name
        FROM jsonb_array_elements(vector->'entries') e
    ) src;
    IF entry_count <> distinct_count THEN
        RAISE EXCEPTION 'source vector entries must be unique';
    END IF;
    canonical := 'vector:' || (vector->>'canonicalVersion');
    IF entry_count > 0 THEN
        SELECT canonical || '|' || string_agg(src.fragment, '|'
                ORDER BY src.name COLLATE "C")
        INTO canonical
        FROM (
            SELECT (e->'source'->>'connectorId') || ':' || (e->'source'->>'catalog') || '.'
                       || (e->'source'->>'schema') || '.' || (e->'source'->>'table') AS name,
                   (e->'source'->>'connectorId') || ':' || (e->'source'->>'catalog') || '.'
                       || (e->'source'->>'schema') || '.' || (e->'source'->>'table') || '@'
                       || (e->'snapshot'->>'snapshotToken') || ':'
                       || (e->'snapshot'->>'schemaFingerprint') || ':'
                       || (e->'snapshot'->>'policyFingerprint') AS fragment
            FROM jsonb_array_elements(vector->'entries') e
        ) src;
    END IF;
    RETURN encode(sha256(convert_to(canonical, 'UTF8')), 'hex');
END $_$;
CREATE OR REPLACE FUNCTION materialization.validate_attempt() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    header materialization.objects%ROWTYPE;
    anchor execution_anchor%ROWTYPE;
    leader distributed_lease%ROWTYPE;
    base materialization.generations%ROWTYPE;
    legal BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'PREPARING' THEN
            RAISE EXCEPTION 'a materialization attempt must begin in PREPARING';
        END IF;
        SELECT * INTO header FROM materialization.objects o
            WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.object_name FOR SHARE;
        IF NOT FOUND OR header.incarnation_id <> NEW.incarnation_id
                OR header.definition_revision <> NEW.definition_revision
                OR header.revision <> NEW.base_object_revision
                OR header.state NOT IN ('CREATING', 'ACTIVE', 'SUSPENDED', 'FAILED') THEN
            RAISE EXCEPTION 'attempt does not match the current materialization head';
        END IF;
        IF NEW.refresh_kind = 'FULL' AND NEW.base_generation_id IS NOT NULL THEN
            RAISE EXCEPTION 'full refresh must not name a base generation';
        END IF;
        IF NEW.refresh_kind <> 'FULL' THEN
            SELECT * INTO base FROM materialization.generations g
                WHERE g.generation_id = NEW.base_generation_id;
            IF NOT FOUND OR (base.tenant_id, base.object_name, base.incarnation_id)
                    IS DISTINCT FROM (NEW.tenant_id, NEW.object_name, NEW.incarnation_id)
                    OR base.state <> 'PUBLISHED'
                    OR header.published_generation_id IS DISTINCT FROM base.generation_id THEN
                RAISE EXCEPTION 'incremental attempt requires the exact published base generation';
            END IF;
        END IF;
        SELECT * INTO anchor FROM execution_anchor a
            WHERE a.execution_id = NEW.execution_anchor FOR SHARE;
        IF NOT FOUND OR anchor.fencing_token <= 0
                OR anchor.expires_at < NEW.lease_expires_at
                OR anchor.expires_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'attempt requires a live execution anchor covering its lease';
        END IF;
        SELECT * INTO leader FROM distributed_lease
            WHERE lease_key = 'coordinator_leader' FOR SHARE;
        IF NOT FOUND OR leader.epoch IS DISTINCT FROM NEW.control_epoch
                OR leader.expires_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'attempt requires the current live coordinator control epoch';
        END IF;
        IF materialization.source_vector_hash(NEW.source_vector)
                IS DISTINCT FROM NEW.source_vector_hash THEN
            RAISE EXCEPTION 'attempt source vector hash does not match its canonical content';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization attempts are immutable history';
    END IF;
    IF (NEW.attempt_id, NEW.tenant_id, NEW.incarnation_id,
            NEW.definition_revision, NEW.base_object_revision, NEW.base_generation_id,
            NEW.refresh_kind, NEW.source_vector, NEW.source_vector_hash, NEW.trigger_kind,
            NEW.requesting_principal, NEW.definer_principal, NEW.execution_anchor,
            NEW.control_epoch, NEW.lease_expires_at, NEW.candidate_catalog,
            NEW.candidate_schema, NEW.candidate_table, NEW.created_at)
            IS DISTINCT FROM
       (OLD.attempt_id, OLD.tenant_id, OLD.incarnation_id,
            OLD.definition_revision, OLD.base_object_revision, OLD.base_generation_id,
            OLD.refresh_kind, OLD.source_vector, OLD.source_vector_hash, OLD.trigger_kind,
            OLD.requesting_principal, OLD.definer_principal, OLD.execution_anchor,
            OLD.control_epoch, OLD.lease_expires_at, OLD.candidate_catalog,
            OLD.candidate_schema, OLD.candidate_table, OLD.created_at)
            OR (NEW.object_name IS DISTINCT FROM OLD.object_name
                AND current_setting('pangrova.materialization_rename', true)
                    IS DISTINCT FROM 'on') THEN
        RAISE EXCEPTION 'materialization attempt identity and candidate are immutable';
    END IF;
    IF NEW.state <> OLD.state THEN
        legal := CASE OLD.state
            WHEN 'PREPARING' THEN NEW.state IN ('BUILDING', 'FAILED', 'ABANDONED')
            WHEN 'BUILDING' THEN NEW.state IN ('COMMITTED', 'FAILED', 'ABANDONED', 'QUARANTINED')
            WHEN 'COMMITTED' THEN NEW.state IN ('PUBLISHED', 'QUARANTINED', 'SUPERSEDED', 'FAILED')
            WHEN 'QUARANTINED' THEN NEW.state IN ('COMMITTED', 'ABANDONED')
            ELSE FALSE END;
        IF NOT legal THEN
            RAISE EXCEPTION 'illegal materialization attempt transition % -> %', OLD.state, NEW.state;
        END IF;
    END IF;
    IF NEW.state IN ('COMMITTED', 'PUBLISHED')
            AND NOT EXISTS (SELECT 1 FROM materialization.commit_evidence e
                WHERE e.attempt_id = NEW.attempt_id) THEN
        RAISE EXCEPTION 'committed or published attempt requires exact commit evidence';
    END IF;
    IF NEW.state = 'PUBLISHED' AND OLD.state IS DISTINCT FROM 'PUBLISHED' THEN
        IF NOT EXISTS (SELECT 1 FROM materialization.generations g
                JOIN materialization.objects o
                    ON o.published_generation_id = g.generation_id
                WHERE g.producing_attempt_id = NEW.attempt_id
                    AND g.state = 'PUBLISHED'
                    AND o.tenant_id = NEW.tenant_id) THEN
            RAISE EXCEPTION 'published attempt must own the published generation of its head';
        END IF;
    END IF;
    IF NEW.state = 'FAILED' AND NEW.error_code IS NULL THEN
        RAISE EXCEPTION 'failed attempt requires an error code';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'attempt update instant must not move backwards';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_command_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization commands are append-only';
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
            OR NEW.command_id IS DISTINCT FROM OLD.command_id
            OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'materialization command identity is immutable';
    END IF;
    IF materialization.is_publication_reference(OLD.result_reference) THEN
        RAISE EXCEPTION 'a publication receipt is terminal';
    END IF;
    IF NOT materialization.is_publication_reference(NEW.result_reference) THEN
        RAISE EXCEPTION 'only a publication receipt may replace an admission receipt';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_commit_evidence() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    attempt materialization.attempts%ROWTYPE;
    captured materialization.native_commit_capture%ROWTYPE;
    anchor execution_anchor%ROWTYPE;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'commit evidence is immutable';
    END IF;
    SELECT * INTO attempt FROM materialization.attempts a
        WHERE a.attempt_id = NEW.attempt_id FOR SHARE;
    IF NOT FOUND OR attempt.tenant_id <> NEW.tenant_id
            OR attempt.state NOT IN ('PREPARING', 'BUILDING', 'COMMITTED', 'QUARANTINED') THEN
        RAISE EXCEPTION 'commit evidence must belong to an unsettled attempt';
    END IF;
    SELECT * INTO anchor FROM execution_anchor a
        WHERE a.execution_id = attempt.execution_anchor FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'commit evidence requires the attempt execution anchor';
    END IF;
    SELECT * INTO captured FROM materialization.native_commit_capture c
        WHERE c.query_id = attempt.execution_anchor FOR SHARE;
    IF NOT FOUND
            OR captured.tenant_id <> attempt.tenant_id
            OR captured.principal IS DISTINCT FROM attempt.requesting_principal
            OR captured.owner_id IS DISTINCT FROM anchor.owner_id
            OR captured.owner_incarnation IS DISTINCT FROM anchor.owner_incarnation
            OR captured.execution_fencing_token IS DISTINCT FROM anchor.fencing_token
            OR captured.commit_fingerprint IS DISTINCT FROM NEW.commit_fingerprint
            OR captured.catalog_name || '.' || captured.table_name
                <> attempt.candidate_catalog || '.' || attempt.candidate_schema || '.' || attempt.candidate_table
            OR encode(captured.manifest_sha256, 'hex') <> NEW.manifest_hash THEN
        RAISE EXCEPTION 'commit evidence must match the exact committed Native write intent';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_dependency() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF current_setting('pangrova.materialization_rename', true) = 'on'
                AND NEW.tenant_id = OLD.tenant_id
                AND NEW.downstream_definition_revision = OLD.downstream_definition_revision
                AND NEW.dependency_kind = OLD.dependency_kind
                AND (NEW.downstream_object IS NOT DISTINCT FROM OLD.downstream_object
                    OR NEW.upstream_object IS NOT DISTINCT FROM OLD.upstream_object) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'materialization dependencies are rebuilt, never updated';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM materialization.definition_revisions d
            WHERE d.tenant_id = NEW.tenant_id AND d.object_name = NEW.downstream_object
                AND d.revision = NEW.downstream_definition_revision) THEN
        RAISE EXCEPTION 'dependency must name an existing downstream definition revision';
    END IF;
    PERFORM 1 FROM materialization.objects o
        WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.upstream_object
            AND o.state NOT IN ('DROPPING', 'DROPPED')
        FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'dependency must name a live upstream materialization';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_event_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization events are append-only';
    END IF;
    IF (NEW.event_id, NEW.event_type, NEW.tenant_id, NEW.object_name, NEW.command_id,
            NEW.attempt_id, NEW.generation_id, NEW.query_id, NEW.requesting_principal,
            NEW.effective_principal, NEW.detail, NEW.occurred_at) IS DISTINCT FROM
       (OLD.event_id, OLD.event_type, OLD.tenant_id, OLD.object_name, OLD.command_id,
            OLD.attempt_id, OLD.generation_id, OLD.query_id, OLD.requesting_principal,
            OLD.effective_principal, OLD.detail, OLD.occurred_at)
            OR OLD.published_at IS NOT NULL OR NEW.published_at IS NULL THEN
        RAISE EXCEPTION 'materialization events can only be marked delivered';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_generation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    attempt materialization.attempts%ROWTYPE;
    base materialization.generations%ROWTYPE;
    header materialization.objects%ROWTYPE;
    legal BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'CANDIDATE' THEN
            RAISE EXCEPTION 'a generation must begin as a candidate';
        END IF;
        SELECT * INTO attempt FROM materialization.attempts a
            WHERE a.attempt_id = NEW.producing_attempt_id FOR SHARE;
        IF NOT FOUND OR (attempt.tenant_id, attempt.object_name, attempt.incarnation_id,
                attempt.definition_revision, attempt.candidate_catalog, attempt.candidate_schema,
                attempt.candidate_table)
                IS DISTINCT FROM
               (NEW.tenant_id, NEW.object_name, NEW.incarnation_id, NEW.definition_revision,
                NEW.physical_catalog, NEW.physical_schema, NEW.physical_table)
                OR attempt.state NOT IN ('PREPARING', 'BUILDING') THEN
            RAISE EXCEPTION 'generation must be the candidate of its exact reserved attempt';
        END IF;
        IF NEW.refresh_kind = 'FULL' AND NEW.base_generation_id IS NOT NULL THEN
            RAISE EXCEPTION 'full generation must not name a base generation';
        END IF;
        IF NEW.refresh_kind <> 'FULL' THEN
            SELECT * INTO base FROM materialization.generations g
                WHERE g.generation_id = NEW.base_generation_id;
            IF NOT FOUND OR (base.tenant_id, base.object_name, base.incarnation_id)
                    IS DISTINCT FROM (NEW.tenant_id, NEW.object_name, NEW.incarnation_id)
                    OR base.state <> 'PUBLISHED' THEN
                RAISE EXCEPTION 'incremental generation requires a published base generation';
            END IF;
        END IF;
        IF materialization.source_vector_hash(NEW.source_vector)
                IS DISTINCT FROM NEW.source_vector_hash THEN
            RAISE EXCEPTION 'generation source vector hash does not match its canonical content';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'materialization generations are retained until collected';
    END IF;
    IF (NEW.generation_id, NEW.tenant_id, NEW.incarnation_id,
            NEW.definition_revision, NEW.producing_attempt_id, NEW.base_generation_id,
            NEW.physical_catalog, NEW.physical_schema, NEW.physical_table,
            NEW.refresh_kind, NEW.source_vector, NEW.source_vector_hash, NEW.created_at)
            IS DISTINCT FROM
       (OLD.generation_id, OLD.tenant_id, OLD.incarnation_id,
            OLD.definition_revision, OLD.producing_attempt_id, OLD.base_generation_id,
            OLD.physical_catalog, OLD.physical_schema, OLD.physical_table,
            OLD.refresh_kind, OLD.source_vector, OLD.source_vector_hash, OLD.created_at)
            OR (NEW.object_name IS DISTINCT FROM OLD.object_name
                AND current_setting('pangrova.materialization_rename', true)
                    IS DISTINCT FROM 'on') THEN
        RAISE EXCEPTION 'materialization generation identity is immutable';
    END IF;
    IF NEW.state <> OLD.state THEN
        legal := CASE OLD.state
            WHEN 'CANDIDATE' THEN NEW.state IN ('COMMITTED', 'QUARANTINED', 'COLLECTED')
            WHEN 'COMMITTED' THEN NEW.state IN ('PUBLISHED', 'QUARANTINED', 'COLLECTED')
            WHEN 'PUBLISHED' THEN NEW.state = 'RETIRED'
            WHEN 'RETIRED' THEN NEW.state = 'COLLECTED'
            WHEN 'QUARANTINED' THEN NEW.state IN ('COMMITTED', 'COLLECTED')
            ELSE FALSE END;
        IF NOT legal THEN
            RAISE EXCEPTION 'illegal materialization generation transition % -> %', OLD.state, NEW.state;
        END IF;
    END IF;
    IF NEW.state = 'COLLECTED' AND OLD.state IN ('CANDIDATE', 'COMMITTED')
            AND NOT EXISTS (SELECT 1 FROM materialization.attempts a
                WHERE a.attempt_id = NEW.producing_attempt_id
                AND a.state IN ('FAILED', 'ABANDONED', 'SUPERSEDED')) THEN
        RAISE EXCEPTION 'unpublished generation can only be collected after its attempt is terminal';
    END IF;
    IF NEW.state IN ('COMMITTED', 'PUBLISHED')
            AND NOT EXISTS (SELECT 1 FROM materialization.commit_evidence e
                WHERE e.attempt_id = NEW.producing_attempt_id) THEN
        RAISE EXCEPTION 'committed or published generation requires exact commit evidence';
    END IF;
    IF NEW.state = 'PUBLISHED' THEN
        IF NEW.published_at IS NULL THEN
            RAISE EXCEPTION 'published generation requires a publication instant';
        END IF;
        SELECT * INTO header FROM materialization.objects o
            WHERE o.tenant_id = NEW.tenant_id AND o.object_name = NEW.object_name FOR SHARE;
        IF NOT FOUND OR header.published_generation_id IS DISTINCT FROM NEW.generation_id
                OR header.state <> 'ACTIVE' THEN
            RAISE EXCEPTION 'generation can only be published through its exact object head';
        END IF;
    END IF;
    IF NEW.state = 'COLLECTED' AND EXISTS (
            SELECT 1 FROM materialization.reader_pins p
            WHERE p.generation_id = NEW.generation_id
              AND p.expires_at > clock_timestamp()) THEN
        RAISE EXCEPTION 'generation with live reader pins cannot be collected';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_materialization_drop() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.state IN ('DROPPING', 'DROPPED') AND NEW.state IS DISTINCT FROM OLD.state THEN
        IF EXISTS (
            SELECT 1 FROM materialization.dependencies d
            JOIN materialization.objects o
                ON o.tenant_id = d.tenant_id AND o.object_name = d.downstream_object
            WHERE d.tenant_id = NEW.tenant_id AND d.upstream_object = NEW.object_name
                AND d.downstream_definition_revision = o.definition_revision
                AND o.state NOT IN ('DROPPING', 'DROPPED')
        ) THEN
            RAISE EXCEPTION 'materialization % still has live dependents', NEW.object_name;
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_object_header() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    generation materialization.generations%ROWTYPE;
    legal BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.revision <> 0 OR NEW.definition_revision <> 1
                OR NEW.published_generation_id IS NOT NULL
                OR NEW.state NOT IN ('CREATING', 'SUSPENDED')
                OR NEW.security_mode <> 'DEFINER' THEN
            RAISE EXCEPTION 'a materialization object must be created without a published generation';
        END IF;
        INSERT INTO materialization.registry_epochs (tenant_id, epoch) VALUES (NEW.tenant_id, 1)
            ON CONFLICT (tenant_id) DO UPDATE SET epoch = materialization.registry_epochs.epoch + 1;
        RETURN NEW;
    END IF;
    IF current_setting('pangrova.materialization_reclaim', true) = 'on'
            AND OLD.state = 'DROPPED'
            AND NEW.state = 'CREATING'
            AND NEW.tenant_id = OLD.tenant_id
            AND NEW.object_name = OLD.object_name
            AND NEW.revision = 0
            AND NEW.definition_revision = 1
            AND NEW.published_generation_id IS NULL
            AND NEW.incarnation_id IS DISTINCT FROM OLD.incarnation_id THEN
        INSERT INTO materialization.registry_epochs (tenant_id, epoch) VALUES (NEW.tenant_id, 1)
            ON CONFLICT (tenant_id) DO UPDATE SET epoch = materialization.registry_epochs.epoch + 1;
        RETURN NEW;
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
            OR NEW.object_kind IS DISTINCT FROM OLD.object_kind
            OR NEW.incarnation_id IS DISTINCT FROM OLD.incarnation_id
            OR NEW.created_at IS DISTINCT FROM OLD.created_at
            OR (NEW.object_name IS DISTINCT FROM OLD.object_name
                AND current_setting('pangrova.materialization_rename', true)
                    IS DISTINCT FROM 'on') THEN
        RAISE EXCEPTION 'materialization object identity is immutable';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION 'materialization object revision must advance exactly one';
    END IF;
    IF NEW.definition_revision NOT IN (OLD.definition_revision, OLD.definition_revision + 1) THEN
        RAISE EXCEPTION 'materialization definition revision must stay or advance exactly one';
    END IF;
    IF NEW.definition_revision = OLD.definition_revision + 1 AND NOT EXISTS (
            SELECT 1 FROM materialization.definition_revisions d
            WHERE d.tenant_id = NEW.tenant_id AND d.object_name = NEW.object_name
                AND d.revision = NEW.definition_revision) THEN
        RAISE EXCEPTION 'materialization definition revision % is not recorded', NEW.definition_revision;
    END IF;
    IF NEW.state <> OLD.state THEN
        legal := CASE OLD.state
            WHEN 'CREATING' THEN NEW.state IN ('ACTIVE', 'FAILED', 'DROPPING')
            WHEN 'ACTIVE' THEN NEW.state IN ('SUSPENDED', 'FAILED', 'DROPPING')
            WHEN 'SUSPENDED' THEN NEW.state IN ('ACTIVE', 'DROPPING')
            WHEN 'FAILED' THEN NEW.state IN ('ACTIVE', 'DROPPING')
            WHEN 'DROPPING' THEN NEW.state = 'DROPPED'
            ELSE FALSE END;
        IF NOT legal THEN
            RAISE EXCEPTION 'illegal materialization object transition % -> %', OLD.state, NEW.state;
        END IF;
    END IF;
    IF NEW.published_generation_id IS DISTINCT FROM OLD.published_generation_id THEN
        IF NEW.published_generation_id IS NULL THEN
            IF NEW.state NOT IN ('DROPPING', 'DROPPED') THEN
                RAISE EXCEPTION 'published generation can only be cleared while dropping';
            END IF;
        ELSE
            SELECT * INTO generation FROM materialization.generations g
                WHERE g.generation_id = NEW.published_generation_id;
            IF NOT FOUND OR (generation.tenant_id, generation.object_name, generation.incarnation_id)
                    IS DISTINCT FROM (NEW.tenant_id, NEW.object_name, NEW.incarnation_id)
                    OR generation.definition_revision <> NEW.definition_revision
                    OR generation.state NOT IN ('COMMITTED', 'PUBLISHED')
                    OR NOT EXISTS (SELECT 1 FROM materialization.commit_evidence e
                        WHERE e.attempt_id = generation.producing_attempt_id) THEN
                RAISE EXCEPTION 'published generation must be the exact committed generation of this head';
            END IF;
            IF NEW.state <> 'ACTIVE' THEN
                RAISE EXCEPTION 'publication must leave the object ACTIVE';
            END IF;
        END IF;
    END IF;
    IF NEW.state = 'ACTIVE' AND NEW.published_generation_id IS NULL THEN
        RAISE EXCEPTION 'an active materialization requires a published generation';
    END IF;
    IF NEW.refresh_mode = 'FULL' AND NEW.incremental_strategy IS NOT NULL THEN
        RAISE EXCEPTION 'full refresh mode cannot carry an incremental strategy';
    END IF;
    INSERT INTO materialization.registry_epochs (tenant_id, epoch) VALUES (NEW.tenant_id, 1)
        ON CONFLICT (tenant_id) DO UPDATE SET epoch = materialization.registry_epochs.epoch + 1;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_reader_pin() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (NEW.generation_id, NEW.query_id, NEW.owner) IS DISTINCT FROM
           (OLD.generation_id, OLD.query_id, OLD.owner) THEN
            RAISE EXCEPTION 'reader pin identity is immutable';
        END IF;
        IF NEW.heartbeat_at < OLD.heartbeat_at OR NEW.expires_at < OLD.expires_at THEN
            RAISE EXCEPTION 'reader pin renewal must not move backwards';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_schedule() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.version <> 0 THEN
            RAISE EXCEPTION 'schedule must be created at version zero';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        IF current_setting('pangrova.materialization_reclaim', true) = 'on' THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'schedules are disabled and retained with their object';
    END IF;
    IF current_setting('pangrova.materialization_rename', true) = 'on'
            AND NEW.tenant_id = OLD.tenant_id
            AND NEW.target_lag_ms IS NOT DISTINCT FROM OLD.target_lag_ms
            AND NEW.next_due_at IS NOT DISTINCT FROM OLD.next_due_at
            AND NEW.last_success_at IS NOT DISTINCT FROM OLD.last_success_at
            AND NEW.consecutive_failures = OLD.consecutive_failures
            AND NEW.state = OLD.state
            AND NEW.lease_owner IS NOT DISTINCT FROM OLD.lease_owner
            AND NEW.lease_expires_at IS NOT DISTINCT FROM OLD.lease_expires_at
            AND NEW.version = OLD.version THEN
        RETURN NEW;
    END IF;
    IF (NEW.tenant_id, NEW.object_name) IS DISTINCT FROM (OLD.tenant_id, OLD.object_name) THEN
        RAISE EXCEPTION 'schedule identity is immutable';
    END IF;
    IF NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'schedule version must advance exactly one';
    END IF;
    IF NEW.state = 'ACTIVE' AND (NEW.target_lag_ms IS NULL OR NEW.next_due_at IS NULL) THEN
        RAISE EXCEPTION 'active schedule requires a target lag and next due instant';
    END IF;
    IF NEW.state = 'DISABLED' AND NEW.target_lag_ms IS NOT NULL THEN
        RAISE EXCEPTION 'disabled schedule cannot carry a target lag';
    END IF;
    IF NEW.state = 'SUSPENDED' AND NEW.target_lag_ms IS NULL THEN
        RAISE EXCEPTION 'suspended schedule must retain its target lag';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_source_pin() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'ACQUIRED' THEN
            RAISE EXCEPTION 'source pins must be acquired before release';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM materialization.attempts a
                WHERE a.attempt_id = NEW.attempt_id AND a.state IN ('PREPARING', 'BUILDING')) THEN
            RAISE EXCEPTION 'source pin must belong to an active attempt';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'source pins are retained as attempt history';
    END IF;
    IF (NEW.attempt_id, NEW.connector_id, NEW.catalog_name, NEW.schema_name, NEW.table_name,
            NEW.snapshot_token, NEW.schema_fingerprint, NEW.policy_fingerprint,
            NEW.observed_at, NEW.expires_at) IS DISTINCT FROM
       (OLD.attempt_id, OLD.connector_id, OLD.catalog_name, OLD.schema_name, OLD.table_name,
            OLD.snapshot_token, OLD.schema_fingerprint, OLD.policy_fingerprint,
            OLD.observed_at, OLD.expires_at) THEN
        RAISE EXCEPTION 'source pin identity is immutable';
    END IF;
    IF OLD.state <> 'ACQUIRED' OR NEW.state NOT IN ('RELEASED', 'EXPIRED') THEN
        RAISE EXCEPTION 'source pin can only move from ACQUIRED to RELEASED or EXPIRED';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION materialization.validate_write_capability() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'materialization write capabilities are immutable';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM materialization.attempts a
            JOIN materialization.generations g ON g.generation_id = NEW.generation_id
            WHERE a.attempt_id = NEW.attempt_id
                AND a.state IN ('PREPARING', 'BUILDING', 'COMMITTED', 'QUARANTINED')
                AND (a.tenant_id, a.object_name, a.incarnation_id, a.definition_revision,
                    a.candidate_catalog, a.candidate_schema, a.candidate_table,
                    a.source_vector_hash)
                    = (NEW.tenant_id, NEW.object_name, NEW.incarnation_id,
                    NEW.definition_revision, NEW.candidate_catalog, NEW.candidate_schema,
                    NEW.candidate_table, NEW.source_vector_hash)
                AND g.producing_attempt_id = NEW.attempt_id
                AND (g.physical_catalog, g.physical_schema, g.physical_table)
                    = (NEW.candidate_catalog, NEW.candidate_schema, NEW.candidate_table)) THEN
        RAISE EXCEPTION 'write capability must describe the exact reserved attempt and candidate';
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER materialization_attempt_identity BEFORE INSERT OR DELETE OR UPDATE ON materialization.attempts FOR EACH ROW EXECUTE FUNCTION materialization.validate_attempt();

-- Name: commands materialization_commands_immutable; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_commands_immutable BEFORE DELETE OR UPDATE ON materialization.commands FOR EACH ROW EXECUTE FUNCTION materialization.validate_command_mutation();

-- Name: commit_evidence materialization_commit_evidence_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_commit_evidence_identity BEFORE INSERT OR DELETE OR UPDATE ON materialization.commit_evidence FOR EACH ROW EXECUTE FUNCTION materialization.validate_commit_evidence();

-- Name: definition_revisions materialization_definitions_immutable; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_definitions_immutable BEFORE DELETE OR UPDATE ON materialization.definition_revisions FOR EACH ROW EXECUTE FUNCTION materialization.reject_definition_mutation();

-- Name: dependencies materialization_dependency_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_dependency_identity BEFORE INSERT OR DELETE OR UPDATE ON materialization.dependencies FOR EACH ROW EXECUTE FUNCTION materialization.validate_dependency();

-- Name: events materialization_event_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_event_identity BEFORE DELETE OR UPDATE ON materialization.events FOR EACH ROW EXECUTE FUNCTION materialization.validate_event_mutation();

-- Name: gc_ledger materialization_gc_immutable; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_gc_immutable BEFORE DELETE ON materialization.gc_ledger FOR EACH ROW EXECUTE FUNCTION materialization.reject_append_only_mutation();

-- Name: generations materialization_generation_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_generation_identity BEFORE INSERT OR DELETE OR UPDATE ON materialization.generations FOR EACH ROW EXECUTE FUNCTION materialization.validate_generation();

-- Name: objects materialization_object_drop_guard; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_object_drop_guard BEFORE UPDATE ON materialization.objects FOR EACH ROW EXECUTE FUNCTION materialization.validate_materialization_drop();

-- Name: objects materialization_object_header; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_object_header BEFORE INSERT OR UPDATE ON materialization.objects FOR EACH ROW EXECUTE FUNCTION materialization.validate_object_header();

-- Name: reader_pins materialization_reader_pin_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_reader_pin_identity BEFORE UPDATE ON materialization.reader_pins FOR EACH ROW EXECUTE FUNCTION materialization.validate_reader_pin();

-- Name: schedules materialization_schedule_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_schedule_identity BEFORE INSERT OR DELETE OR UPDATE ON materialization.schedules FOR EACH ROW EXECUTE FUNCTION materialization.validate_schedule();

-- Name: schema_version materialization_schema_version_immutable; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_schema_version_immutable BEFORE DELETE OR UPDATE ON materialization.schema_version FOR EACH ROW EXECUTE FUNCTION materialization.reject_append_only_mutation();

-- Name: source_pins materialization_source_pin_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_source_pin_identity BEFORE INSERT OR DELETE OR UPDATE ON materialization.source_pins FOR EACH ROW EXECUTE FUNCTION materialization.validate_source_pin();

-- Name: write_capabilities materialization_write_capability_identity; Type: TRIGGER; Schema: materialization; Owner: -

CREATE OR REPLACE TRIGGER materialization_write_capability_identity BEFORE INSERT OR DELETE OR UPDATE ON materialization.write_capabilities FOR EACH ROW EXECUTE FUNCTION materialization.validate_write_capability();

-- Name: attempts attempts_base_generation_id_fkey; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.attempts
        ADD CONSTRAINT attempts_base_generation_id_fkey FOREIGN KEY (base_generation_id) REFERENCES materialization.generations(generation_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: attempts attempts_object_fk; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.attempts
        ADD CONSTRAINT attempts_object_fk FOREIGN KEY (tenant_id, object_name) REFERENCES materialization.objects(tenant_id, object_name) ON UPDATE CASCADE;
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: definition_revisions definition_revisions_object_fk; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.definition_revisions
        ADD CONSTRAINT definition_revisions_object_fk FOREIGN KEY (tenant_id, object_name) REFERENCES materialization.objects(tenant_id, object_name) ON UPDATE CASCADE;
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: dependencies dependencies_downstream_object_fk; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.dependencies
        ADD CONSTRAINT dependencies_downstream_object_fk FOREIGN KEY (tenant_id, downstream_object) REFERENCES materialization.objects(tenant_id, object_name) ON UPDATE CASCADE;
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: dependencies dependencies_upstream_object_fk; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.dependencies
        ADD CONSTRAINT dependencies_upstream_object_fk FOREIGN KEY (tenant_id, upstream_object) REFERENCES materialization.objects(tenant_id, object_name) ON UPDATE CASCADE;
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: objects fk_materialization_published_generation; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.objects
        ADD CONSTRAINT fk_materialization_published_generation FOREIGN KEY (published_generation_id) REFERENCES materialization.generations(generation_id) DEFERRABLE INITIALLY DEFERRED;
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: gc_ledger gc_ledger_generation_id_fkey; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.gc_ledger
        ADD CONSTRAINT gc_ledger_generation_id_fkey FOREIGN KEY (generation_id) REFERENCES materialization.generations(generation_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: generations generations_base_generation_id_fkey; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.generations
        ADD CONSTRAINT generations_base_generation_id_fkey FOREIGN KEY (base_generation_id) REFERENCES materialization.generations(generation_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: generations generations_object_fk; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.generations
        ADD CONSTRAINT generations_object_fk FOREIGN KEY (tenant_id, object_name) REFERENCES materialization.objects(tenant_id, object_name) ON UPDATE CASCADE;
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: reader_pins reader_pins_generation_id_fkey; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.reader_pins
        ADD CONSTRAINT reader_pins_generation_id_fkey FOREIGN KEY (generation_id) REFERENCES materialization.generations(generation_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: schedules schedules_object_fk; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.schedules
        ADD CONSTRAINT schedules_object_fk FOREIGN KEY (tenant_id, object_name) REFERENCES materialization.objects(tenant_id, object_name) ON UPDATE CASCADE;
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: source_pins source_pins_attempt_id_fkey; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.source_pins
        ADD CONSTRAINT source_pins_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES materialization.attempts(attempt_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: write_capabilities write_capabilities_attempt_id_fkey; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.write_capabilities
        ADD CONSTRAINT write_capabilities_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES materialization.attempts(attempt_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Name: write_capabilities write_capabilities_generation_id_fkey; Type: FK CONSTRAINT; Schema: materialization; Owner: -

DO $$
BEGIN
    ALTER TABLE ONLY materialization.write_capabilities
        ADD CONSTRAINT write_capabilities_generation_id_fkey FOREIGN KEY (generation_id) REFERENCES materialization.generations(generation_id);
EXCEPTION
    WHEN duplicate_object OR duplicate_table OR invalid_table_definition THEN NULL;
END $$;

-- Cross-owner trigger: materialization observes query.write_commit_journal commits.
-- Requires the query baseline (db/query/migration) to have been applied first.
CREATE OR REPLACE TRIGGER materialization_native_commit_capture AFTER INSERT OR UPDATE OF state, published_version, manifest_sha256 ON write_commit_journal FOR EACH ROW WHEN (((new.state)::text = 'COMMITTED'::text)) EXECUTE FUNCTION materialization.capture_native_commit();
