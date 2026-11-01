-- indexing baseline: single migration owning the indexing durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: index descriptors/generation/commit/claims + artifact build/publication + generated values.

-- PostgreSQL database dump

-- Name: artifact_build_dependencies; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_build_dependencies (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    dependency_ordinal integer NOT NULL,
    dependency_kind character varying(128) NOT NULL,
    dependency_id character varying(2048) NOT NULL,
    dependency_version character varying(1024) NOT NULL,
    CONSTRAINT chk_artifact_build_dependency_ordinal CHECK ((dependency_ordinal >= 0))
);

-- Name: artifact_build_dispatch_specs; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_build_dispatch_specs (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    principal character varying(512) DEFAULT ''::character varying NOT NULL,
    max_memory_bytes bigint NOT NULL,
    max_threads integer NOT NULL,
    max_spill_bytes bigint NOT NULL,
    task_timeout_millis bigint NOT NULL,
    authorization_scope_bytes bytea,
    stage_graph_bytes bytea NOT NULL,
    stage_graph_sha256 character varying(64) NOT NULL,
    CONSTRAINT chk_artifact_dispatch_resources CHECK (((max_memory_bytes > 0) AND (max_threads > 0) AND (max_spill_bytes > 0) AND (task_timeout_millis > 0))),
    CONSTRAINT chk_artifact_stage_graph CHECK ((((octet_length(stage_graph_bytes) >= 1) AND (octet_length(stage_graph_bytes) <= 16777216)) AND ((stage_graph_sha256)::text ~ '^[0-9a-f]{64}$'::text)))
);

-- Name: artifact_build_jobs; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_build_jobs (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    artifact_id character varying(255) NOT NULL,
    artifact_type character varying(128) NOT NULL,
    staging_generation character varying(255) NOT NULL,
    source_id character varying(1024) NOT NULL,
    source_snapshot character varying(512) NOT NULL,
    staging_uri character varying(2048) NOT NULL,
    format_id character varying(255) NOT NULL,
    format_major integer NOT NULL,
    format_minor integer NOT NULL,
    schema_sha256 character varying(64) NOT NULL,
    writer_build_id character varying(255) NOT NULL,
    encryption_key_ref character varying(1024) DEFAULT ''::character varying NOT NULL,
    idempotency_key character varying(512) NOT NULL,
    expected_pages integer NOT NULL,
    completed_pages integer DEFAULT 0 NOT NULL,
    state character varying(32) NOT NULL,
    publication_owner character varying(255),
    publication_owner_epoch bigint,
    publication_fencing_token bigint DEFAULT 0 NOT NULL,
    publication_lease_expires_at timestamp(6) with time zone,
    failure_reason character varying(2048),
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    publication_expected_updated_at timestamp(6) with time zone NOT NULL,
    manifest_sha256 character varying(64),
    published_object_count bigint,
    published_row_count bigint,
    published_total_bytes bigint,
    published_at timestamp(6) with time zone,
    execution_kind character varying(16) DEFAULT 'BUILD'::character varying NOT NULL,
    empty_partition_policy character varying(32) DEFAULT 'FORBID'::character varying NOT NULL,
    dependency_set_sha256 character varying(64) NOT NULL,
    publication_execution_owner_id character varying(128),
    publication_execution_owner_incarnation character varying(64),
    publication_execution_owner_token bigint,
    CONSTRAINT chk_artifact_build_format_version CHECK (((format_major > 0) AND (format_minor >= 0))),
    CONSTRAINT chk_artifact_build_pages CHECK (((expected_pages >= 0) AND (completed_pages >= 0) AND (completed_pages <= expected_pages))),
    CONSTRAINT chk_artifact_build_state CHECK (((state)::text = ANY ((ARRAY['QUEUED'::character varying, 'RUNNING'::character varying, 'READY_TO_PUBLISH'::character varying, 'PUBLISHED'::character varying, 'FAILED'::character varying, 'CANCELLED'::character varying])::text[]))),
    CONSTRAINT chk_artifact_dependency_set_sha256 CHECK (((dependency_set_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_artifact_empty_partition_policy CHECK (((empty_partition_policy)::text = ANY ((ARRAY['FORBID'::character varying, 'ALLOW_ZERO_ROWS'::character varying])::text[]))),
    CONSTRAINT chk_artifact_execution_kind CHECK (((execution_kind)::text = ANY ((ARRAY['BUILD'::character varying, 'EXPORT'::character varying])::text[]))),
    CONSTRAINT chk_artifact_publication_execution_owner CHECK (((publication_owner IS NULL) OR ((publication_execution_owner_id IS NOT NULL) AND (publication_execution_owner_incarnation IS NOT NULL) AND (publication_execution_owner_token > 0))))
);

-- Name: artifact_build_outputs; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_build_outputs (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    page_id character varying(255) NOT NULL,
    object_id character varying(255) NOT NULL,
    opaque_uri character varying(2048) NOT NULL,
    object_role character varying(128) NOT NULL,
    source_object_id character varying(2048) NOT NULL,
    object_ordinal integer NOT NULL,
    object_scope character varying(32) NOT NULL,
    size_bytes bigint NOT NULL,
    row_count bigint NOT NULL,
    checksum_sha256 character varying(64) NOT NULL,
    encryption_key_ref character varying(1024) DEFAULT ''::character varying NOT NULL,
    CONSTRAINT chk_artifact_build_output_scope CHECK (((object_scope)::text = ANY ((ARRAY['SOURCE_OBJECT'::character varying, 'WORK_UNIT'::character varying, 'GENERATION'::character varying])::text[]))),
    CONSTRAINT chk_artifact_build_output_size CHECK (((size_bytes > 0) AND (row_count >= 0)))
);

-- Name: artifact_build_page_dependencies; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_build_page_dependencies (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    page_id character varying(255) NOT NULL,
    dependency_ordinal integer NOT NULL,
    dependency_page_id character varying(255) NOT NULL,
    CONSTRAINT chk_artifact_dependency_not_self CHECK (((page_id)::text <> (dependency_page_id)::text))
);

-- Name: artifact_build_page_sources; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_build_page_sources (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    page_id character varying(255) NOT NULL,
    source_ordinal integer NOT NULL,
    source_object_id character varying(2048) NOT NULL,
    split_bytes bytea,
    split_sha256 character varying(64)
);

-- Name: artifact_build_pages; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_build_pages (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    page_id character varying(255) NOT NULL,
    stage_id integer NOT NULL,
    partition_id integer NOT NULL,
    page_ordinal integer NOT NULL,
    page_kind character varying(32) NOT NULL,
    state character varying(32) NOT NULL,
    attempt_id integer DEFAULT '-1'::integer NOT NULL,
    fencing_token bigint DEFAULT 0 NOT NULL,
    lease_owner character varying(255),
    owner_epoch bigint,
    lease_expires_at timestamp(6) with time zone,
    next_available_at timestamp(6) with time zone NOT NULL,
    max_attempts integer NOT NULL,
    report_bytes bytea,
    report_sha256 character varying(64),
    failure_reason character varying(2048),
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    execution_owner_id character varying(128),
    execution_owner_incarnation character varying(64),
    execution_owner_token bigint,
    CONSTRAINT chk_artifact_build_page_attempt CHECK (((attempt_id >= '-1'::integer) AND (max_attempts > 0))),
    CONSTRAINT chk_artifact_build_page_kind CHECK (((page_kind)::text = ANY ((ARRAY['SOURCE_MAP'::character varying, 'DEPENDENCY_REDUCE'::character varying, 'GENERATION_FINALIZE'::character varying, 'RELATIONAL_COMPUTE'::character varying, 'RELATIONAL_PARTITION'::character varying])::text[]))),
    CONSTRAINT chk_artifact_build_page_state CHECK (((state)::text = ANY ((ARRAY['PENDING'::character varying, 'LEASED'::character varying, 'SUCCEEDED'::character varying, 'FAILED'::character varying, 'CANCELLED'::character varying])::text[]))),
    CONSTRAINT chk_artifact_leased_execution_owner CHECK ((((state)::text <> ALL ((ARRAY['LEASED'::character varying, 'SUCCEEDED'::character varying])::text[])) OR (execution_owner_token IS NOT NULL))),
    CONSTRAINT chk_artifact_page_execution_owner CHECK ((((execution_owner_id IS NULL) AND (execution_owner_incarnation IS NULL) AND (execution_owner_token IS NULL)) OR ((execution_owner_id IS NOT NULL) AND (execution_owner_incarnation IS NOT NULL) AND (execution_owner_token > 0))))
);

-- Name: artifact_exchange_finalizations; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_exchange_finalizations (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    finalization_bytes bytea NOT NULL,
    finalization_sha256 character(64) NOT NULL,
    CONSTRAINT artifact_exchange_finalizations_finalization_bytes_check CHECK (((octet_length(finalization_bytes) >= 1) AND (octet_length(finalization_bytes) <= 16777216)))
);

-- Name: artifact_exchange_registrations; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_exchange_registrations (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    registration_bytes bytea NOT NULL,
    registration_sha256 character(64) NOT NULL,
    CONSTRAINT artifact_exchange_registrations_registration_bytes_check CHECK (((octet_length(registration_bytes) >= 1) AND (octet_length(registration_bytes) <= 16777216)))
);

-- Name: artifact_execution_routes; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_execution_routes (
    execution_id character varying(128) NOT NULL,
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL
);

-- Name: artifact_stage_exchanges; Type: TABLE; Schema: public; Owner: -

CREATE TABLE artifact_stage_exchanges (
    tenant_id character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    exchange_id character varying(255) NOT NULL,
    producer_stage_id integer NOT NULL,
    consumer_stage_id integer NOT NULL,
    graph_sha256 character(64) NOT NULL,
    CONSTRAINT artifact_stage_exchanges_consumer_stage_id_check CHECK ((consumer_stage_id >= 0)),
    CONSTRAINT artifact_stage_exchanges_producer_stage_id_check CHECK ((producer_stage_id >= 0))
);

-- Name: generated_value_artifacts; Type: TABLE; Schema: public; Owner: -

CREATE TABLE generated_value_artifacts (
    tenant_id character varying(255) NOT NULL,
    artifact_id character varying(255) NOT NULL,
    source_id character varying(1024) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    target_column character varying(255) NOT NULL,
    definition_sha256 character varying(64) NOT NULL,
    target_generation character varying(255) NOT NULL,
    target_source_snapshot character varying(512) NOT NULL,
    active_generation character varying(255),
    active_source_snapshot character varying(512),
    publication_epoch bigint DEFAULT 0 NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL
);

-- Name: generated_value_generation_manifests; Type: TABLE; Schema: public; Owner: -

CREATE TABLE generated_value_generation_manifests (
    tenant_id character varying(255) NOT NULL,
    artifact_id character varying(255) NOT NULL,
    generation character varying(255) NOT NULL,
    source_id character varying(1024) NOT NULL,
    source_snapshot character varying(512) NOT NULL,
    target_column character varying(255) NOT NULL,
    format_id character varying(255) NOT NULL,
    format_major integer NOT NULL,
    format_minor integer NOT NULL,
    schema_sha256 character varying(64) NOT NULL,
    writer_build_id character varying(255) NOT NULL,
    object_count bigint NOT NULL,
    row_count bigint NOT NULL,
    total_bytes bigint NOT NULL,
    manifest_sha256 character varying(64) NOT NULL,
    publication_epoch bigint NOT NULL,
    state character varying(16) NOT NULL,
    published_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_generated_value_generation_state CHECK (((state)::text = ANY ((ARRAY['ACTIVE'::character varying, 'RETIRED'::character varying])::text[])))
);

-- Name: index_build_claims; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_build_claims (
    index_id character varying(255) NOT NULL,
    generation integer NOT NULL,
    owner_id character varying(255) NOT NULL,
    claimed_at timestamp without time zone NOT NULL,
    heartbeat_at timestamp without time zone NOT NULL
);

-- Name: index_cache_events; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_cache_events (
    event_id bigint NOT NULL,
    event_type character varying(50) NOT NULL,
    index_id character varying(255),
    catalog_name character varying(255),
    schema_name character varying(255),
    table_name character varying(255),
    created_at timestamp without time zone NOT NULL
);

-- Name: index_cache_events_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE index_cache_events ALTER COLUMN event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME index_cache_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: index_commit_log; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_commit_log (
    event_id bigint NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    snapshot_id character varying(255),
    added_files jsonb,
    removed_files jsonb,
    event_type character varying(50) DEFAULT 'APPEND'::character varying NOT NULL,
    processed boolean DEFAULT false NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    tenant_id character varying(255) NOT NULL,
    commit_fingerprint character varying(64),
    table_sequence bigint,
    CONSTRAINT chk_icl_commit_fingerprint_sha256 CHECK (((commit_fingerprint IS NULL) OR ((commit_fingerprint)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT chk_index_commit_unprocessed_sequence CHECK ((processed OR (table_sequence IS NOT NULL)))
);

-- Name: index_commit_log_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE index_commit_log ALTER COLUMN event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME index_commit_log_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: index_commit_table_heads; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_commit_table_heads (
    tenant_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    head_event_id bigint,
    head_sequence bigint,
    next_sequence bigint NOT NULL,
    last_polled_at timestamp(6) with time zone DEFAULT '-infinity'::timestamp with time zone NOT NULL,
    claim_epoch bigint DEFAULT 0 NOT NULL,
    CONSTRAINT index_commit_table_heads_check CHECK (((head_event_id IS NULL) = (head_sequence IS NULL))),
    CONSTRAINT index_commit_table_heads_claim_epoch_check CHECK ((claim_epoch >= 0)),
    CONSTRAINT index_commit_table_heads_next_sequence_check CHECK ((next_sequence > 0))
);

-- Name: index_descriptors; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_descriptors (
    index_id character varying(255) NOT NULL,
    index_name character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    column_names jsonb NOT NULL,
    index_type character varying(50) NOT NULL,
    properties jsonb NOT NULL,
    state character varying(20) NOT NULL,
    stale_reason character varying(1024),
    generation integer DEFAULT 0 NOT NULL,
    last_indexed_snapshot_id character varying(255),
    build_owner character varying(255),
    build_heartbeat_at timestamp without time zone,
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    build_snapshot_id character varying(255),
    tenant_id character varying(255) NOT NULL,
    build_principal character varying(255),
    build_authorization_scope bytea,
    CONSTRAINT ck_index_build_authority_pair CHECK (((build_principal IS NULL) = (build_authorization_scope IS NULL)))
);

-- Name: index_eval_breaker_state; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_eval_breaker_state (
    index_id character varying(255) NOT NULL,
    failure_count integer DEFAULT 0 NOT NULL,
    last_failure_at timestamp without time zone,
    last_success_at timestamp without time zone,
    state character varying(20) DEFAULT 'CLOSED'::character varying NOT NULL
);

-- Name: index_file_mappings; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_file_mappings (
    id bigint NOT NULL,
    index_id character varying(255) NOT NULL,
    data_file_path character varying(2048) NOT NULL,
    index_file_path character varying(2048) NOT NULL,
    row_count bigint DEFAULT 0 NOT NULL,
    index_size_bytes bigint DEFAULT 0 NOT NULL,
    checksum character varying(64),
    generation integer DEFAULT 0 NOT NULL,
    snapshot_token character varying(255) DEFAULT ''::character varying NOT NULL,
    sequence_num bigint DEFAULT 0 NOT NULL,
    last_verified_at bigint DEFAULT 0 NOT NULL,
    created_at timestamp without time zone NOT NULL
);

-- Name: index_file_mappings_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE index_file_mappings ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME index_file_mappings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: index_generation_builds; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_generation_builds (
    index_id character varying(255) NOT NULL,
    generation integer NOT NULL,
    base_generation integer NOT NULL,
    source_snapshot character varying(255) DEFAULT ''::character varying NOT NULL,
    state character varying(32) NOT NULL,
    failure_reason character varying(1024),
    started_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL
);

-- Name: index_generation_manifests; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_generation_manifests (
    index_id character varying(255) NOT NULL,
    generation integer NOT NULL,
    source_id character varying(1024) NOT NULL,
    source_snapshot character varying(512) DEFAULT ''::character varying NOT NULL,
    artifact_type character varying(64) NOT NULL,
    format_id character varying(255) NOT NULL,
    format_major integer NOT NULL,
    format_minor integer NOT NULL,
    schema_sha256 character varying(64) NOT NULL,
    writer_build_id character varying(255) NOT NULL,
    object_count bigint NOT NULL,
    row_count bigint NOT NULL,
    total_bytes bigint NOT NULL,
    manifest_sha256 character varying(64) NOT NULL,
    publication_epoch bigint NOT NULL,
    state character varying(32) NOT NULL,
    published_at timestamp(6) with time zone NOT NULL
);

-- Name: index_maintenance_lease; Type: TABLE; Schema: public; Owner: -

CREATE TABLE index_maintenance_lease (
    lease_key character varying(255) NOT NULL,
    holder_id character varying(255) NOT NULL,
    acquired_at timestamp(6) with time zone NOT NULL,
    expires_at timestamp(6) with time zone NOT NULL
);

-- Name: artifact_build_dependencies artifact_build_dependencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_dependencies
    ADD CONSTRAINT artifact_build_dependencies_pkey PRIMARY KEY (tenant_id, job_id, dependency_ordinal);

-- Name: artifact_build_dispatch_specs artifact_build_dispatch_specs_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_dispatch_specs
    ADD CONSTRAINT artifact_build_dispatch_specs_pkey PRIMARY KEY (tenant_id, job_id);

-- Name: artifact_build_jobs artifact_build_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_jobs
    ADD CONSTRAINT artifact_build_jobs_pkey PRIMARY KEY (tenant_id, job_id);

-- Name: artifact_build_outputs artifact_build_outputs_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_outputs
    ADD CONSTRAINT artifact_build_outputs_pkey PRIMARY KEY (tenant_id, job_id, page_id, object_id);

-- Name: artifact_build_page_dependencies artifact_build_page_dependencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_page_dependencies
    ADD CONSTRAINT artifact_build_page_dependencies_pkey PRIMARY KEY (tenant_id, job_id, page_id, dependency_ordinal);

-- Name: artifact_build_page_sources artifact_build_page_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_page_sources
    ADD CONSTRAINT artifact_build_page_sources_pkey PRIMARY KEY (tenant_id, job_id, page_id, source_ordinal);

-- Name: artifact_build_pages artifact_build_pages_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_pages
    ADD CONSTRAINT artifact_build_pages_pkey PRIMARY KEY (tenant_id, job_id, page_id);

-- Name: artifact_exchange_finalizations artifact_exchange_finalizations_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_exchange_finalizations
    ADD CONSTRAINT artifact_exchange_finalizations_pkey PRIMARY KEY (tenant_id, job_id, exchange_id);

-- Name: artifact_exchange_registrations artifact_exchange_registrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_exchange_registrations
    ADD CONSTRAINT artifact_exchange_registrations_pkey PRIMARY KEY (tenant_id, job_id, exchange_id);

-- Name: artifact_execution_routes artifact_execution_routes_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_execution_routes
    ADD CONSTRAINT artifact_execution_routes_pkey PRIMARY KEY (execution_id);

-- Name: artifact_execution_routes artifact_execution_routes_tenant_id_job_id_key; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_execution_routes
    ADD CONSTRAINT artifact_execution_routes_tenant_id_job_id_key UNIQUE (tenant_id, job_id);

-- Name: artifact_stage_exchanges artifact_stage_exchanges_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_stage_exchanges
    ADD CONSTRAINT artifact_stage_exchanges_pkey PRIMARY KEY (tenant_id, job_id, exchange_id);

-- Name: generated_value_artifacts generated_value_artifacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY generated_value_artifacts
    ADD CONSTRAINT generated_value_artifacts_pkey PRIMARY KEY (tenant_id, artifact_id);

-- Name: generated_value_generation_manifests generated_value_generation_manifests_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY generated_value_generation_manifests
    ADD CONSTRAINT generated_value_generation_manifests_pkey PRIMARY KEY (tenant_id, artifact_id, generation);

-- Name: index_build_claims index_build_claims_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_build_claims
    ADD CONSTRAINT index_build_claims_pkey PRIMARY KEY (index_id, generation);

-- Name: index_cache_events index_cache_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_cache_events
    ADD CONSTRAINT index_cache_events_pkey PRIMARY KEY (event_id);

-- Name: index_commit_log index_commit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_commit_log
    ADD CONSTRAINT index_commit_log_pkey PRIMARY KEY (event_id);

-- Name: index_commit_table_heads index_commit_table_heads_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_commit_table_heads
    ADD CONSTRAINT index_commit_table_heads_pkey PRIMARY KEY (tenant_id, catalog_name, schema_name, table_name);

-- Name: index_descriptors index_descriptors_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_descriptors
    ADD CONSTRAINT index_descriptors_pkey PRIMARY KEY (index_id);

-- Name: index_eval_breaker_state index_eval_breaker_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_eval_breaker_state
    ADD CONSTRAINT index_eval_breaker_state_pkey PRIMARY KEY (index_id);

-- Name: index_file_mappings index_file_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_file_mappings
    ADD CONSTRAINT index_file_mappings_pkey PRIMARY KEY (id);

-- Name: index_generation_builds index_generation_builds_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_generation_builds
    ADD CONSTRAINT index_generation_builds_pkey PRIMARY KEY (index_id, generation);

-- Name: index_generation_manifests index_generation_manifests_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_generation_manifests
    ADD CONSTRAINT index_generation_manifests_pkey PRIMARY KEY (index_id, generation);

-- Name: index_maintenance_lease index_maintenance_lease_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_maintenance_lease
    ADD CONSTRAINT index_maintenance_lease_pkey PRIMARY KEY (lease_key);

-- Name: artifact_build_dependencies uq_artifact_build_dependency_identity; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_dependencies
    ADD CONSTRAINT uq_artifact_build_dependency_identity UNIQUE (tenant_id, job_id, dependency_kind, dependency_id);

-- Name: artifact_build_jobs uq_artifact_build_identity; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_jobs
    ADD CONSTRAINT uq_artifact_build_identity UNIQUE (tenant_id, artifact_id, staging_generation, idempotency_key);

-- Name: artifact_build_outputs uq_artifact_build_output_ordinal; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_outputs
    ADD CONSTRAINT uq_artifact_build_output_ordinal UNIQUE (tenant_id, job_id, page_id, object_ordinal);

-- Name: artifact_build_pages uq_artifact_build_page_ordinal; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_pages
    ADD CONSTRAINT uq_artifact_build_page_ordinal UNIQUE (tenant_id, job_id, page_ordinal);

-- Name: artifact_build_pages uq_artifact_build_page_partition; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_pages
    ADD CONSTRAINT uq_artifact_build_page_partition UNIQUE (tenant_id, job_id, stage_id, partition_id);

-- Name: artifact_build_page_sources uq_artifact_build_page_source; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_page_sources
    ADD CONSTRAINT uq_artifact_build_page_source UNIQUE (tenant_id, job_id, page_id, source_object_id);

-- Name: artifact_build_page_dependencies uq_artifact_dependency; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_page_dependencies
    ADD CONSTRAINT uq_artifact_dependency UNIQUE (tenant_id, job_id, page_id, dependency_page_id);

-- Name: generated_value_generation_manifests uq_generated_value_publication_epoch; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY generated_value_generation_manifests
    ADD CONSTRAINT uq_generated_value_publication_epoch UNIQUE (tenant_id, artifact_id, publication_epoch);

-- Name: generated_value_artifacts uq_generated_value_target; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY generated_value_artifacts
    ADD CONSTRAINT uq_generated_value_target UNIQUE (tenant_id, source_id, target_column);

-- Name: artifact_stage_consumer_barrier; Type: INDEX; Schema: public; Owner: -

CREATE INDEX artifact_stage_consumer_barrier ON artifact_stage_exchanges USING btree (tenant_id, job_id, consumer_stage_id);

-- Name: idx_artifact_build_artifact_state; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_artifact_build_artifact_state ON artifact_build_jobs USING btree (tenant_id, artifact_id, state, updated_at);

-- Name: idx_artifact_build_job_publish; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_artifact_build_job_publish ON artifact_build_jobs USING btree (state, publication_lease_expires_at, updated_at, tenant_id, job_id);

-- Name: idx_artifact_build_page_claim; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_artifact_build_page_claim ON artifact_build_pages USING btree (state, next_available_at, lease_expires_at, tenant_id, job_id, page_ordinal);

-- Name: idx_artifact_build_runnable_jobs; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_artifact_build_runnable_jobs ON artifact_build_jobs USING btree (state, updated_at, tenant_id, job_id);

-- Name: idx_descriptors_state_updated; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_descriptors_state_updated ON index_descriptors USING btree (state, updated_at);

-- Name: idx_file_mappings_generation; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_file_mappings_generation ON index_file_mappings USING btree (index_id, generation);

-- Name: idx_file_mappings_index_data; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_file_mappings_index_data ON index_file_mappings USING btree (index_id, data_file_path);

-- Name: idx_file_mappings_last_verified; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_file_mappings_last_verified ON index_file_mappings USING btree (last_verified_at);

-- Name: idx_file_mappings_snapshot; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_file_mappings_snapshot ON index_file_mappings USING btree (index_id, data_file_path, snapshot_token);

-- Name: idx_file_mappings_verified_created; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_file_mappings_verified_created ON index_file_mappings USING btree (last_verified_at, created_at);

-- Name: idx_generated_value_snapshot; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_generated_value_snapshot ON generated_value_generation_manifests USING btree (tenant_id, source_id, target_column, source_snapshot, state);

-- Name: idx_ich_ready_fairness; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_ich_ready_fairness ON index_commit_table_heads USING btree (last_polled_at, tenant_id, catalog_name, schema_name, table_name) WHERE (head_event_id IS NOT NULL);

-- Name: idx_icl_pending_table_sequence; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_icl_pending_table_sequence ON index_commit_log USING btree (tenant_id, catalog_name, schema_name, table_name, table_sequence) WHERE (processed = false);

-- Name: idx_icl_table_sequence; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX idx_icl_table_sequence ON index_commit_log USING btree (tenant_id, catalog_name, schema_name, table_name, table_sequence) WHERE (table_sequence IS NOT NULL);

-- Name: idx_index_descriptors_tenant_table_state; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_index_descriptors_tenant_table_state ON index_descriptors USING btree (tenant_id, catalog_name, schema_name, table_name, state);

-- Name: idx_index_generation_build_state; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_index_generation_build_state ON index_generation_builds USING btree (state, updated_at);

-- Name: uq_active_index_name_per_tenant_table; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_active_index_name_per_tenant_table ON index_descriptors USING btree (tenant_id, catalog_name, schema_name, table_name, index_name) WHERE ((state)::text <> 'DROPPED'::text);

-- Name: uq_artifact_build_generation; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_artifact_build_generation ON artifact_build_jobs USING btree (tenant_id, artifact_id, staging_generation);

-- Name: uq_file_mappings_generation_data; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_file_mappings_generation_data ON index_file_mappings USING btree (index_id, generation, data_file_path);

-- Name: uq_icl_journaled_commit; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_icl_journaled_commit ON index_commit_log USING btree (tenant_id, catalog_name, schema_name, table_name, commit_fingerprint) WHERE (commit_fingerprint IS NOT NULL);

-- Name: uq_index_generation_active; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_index_generation_active ON index_generation_manifests USING btree (index_id) WHERE ((state)::text = 'ACTIVE'::text);

-- Name: uq_index_generation_building; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_index_generation_building ON index_generation_builds USING btree (index_id) WHERE ((state)::text = 'BUILDING'::text);

-- Name: artifact_exchange_finalizations artifact_exchange_finalizatio_tenant_id_job_id_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_exchange_finalizations
    ADD CONSTRAINT artifact_exchange_finalizatio_tenant_id_job_id_exchange_id_fkey FOREIGN KEY (tenant_id, job_id, exchange_id) REFERENCES artifact_exchange_registrations(tenant_id, job_id, exchange_id) ON DELETE CASCADE;

-- Name: artifact_exchange_registrations artifact_exchange_registratio_tenant_id_job_id_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_exchange_registrations
    ADD CONSTRAINT artifact_exchange_registratio_tenant_id_job_id_exchange_id_fkey FOREIGN KEY (tenant_id, job_id, exchange_id) REFERENCES artifact_stage_exchanges(tenant_id, job_id, exchange_id) ON DELETE CASCADE;

-- Name: artifact_execution_routes artifact_execution_routes_tenant_id_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_execution_routes
    ADD CONSTRAINT artifact_execution_routes_tenant_id_job_id_fkey FOREIGN KEY (tenant_id, job_id) REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE;

-- Name: artifact_stage_exchanges artifact_stage_exchanges_tenant_id_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_stage_exchanges
    ADD CONSTRAINT artifact_stage_exchanges_tenant_id_job_id_fkey FOREIGN KEY (tenant_id, job_id) REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE;

-- Name: artifact_build_dependencies fk_artifact_build_dependency_job; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_dependencies
    ADD CONSTRAINT fk_artifact_build_dependency_job FOREIGN KEY (tenant_id, job_id) REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE;

-- Name: artifact_build_outputs fk_artifact_build_output_page; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_outputs
    ADD CONSTRAINT fk_artifact_build_output_page FOREIGN KEY (tenant_id, job_id, page_id) REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE;

-- Name: artifact_build_pages fk_artifact_build_page_job; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_pages
    ADD CONSTRAINT fk_artifact_build_page_job FOREIGN KEY (tenant_id, job_id) REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE;

-- Name: artifact_build_page_sources fk_artifact_build_source_page; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_page_sources
    ADD CONSTRAINT fk_artifact_build_source_page FOREIGN KEY (tenant_id, job_id, page_id) REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE;

-- Name: artifact_build_page_dependencies fk_artifact_dependency_input; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_page_dependencies
    ADD CONSTRAINT fk_artifact_dependency_input FOREIGN KEY (tenant_id, job_id, dependency_page_id) REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE;

-- Name: artifact_build_page_dependencies fk_artifact_dependency_page; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_page_dependencies
    ADD CONSTRAINT fk_artifact_dependency_page FOREIGN KEY (tenant_id, job_id, page_id) REFERENCES artifact_build_pages(tenant_id, job_id, page_id) ON DELETE CASCADE;

-- Name: artifact_build_dispatch_specs fk_artifact_dispatch_job; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY artifact_build_dispatch_specs
    ADD CONSTRAINT fk_artifact_dispatch_job FOREIGN KEY (tenant_id, job_id) REFERENCES artifact_build_jobs(tenant_id, job_id) ON DELETE CASCADE;

-- Name: generated_value_generation_manifests fk_generated_value_artifact; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY generated_value_generation_manifests
    ADD CONSTRAINT fk_generated_value_artifact FOREIGN KEY (tenant_id, artifact_id) REFERENCES generated_value_artifacts(tenant_id, artifact_id) ON DELETE CASCADE;

-- Name: index_file_mappings fk_index_file_mapping_index; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_file_mappings
    ADD CONSTRAINT fk_index_file_mapping_index FOREIGN KEY (index_id) REFERENCES index_descriptors(index_id) ON DELETE CASCADE;

-- Name: index_generation_builds fk_index_generation_build; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_generation_builds
    ADD CONSTRAINT fk_index_generation_build FOREIGN KEY (index_id) REFERENCES index_descriptors(index_id) ON DELETE CASCADE;

-- Name: index_generation_manifests fk_index_generation_manifest; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY index_generation_manifests
    ADD CONSTRAINT fk_index_generation_manifest FOREIGN KEY (index_id) REFERENCES index_descriptors(index_id) ON DELETE CASCADE;



-- Name: file_column_statistics; Type: TABLE; Schema: public; Owner: -

CREATE TABLE file_column_statistics (
    id bigint NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    data_file_path character varying(2048) NOT NULL,
    snapshot_id character varying(255),
    column_name character varying(255) NOT NULL,
    min_value character varying(1024),
    max_value character varying(1024),
    null_count bigint DEFAULT 0 NOT NULL,
    row_count bigint DEFAULT 0 NOT NULL,
    approximate_ndv bigint DEFAULT 0 NOT NULL,
    total_size_bytes bigint DEFAULT 0 NOT NULL,
    created_at timestamp without time zone NOT NULL,
    tenant_id character varying(255),
    CONSTRAINT chk_file_statistics_tenant CHECK (((tenant_id IS NULL) OR ((btrim((tenant_id)::text) <> ''::text) AND (snapshot_id IS NOT NULL) AND (btrim((snapshot_id)::text) <> ''::text))))
);

-- Name: file_column_statistics_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE file_column_statistics ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME file_column_statistics_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: file_column_statistics file_column_statistics_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY file_column_statistics
    ADD CONSTRAINT file_column_statistics_pkey PRIMARY KEY (id);

-- Name: idx_fcs_table; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_fcs_table ON file_column_statistics USING btree (catalog_name, schema_name, table_name, data_file_path);

-- Name: idx_file_statistics_tenant_snapshot; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_file_statistics_tenant_snapshot ON file_column_statistics USING btree (tenant_id, snapshot_id) WHERE (tenant_id IS NOT NULL);
