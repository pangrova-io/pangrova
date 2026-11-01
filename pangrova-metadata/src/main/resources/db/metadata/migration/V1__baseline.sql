-- metadata baseline: single migration owning the metadata durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: catalog, table_control, column tags, object dependencies, managed sources, invalidation/sync.

;
;

-- PostgreSQL database dump

-- Name: catalog_definitions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE catalog_definitions (
    name character varying(255) NOT NULL,
    type character varying(50) NOT NULL,
    provider character varying(50) NOT NULL,
    state character varying(20) DEFAULT 'CREATING'::character varying NOT NULL,
    properties jsonb NOT NULL,
    comment text,
    allowed_tenants jsonb,
    created_by character varying(255),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    incarnation_id character varying(36) DEFAULT '00000000-0000-0000-0000-000000000000'::character varying NOT NULL,
    revision bigint DEFAULT 1 NOT NULL
);

-- Name: catalog_descriptor_revisions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE catalog_descriptor_revisions (
    name character varying(255) NOT NULL,
    incarnation_id character varying(36) NOT NULL,
    revision bigint NOT NULL,
    type character varying(50) NOT NULL,
    provider character varying(50) NOT NULL,
    state character varying(20) NOT NULL,
    properties jsonb NOT NULL,
    comment text,
    allowed_tenants jsonb,
    recorded_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT catalog_descriptor_revisions_revision_check CHECK ((revision > 0))
);

-- Name: invalidation_log; Type: TABLE; Schema: public; Owner: -

CREATE TABLE invalidation_log (
    id bigint NOT NULL,
    cache_key character varying(1024) NOT NULL,
    invalidation_type character varying(64) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Name: invalidation_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE invalidation_log ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME invalidation_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: managed_source_cleanup_batches; Type: TABLE; Schema: public; Owner: -

CREATE TABLE managed_source_cleanup_batches (
    cleanup_id text NOT NULL,
    fingerprint text NOT NULL,
    objects_sha256 bytea NOT NULL,
    object_count integer NOT NULL,
    state text NOT NULL,
    reserved_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone,
    CONSTRAINT managed_source_cleanup_batches_check CHECK ((((state = 'DELETING'::text) AND (deleted_at IS NULL)) OR ((state = 'DELETED'::text) AND (deleted_at IS NOT NULL)))),
    CONSTRAINT managed_source_cleanup_batches_cleanup_id_check CHECK ((cleanup_id ~ '^[0-9a-f]{32}$'::text)),
    CONSTRAINT managed_source_cleanup_batches_object_count_check CHECK (((object_count >= 1) AND (object_count <= 512))),
    CONSTRAINT managed_source_cleanup_batches_objects_sha256_check CHECK ((octet_length(objects_sha256) = 32)),
    CONSTRAINT managed_source_cleanup_batches_state_check CHECK ((state = ANY (ARRAY['DELETING'::text, 'DELETED'::text])))
);

-- Name: managed_source_heads; Type: TABLE; Schema: public; Owner: -

CREATE TABLE managed_source_heads (
    catalog_name text NOT NULL,
    catalog_incarnation text NOT NULL,
    tenant_id text NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    table_id text NOT NULL,
    dataset_uri text NOT NULL,
    generation bigint NOT NULL,
    revision bigint NOT NULL,
    visible_version bigint NOT NULL,
    row_count bigint NOT NULL,
    schema_sha256 bytea NOT NULL,
    payload bytea NOT NULL,
    CONSTRAINT managed_source_heads_generation_check CHECK ((generation > 0)),
    CONSTRAINT managed_source_heads_payload_check CHECK (((octet_length(payload) > 0) AND (octet_length(payload) <= 4194304))),
    CONSTRAINT managed_source_heads_revision_check CHECK ((revision > 0)),
    CONSTRAINT managed_source_heads_row_count_check CHECK ((row_count >= 0)),
    CONSTRAINT managed_source_heads_schema_sha256_check CHECK ((octet_length(schema_sha256) = 32)),
    CONSTRAINT managed_source_heads_visible_version_check CHECK ((visible_version >= 0))
);

-- Name: managed_source_object_pages; Type: TABLE; Schema: public; Owner: -

CREATE TABLE managed_source_object_pages (
    fingerprint text NOT NULL,
    page_index integer NOT NULL,
    after_path text NOT NULL,
    last_path text NOT NULL,
    object_count integer NOT NULL,
    page_sha256 bytea NOT NULL,
    is_last boolean NOT NULL,
    CONSTRAINT managed_source_object_pages_object_count_check CHECK (((object_count >= 1) AND (object_count <= 512))),
    CONSTRAINT managed_source_object_pages_page_index_check CHECK (((page_index >= 0) AND (page_index <= 4095))),
    CONSTRAINT managed_source_object_pages_page_sha256_check CHECK ((octet_length(page_sha256) = 32))
);

-- Name: managed_source_objects; Type: TABLE; Schema: public; Owner: -

CREATE TABLE managed_source_objects (
    fingerprint text NOT NULL,
    relative_path text NOT NULL,
    size_bytes bigint NOT NULL,
    purpose text NOT NULL,
    cleanup_id text,
    deleted_at timestamp with time zone,
    CONSTRAINT managed_source_objects_purpose_check CHECK ((purpose = ANY (ARRAY['SOURCE'::text, 'WINNER_SIDECAR'::text]))),
    CONSTRAINT managed_source_objects_size_bytes_check CHECK ((size_bytes >= 0))
);

-- Name: managed_source_prepare_invocations; Type: TABLE; Schema: public; Owner: -

CREATE TABLE managed_source_prepare_invocations (
    invocation_id text NOT NULL,
    fingerprint text NOT NULL,
    execution_id text NOT NULL,
    page_index integer NOT NULL,
    after_path text NOT NULL,
    worker_endpoint text NOT NULL,
    request_sha256 bytea NOT NULL,
    issued_owner_id text NOT NULL,
    issued_owner_incarnation text NOT NULL,
    issued_owner_token bigint NOT NULL,
    state text NOT NULL,
    response_sha256 bytea,
    issued_at timestamp with time zone NOT NULL,
    drained_at timestamp with time zone,
    CONSTRAINT managed_source_prepare_invocations_check CHECK ((((state = 'ISSUED'::text) AND (response_sha256 IS NULL) AND (drained_at IS NULL)) OR ((state = 'DRAINED'::text) AND (octet_length(response_sha256) = 32) AND (drained_at IS NOT NULL)))),
    CONSTRAINT managed_source_prepare_invocations_invocation_id_check CHECK ((invocation_id ~ '^[0-9a-f]{32}$'::text)),
    CONSTRAINT managed_source_prepare_invocations_issued_owner_token_check CHECK ((issued_owner_token > 0)),
    CONSTRAINT managed_source_prepare_invocations_page_index_check CHECK (((page_index >= 0) AND (page_index <= 4095))),
    CONSTRAINT managed_source_prepare_invocations_request_sha256_check CHECK ((octet_length(request_sha256) = 32)),
    CONSTRAINT managed_source_prepare_invocations_state_check CHECK ((state = ANY (ARRAY['ISSUED'::text, 'DRAINED'::text]))),
    CONSTRAINT managed_source_prepare_invocations_worker_endpoint_check CHECK (((length(worker_endpoint) >= 1) AND (length(worker_endpoint) <= 2048)))
);

-- Name: managed_source_publications; Type: TABLE; Schema: public; Owner: -

CREATE TABLE managed_source_publications (
    fingerprint text NOT NULL,
    execution_id text NOT NULL,
    catalog_name text NOT NULL,
    catalog_incarnation text NOT NULL,
    catalog_revision bigint NOT NULL,
    tenant_id text NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    table_id text NOT NULL,
    dataset_uri text NOT NULL,
    generation bigint NOT NULL,
    base_revision bigint NOT NULL,
    base_version bigint NOT NULL,
    state text NOT NULL,
    manifest_relative_path text NOT NULL,
    published_version bigint NOT NULL,
    schema_sha256 bytea NOT NULL,
    manifest_sha256 bytea,
    manifest_size bigint,
    naming_scheme text,
    row_count bigint,
    payload bytea,
    inherited_membership text,
    object_count bigint,
    objects_sha256 bytea,
    registered_count bigint DEFAULT 0 NOT NULL,
    next_object_page integer DEFAULT 0 NOT NULL,
    intent_owner_id text NOT NULL,
    intent_owner_incarnation text NOT NULL,
    intent_owner_token bigint NOT NULL,
    CONSTRAINT managed_source_publications_base_revision_check CHECK ((base_revision > 0)),
    CONSTRAINT managed_source_publications_base_version_check CHECK ((base_version >= 0)),
    CONSTRAINT managed_source_publications_catalog_revision_check CHECK ((catalog_revision > 0)),
    CONSTRAINT managed_source_publications_check CHECK (((state <> 'ACCEPTED'::text) OR ((octet_length(manifest_sha256) = 32) AND (manifest_size > 0) AND (naming_scheme = ANY (ARRAY['V1'::text, 'V2'::text])) AND (row_count >= 0) AND (octet_length(payload) > 0)))),
    CONSTRAINT managed_source_publications_check1 CHECK (((state <> 'ABORTED'::text) OR (payload IS NULL))),
    CONSTRAINT managed_source_publications_fingerprint_check CHECK ((fingerprint ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT managed_source_publications_generation_check CHECK ((generation > 0)),
    CONSTRAINT managed_source_publications_intent_owner_token_check CHECK ((intent_owner_token > 0)),
    CONSTRAINT managed_source_publications_next_object_page_check CHECK (((next_object_page >= 0) AND (next_object_page <= 4096))),
    CONSTRAINT managed_source_publications_published_version_check CHECK ((published_version > 0)),
    CONSTRAINT managed_source_publications_schema_sha256_check CHECK ((octet_length(schema_sha256) = 32)),
    CONSTRAINT managed_source_publications_state_check CHECK ((state = ANY (ARRAY['PREPARING'::text, 'PREPARED'::text, 'ACCEPTED'::text, 'ABORTED'::text])))
);

-- Name: managed_source_schemas; Type: TABLE; Schema: public; Owner: -

CREATE TABLE managed_source_schemas (
    catalog_name text NOT NULL,
    catalog_incarnation text NOT NULL,
    tenant_id text NOT NULL,
    schema_name text NOT NULL
);

-- Name: pangrova_object_dependencies; Type: TABLE; Schema: public; Owner: -

CREATE TABLE pangrova_object_dependencies (
    parent_catalog character varying(255) NOT NULL,
    parent_schema character varying(255) NOT NULL,
    parent_object character varying(255) NOT NULL,
    parent_kind character varying(32) NOT NULL,
    child_catalog character varying(255) NOT NULL,
    child_schema character varying(255) NOT NULL,
    child_object character varying(255) NOT NULL,
    child_kind character varying(32) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Name: schema_metadata; Type: TABLE; Schema: public; Owner: -

CREATE TABLE schema_metadata (
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    properties jsonb,
    synced_at timestamp without time zone NOT NULL
);

-- Name: sync_history; Type: TABLE; Schema: public; Owner: -

CREATE TABLE sync_history (
    id bigint NOT NULL,
    catalog_name character varying(255) NOT NULL,
    sync_type character varying(20) NOT NULL,
    sync_time timestamp without time zone NOT NULL,
    changes_applied integer DEFAULT 0 NOT NULL,
    errors integer DEFAULT 0 NOT NULL,
    error_messages jsonb,
    duration_ms bigint NOT NULL
);

-- Name: sync_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE sync_history ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME sync_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: table_control_governance_binding; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_control_governance_binding (
    binding_id uuid NOT NULL,
    table_id uuid NOT NULL,
    binding_type character varying(32) NOT NULL,
    binding_ref character varying(1024) NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_by character varying(255) DEFAULT ''::character varying NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT table_control_governance_binding_type_check CHECK (((binding_type)::text = ANY ((ARRAY['COLUMN_TAG'::character varying, 'COLUMN_MASK'::character varying, 'ROW_FILTER'::character varying, 'QUALITY_RULE'::character varying, 'LINEAGE'::character varying])::text[])))
);

-- Name: table_control_maintenance_policy; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_control_maintenance_policy (
    table_id uuid NOT NULL,
    enabled_operations jsonb DEFAULT '[]'::jsonb NOT NULL,
    retain_last integer DEFAULT 10 NOT NULL,
    orphan_file_age_seconds bigint DEFAULT 259200 NOT NULL,
    snapshot_retention_seconds bigint DEFAULT 604800 NOT NULL,
    require_stable_external_version boolean DEFAULT true NOT NULL,
    updated_by character varying(255) DEFAULT ''::character varying NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT table_control_maintenance_policy_orphan_age_check CHECK ((orphan_file_age_seconds >= 86400)),
    CONSTRAINT table_control_maintenance_policy_retain_last_check CHECK ((retain_last >= 1)),
    CONSTRAINT table_control_maintenance_policy_snapshot_retention_check CHECK ((snapshot_retention_seconds >= 86400))
);

-- Name: table_control_maintenance_run; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_control_maintenance_run (
    run_id uuid NOT NULL,
    table_id uuid NOT NULL,
    operation character varying(32) NOT NULL,
    mode character varying(16) NOT NULL,
    state character varying(16) NOT NULL,
    version_before character varying(1024),
    version_after character varying(1024),
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    actor character varying(255) DEFAULT ''::character varying NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone
);

-- Name: table_control_name_reservation; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_control_name_reservation (
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    table_id uuid NOT NULL
);

-- Name: table_control_registry; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_control_registry (
    table_id uuid NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    format character varying(32) NOT NULL,
    format_table_uuid character varying(255),
    format_location text,
    ownership_mode character varying(16) NOT NULL,
    state character varying(32) NOT NULL,
    provenance character varying(16) NOT NULL,
    catalog_incarnation_id character varying(64),
    catalog_revision bigint,
    metadata_revision bigint DEFAULT 0 NOT NULL,
    version_token character varying(1024),
    version_observed_at timestamp with time zone,
    owner_principal character varying(255),
    properties jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_signature character varying(512),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_synced_at timestamp with time zone,
    last_maintained_at timestamp with time zone,
    CONSTRAINT table_control_registry_name_not_blank CHECK ((((catalog_name)::text <> ''::text) AND ((schema_name)::text <> ''::text) AND ((table_name)::text <> ''::text))),
    CONSTRAINT table_control_registry_ownership_mode_check CHECK (((ownership_mode)::text = ANY ((ARRAY['LINKED'::character varying, 'MANAGED'::character varying])::text[])))
);

-- Name: table_control_statistics; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_control_statistics (
    table_id uuid NOT NULL,
    stats_version bigint NOT NULL,
    version_token character varying(1024) NOT NULL,
    row_count bigint NOT NULL,
    data_size bigint NOT NULL,
    source_work_units bigint DEFAULT '-1'::integer NOT NULL,
    column_stats jsonb DEFAULT '{}'::jsonb NOT NULL,
    source character varying(64) NOT NULL,
    computed_at timestamp with time zone NOT NULL,
    CONSTRAINT chk_table_control_statistics_counts CHECK (((row_count >= '-1'::integer) AND (data_size >= '-1'::integer) AND (source_work_units >= '-1'::integer))),
    CONSTRAINT chk_table_control_statistics_version CHECK ((stats_version >= 0))
);

-- Name: table_control_version_observation; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_control_version_observation (
    observation_id bigint NOT NULL,
    table_id uuid NOT NULL,
    version_token character varying(1024) NOT NULL,
    previous_version_token character varying(1024),
    source character varying(32) NOT NULL,
    observed_at timestamp with time zone NOT NULL
);

-- Name: table_control_version_observation_observation_id_seq; Type: SEQUENCE; Schema: public; Owner: -

CREATE SEQUENCE table_control_version_observation_observation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: table_control_version_observation_observation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -

ALTER SEQUENCE table_control_version_observation_observation_id_seq OWNED BY table_control_version_observation.observation_id;

-- Name: table_metadata; Type: TABLE; Schema: public; Owner: -

CREATE TABLE table_metadata (
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    columns jsonb NOT NULL,
    partition_spec jsonb,
    sort_order jsonb,
    properties jsonb,
    table_created_at timestamp without time zone,
    table_updated_at timestamp without time zone,
    synced_at timestamp without time zone NOT NULL,
    table_id uuid,
    metadata_version bigint DEFAULT 0 NOT NULL,
    projection_hash character varying(64)
);

-- Name: table_control_version_observation observation_id; Type: DEFAULT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_version_observation ALTER COLUMN observation_id SET DEFAULT nextval('table_control_version_observation_observation_id_seq'::regclass);

-- Name: catalog_definitions catalog_definitions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY catalog_definitions
    ADD CONSTRAINT catalog_definitions_pkey PRIMARY KEY (name);

-- Name: catalog_descriptor_revisions catalog_descriptor_revisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY catalog_descriptor_revisions
    ADD CONSTRAINT catalog_descriptor_revisions_pkey PRIMARY KEY (name, incarnation_id, revision);

-- Name: invalidation_log invalidation_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY invalidation_log
    ADD CONSTRAINT invalidation_log_pkey PRIMARY KEY (id);

-- Name: managed_source_cleanup_batches managed_source_cleanup_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_cleanup_batches
    ADD CONSTRAINT managed_source_cleanup_batches_pkey PRIMARY KEY (cleanup_id);

-- Name: managed_source_heads managed_source_heads_catalog_name_catalog_incarnation_tenan_key; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_heads
    ADD CONSTRAINT managed_source_heads_catalog_name_catalog_incarnation_tenan_key UNIQUE (catalog_name, catalog_incarnation, tenant_id, table_id);

-- Name: managed_source_heads managed_source_heads_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_heads
    ADD CONSTRAINT managed_source_heads_pkey PRIMARY KEY (catalog_name, catalog_incarnation, tenant_id, schema_name, table_name);

-- Name: managed_source_object_pages managed_source_object_pages_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_object_pages
    ADD CONSTRAINT managed_source_object_pages_pkey PRIMARY KEY (fingerprint, page_index);

-- Name: managed_source_objects managed_source_objects_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_objects
    ADD CONSTRAINT managed_source_objects_pkey PRIMARY KEY (fingerprint, relative_path);

-- Name: managed_source_prepare_invocations managed_source_prepare_invocations_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_prepare_invocations
    ADD CONSTRAINT managed_source_prepare_invocations_pkey PRIMARY KEY (invocation_id);

-- Name: managed_source_publications managed_source_publications_catalog_name_catalog_incarnatio_key; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_publications
    ADD CONSTRAINT managed_source_publications_catalog_name_catalog_incarnatio_key UNIQUE (catalog_name, catalog_incarnation, tenant_id, table_id, published_version, fingerprint);

-- Name: managed_source_publications managed_source_publications_execution_id_key; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_publications
    ADD CONSTRAINT managed_source_publications_execution_id_key UNIQUE (execution_id);

-- Name: managed_source_publications managed_source_publications_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_publications
    ADD CONSTRAINT managed_source_publications_pkey PRIMARY KEY (fingerprint);

-- Name: managed_source_schemas managed_source_schemas_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_schemas
    ADD CONSTRAINT managed_source_schemas_pkey PRIMARY KEY (catalog_name, catalog_incarnation, tenant_id, schema_name);

-- Name: pangrova_object_dependencies pangrova_object_dependencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY pangrova_object_dependencies
    ADD CONSTRAINT pangrova_object_dependencies_pkey PRIMARY KEY (parent_catalog, parent_schema, parent_object, child_catalog, child_schema, child_object);

-- Name: schema_metadata schema_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY schema_metadata
    ADD CONSTRAINT schema_metadata_pkey PRIMARY KEY (catalog_name, schema_name);

-- Name: sync_history sync_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY sync_history
    ADD CONSTRAINT sync_history_pkey PRIMARY KEY (id);

-- Name: table_control_governance_binding table_control_governance_binding_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_governance_binding
    ADD CONSTRAINT table_control_governance_binding_pkey PRIMARY KEY (binding_id);

-- Name: table_control_governance_binding table_control_governance_binding_unique; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_governance_binding
    ADD CONSTRAINT table_control_governance_binding_unique UNIQUE (table_id, binding_type, binding_ref);

-- Name: table_control_maintenance_policy table_control_maintenance_policy_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_maintenance_policy
    ADD CONSTRAINT table_control_maintenance_policy_pkey PRIMARY KEY (table_id);

-- Name: table_control_maintenance_run table_control_maintenance_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_maintenance_run
    ADD CONSTRAINT table_control_maintenance_run_pkey PRIMARY KEY (run_id);

-- Name: table_control_name_reservation table_control_name_reservation_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_name_reservation
    ADD CONSTRAINT table_control_name_reservation_pkey PRIMARY KEY (catalog_name, schema_name, table_name);

-- Name: table_control_registry table_control_registry_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_registry
    ADD CONSTRAINT table_control_registry_pkey PRIMARY KEY (table_id);

-- Name: table_control_statistics table_control_statistics_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_statistics
    ADD CONSTRAINT table_control_statistics_pkey PRIMARY KEY (table_id);

-- Name: table_control_version_observation table_control_version_observation_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_version_observation
    ADD CONSTRAINT table_control_version_observation_pkey PRIMARY KEY (observation_id);

-- Name: table_metadata table_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_metadata
    ADD CONSTRAINT table_metadata_pkey PRIMARY KEY (catalog_name, schema_name, table_name);

-- Name: idx_catalog_time; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_catalog_time ON sync_history USING btree (catalog_name, sync_time);

-- Name: idx_obj_deps_child; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_obj_deps_child ON pangrova_object_dependencies USING btree (child_catalog, child_schema, child_object);

-- Name: idx_obj_deps_parent; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_obj_deps_parent ON pangrova_object_dependencies USING btree (parent_catalog, parent_schema, parent_object);

-- Name: idx_table_control_governance_binding_table; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_table_control_governance_binding_table ON table_control_governance_binding USING btree (table_id, binding_type);

-- Name: idx_table_control_maintenance_run_table; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_table_control_maintenance_run_table ON table_control_maintenance_run USING btree (table_id, started_at DESC);

-- Name: idx_table_control_name_reservation_id; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_table_control_name_reservation_id ON table_control_name_reservation USING btree (table_id);

-- Name: idx_table_control_registry_catalog; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_table_control_registry_catalog ON table_control_registry USING btree (catalog_name, ownership_mode, state);

-- Name: idx_table_control_registry_format_uuid; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_table_control_registry_format_uuid ON table_control_registry USING btree (format_table_uuid);

-- Name: idx_table_control_registry_revision; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_table_control_registry_revision ON table_control_registry USING btree (metadata_revision);

-- Name: idx_table_control_version_observation_table; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_table_control_version_observation_table ON table_control_version_observation USING btree (table_id, observed_at DESC);

-- Name: managed_source_accepted_versions; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX managed_source_accepted_versions ON managed_source_publications USING btree (catalog_name, catalog_incarnation, tenant_id, table_id, published_version) WHERE (state = 'ACCEPTED'::text);

-- Name: managed_source_prepare_by_candidate; Type: INDEX; Schema: public; Owner: -

CREATE INDEX managed_source_prepare_by_candidate ON managed_source_prepare_invocations USING btree (fingerprint, invocation_id);

-- Name: uq_table_control_registry_live_name; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_table_control_registry_live_name ON table_control_registry USING btree (catalog_name, schema_name, table_name) WHERE ((state)::text <> 'DROPPED'::text);

-- Name: uq_table_metadata_table_id; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX uq_table_metadata_table_id ON table_metadata USING btree (table_id) WHERE (table_id IS NOT NULL);

-- Name: catalog_descriptor_revisions catalog_descriptor_revisions_no_update; Type: TRIGGER; Schema: public; Owner: -

CREATE OR REPLACE FUNCTION fence_invalidation_append()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        current_database() || ':' || TG_TABLE_SCHEMA || ':' || TG_TABLE_NAME, 0));
    RETURN NULL;
END;
$function$;
CREATE OR REPLACE FUNCTION reject_catalog_descriptor_revision_update()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'catalog descriptor revisions are immutable';
END;
$function$;
CREATE TRIGGER catalog_descriptor_revisions_no_update BEFORE UPDATE ON catalog_descriptor_revisions FOR EACH ROW EXECUTE FUNCTION reject_catalog_descriptor_revision_update();

-- Name: invalidation_log invalidation_append_commit_gate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER invalidation_append_commit_gate BEFORE INSERT ON invalidation_log FOR EACH STATEMENT EXECUTE FUNCTION fence_invalidation_append();

-- Name: managed_source_cleanup_batches managed_source_cleanup_batches_fingerprint_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_cleanup_batches
    ADD CONSTRAINT managed_source_cleanup_batches_fingerprint_fkey FOREIGN KEY (fingerprint) REFERENCES managed_source_publications(fingerprint);

-- Name: managed_source_objects managed_source_cleanup_owner; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_objects
    ADD CONSTRAINT managed_source_cleanup_owner FOREIGN KEY (cleanup_id) REFERENCES managed_source_cleanup_batches(cleanup_id);

-- Name: managed_source_heads managed_source_heads_catalog_name_catalog_incarnation_tena_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_heads
    ADD CONSTRAINT managed_source_heads_catalog_name_catalog_incarnation_tena_fkey FOREIGN KEY (catalog_name, catalog_incarnation, tenant_id, schema_name) REFERENCES managed_source_schemas(catalog_name, catalog_incarnation, tenant_id, schema_name);

-- Name: managed_source_object_pages managed_source_object_pages_fingerprint_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_object_pages
    ADD CONSTRAINT managed_source_object_pages_fingerprint_fkey FOREIGN KEY (fingerprint) REFERENCES managed_source_publications(fingerprint);

-- Name: managed_source_objects managed_source_objects_fingerprint_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_objects
    ADD CONSTRAINT managed_source_objects_fingerprint_fkey FOREIGN KEY (fingerprint) REFERENCES managed_source_publications(fingerprint);

-- Name: managed_source_prepare_invocations managed_source_prepare_invocations_fingerprint_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_prepare_invocations
    ADD CONSTRAINT managed_source_prepare_invocations_fingerprint_fkey FOREIGN KEY (fingerprint) REFERENCES managed_source_publications(fingerprint);

-- Name: managed_source_publications managed_source_publications_inherited_membership_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY managed_source_publications
    ADD CONSTRAINT managed_source_publications_inherited_membership_fkey FOREIGN KEY (inherited_membership) REFERENCES managed_source_publications(fingerprint);

-- Name: schema_metadata schema_metadata_catalog_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY schema_metadata
    ADD CONSTRAINT schema_metadata_catalog_name_fkey FOREIGN KEY (catalog_name) REFERENCES catalog_definitions(name) ON DELETE CASCADE;

-- Name: table_control_name_reservation table_control_name_reservation_table_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_control_name_reservation
    ADD CONSTRAINT table_control_name_reservation_table_id_fkey FOREIGN KEY (table_id) REFERENCES table_control_registry(table_id);

-- Name: table_metadata table_metadata_catalog_name_schema_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY table_metadata
    ADD CONSTRAINT table_metadata_catalog_name_schema_name_fkey FOREIGN KEY (catalog_name, schema_name) REFERENCES schema_metadata(catalog_name, schema_name) ON DELETE CASCADE;

