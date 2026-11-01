-- query baseline: single migration owning the query durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: journal/anchor/write commit/copy admission/checkpoints.
-- Depends on the security schema (storage_profile_revisions FK).

;
;
;
;
;
;
;

-- PostgreSQL database dump

-- Name: checkpoints; Type: TABLE; Schema: public; Owner: -

CREATE TABLE checkpoints (
    component_id character varying(255) NOT NULL,
    state bytea NOT NULL,
    saved_at timestamp without time zone NOT NULL
);

-- Name: copy_file_claims; Type: TABLE; Schema: public; Owner: -

CREATE TABLE copy_file_claims (
    claim_key character(64) NOT NULL,
    claim_digest character(64) NOT NULL,
    tenant_id text NOT NULL,
    table_uuid uuid NOT NULL,
    source_url text NOT NULL,
    source_version_id text NOT NULL,
    source_sha256 character(64) NOT NULL,
    source_size bigint NOT NULL,
    profile_name text NOT NULL,
    profile_incarnation_id uuid NOT NULL,
    profile_revision bigint NOT NULL,
    execution_id text NOT NULL,
    claimed_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    load_generation bigint NOT NULL,
    CONSTRAINT chk_copy_file_generation CHECK ((load_generation >= 0)),
    CONSTRAINT copy_file_claims_claim_digest_check CHECK ((claim_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_file_claims_claim_key_check CHECK ((claim_key ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_file_claims_execution_id_check CHECK ((length(execution_id) > 0)),
    CONSTRAINT copy_file_claims_profile_name_check CHECK ((length(profile_name) > 0)),
    CONSTRAINT copy_file_claims_profile_revision_check CHECK ((profile_revision > 0)),
    CONSTRAINT copy_file_claims_source_sha256_check CHECK ((source_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_file_claims_source_size_check CHECK ((source_size >= 0)),
    CONSTRAINT copy_file_claims_source_url_check CHECK (((length(source_url) > 0) AND (octet_length(source_url) <= 1093))),
    CONSTRAINT copy_file_claims_source_version_id_check CHECK (((length(source_version_id) > 0) AND (source_version_id <> 'null'::text) AND (octet_length(source_version_id) <= 1024))),
    CONSTRAINT copy_file_claims_tenant_id_check CHECK ((length(tenant_id) > 0))
);

-- Name: copy_file_publications; Type: TABLE; Schema: public; Owner: -

CREATE TABLE copy_file_publications (
    claim_key character(64) NOT NULL,
    load_generation bigint NOT NULL,
    claim_digest character(64) NOT NULL,
    execution_id character varying(255) NOT NULL,
    tenant_id character varying(255) NOT NULL,
    table_uuid uuid NOT NULL,
    commit_fingerprint character(64) NOT NULL,
    snapshot_id bigint NOT NULL,
    published_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT copy_file_publications_claim_digest_check CHECK ((claim_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_file_publications_commit_fingerprint_check CHECK ((commit_fingerprint ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_file_publications_snapshot_id_check CHECK ((snapshot_id > 0))
);

-- Name: copy_source_read_admissions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE copy_source_read_admissions (
    nonce_sha256 character(64) NOT NULL,
    tenant_id text NOT NULL,
    principal text NOT NULL,
    identity_groups jsonb NOT NULL,
    identity_attributes jsonb NOT NULL,
    authenticated_at timestamp with time zone NOT NULL,
    authorization_revision bigint NOT NULL,
    profile_name character varying(255) NOT NULL,
    profile_incarnation_id uuid NOT NULL,
    profile_revision bigint NOT NULL,
    canonical_source_url text NOT NULL,
    managed_storage_root_uri text NOT NULL,
    managed_backend_identity_sha256 character(64) NOT NULL,
    execution_id text NOT NULL,
    staging_attempt_id text NOT NULL,
    staging_command_sha256 character(64) NOT NULL,
    owner_id text NOT NULL,
    owner_incarnation text NOT NULL,
    control_epoch bigint NOT NULL,
    owner_fencing_token bigint NOT NULL,
    deadline_at timestamp with time zone NOT NULL,
    issued_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    redeemed_at timestamp with time zone,
    staged_source_version_id text,
    staged_managed_object_uri text,
    staged_content_sha256 character(64),
    staged_content_size bigint,
    staged_tenant_scope character(64),
    staged_at timestamp with time zone,
    parent_nonce_sha256 character(64),
    CONSTRAINT chk_copy_source_staged_proof CHECK ((((staged_source_version_id IS NULL) AND (staged_managed_object_uri IS NULL) AND (staged_content_sha256 IS NULL) AND (staged_content_size IS NULL) AND (staged_tenant_scope IS NULL) AND (staged_at IS NULL)) OR (((redeemed_at IS NOT NULL) OR (parent_nonce_sha256 IS NOT NULL)) AND (staged_source_version_id IS NOT NULL) AND (staged_source_version_id <> ''::text) AND (staged_source_version_id <> 'null'::text) AND (octet_length(staged_source_version_id) <= 1024) AND (staged_managed_object_uri IS NOT NULL) AND (length(staged_managed_object_uri) > 0) AND (octet_length(staged_managed_object_uri) <= 2048) AND (staged_content_sha256 IS NOT NULL) AND (staged_content_sha256 ~ '^[0-9a-f]{64}$'::text) AND (staged_content_size IS NOT NULL) AND (staged_content_size >= 0) AND (staged_tenant_scope IS NOT NULL) AND (staged_tenant_scope ~ '^[0-9a-f]{64}$'::text) AND (staged_at IS NOT NULL)))),
    CONSTRAINT copy_source_read_admissions_authorization_revision_check CHECK ((authorization_revision > 0)),
    CONSTRAINT copy_source_read_admissions_canonical_source_url_check CHECK (((length(canonical_source_url) > 0) AND (octet_length(canonical_source_url) <= 1093))),
    CONSTRAINT copy_source_read_admissions_control_epoch_check CHECK ((control_epoch > 0)),
    CONSTRAINT copy_source_read_admissions_execution_id_check CHECK ((length(execution_id) > 0)),
    CONSTRAINT copy_source_read_admissions_managed_backend_identity_sha2_check CHECK ((managed_backend_identity_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_source_read_admissions_managed_storage_root_uri_check CHECK (((length(managed_storage_root_uri) > 0) AND (octet_length(managed_storage_root_uri) <= 1024))),
    CONSTRAINT copy_source_read_admissions_nonce_sha256_check CHECK ((nonce_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_source_read_admissions_owner_fencing_token_check CHECK ((owner_fencing_token > 0)),
    CONSTRAINT copy_source_read_admissions_owner_id_check CHECK ((length(owner_id) > 0)),
    CONSTRAINT copy_source_read_admissions_owner_incarnation_check CHECK ((length(owner_incarnation) > 0)),
    CONSTRAINT copy_source_read_admissions_principal_check CHECK ((length(principal) > 0)),
    CONSTRAINT copy_source_read_admissions_profile_revision_check CHECK ((profile_revision > 0)),
    CONSTRAINT copy_source_read_admissions_staging_attempt_id_check CHECK ((length(staging_attempt_id) > 0)),
    CONSTRAINT copy_source_read_admissions_staging_command_sha256_check CHECK ((staging_command_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT copy_source_read_admissions_tenant_id_check CHECK ((length(tenant_id) > 0))
);

-- Name: execution_anchor_token_seq; Type: SEQUENCE; Schema: public; Owner: -
-- CACHE 1 is required: pooled sessions must never hold a cached range that could issue a
-- token below one already handed out on another connection. First issued token is 2, the
-- final shape of the legacy chain's fencing-token migration (restart above reserved range).

CREATE SEQUENCE execution_anchor_token_seq
    AS BIGINT
    START WITH 2
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1
    NO CYCLE;

-- Name: execution_anchor; Type: TABLE; Schema: public; Owner: -

CREATE TABLE execution_anchor (
    execution_id character varying(128) NOT NULL,
    owner_id character varying(128) NOT NULL,
    owner_incarnation character varying(64) NOT NULL,
    fencing_token bigint NOT NULL,
    acquired_at timestamp(6) with time zone NOT NULL,
    expires_at timestamp(6) with time zone NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT execution_anchor_fencing_token_check CHECK ((fencing_token > 0))
);

-- Name: query_lineage_outbox; Type: TABLE; Schema: public; Owner: -

CREATE TABLE query_lineage_outbox (
    event_id uuid NOT NULL,
    namespace text NOT NULL,
    query_id text NOT NULL,
    category text NOT NULL,
    payload jsonb NOT NULL,
    created_at_ms bigint NOT NULL,
    delivery_token bigint DEFAULT 0 NOT NULL,
    lease_until timestamp with time zone,
    delivered_at timestamp with time zone,
    CONSTRAINT query_lineage_outbox_category_check CHECK ((category = ANY (ARRAY['START'::text, 'TERMINAL'::text]))),
    CONSTRAINT query_lineage_outbox_delivery_token_check CHECK ((delivery_token >= 0))
);

-- Name: query_state_journal; Type: TABLE; Schema: public; Owner: -

CREATE TABLE query_state_journal (
    id bigint NOT NULL,
    query_id character varying(64) NOT NULL,
    sql_text text,
    session_json text,
    state character varying(32) NOT NULL,
    timestamp_ms bigint NOT NULL,
    coordinator_id character varying(128),
    epoch bigint DEFAULT 0 NOT NULL,
    event_id uuid NOT NULL,
    lineage_payload jsonb
);

-- Name: query_state_journal_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE query_state_journal ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME query_state_journal_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: task_state_journal; Type: TABLE; Schema: public; Owner: -

CREATE TABLE task_state_journal (
    id bigint NOT NULL,
    query_id character varying(64) NOT NULL,
    task_id character varying(64) NOT NULL,
    worker_id character varying(64),
    state character varying(32) NOT NULL,
    stage_id integer NOT NULL,
    report_sequence bigint DEFAULT 0 NOT NULL,
    timestamp_ms bigint NOT NULL,
    coordinator_id character varying(128),
    epoch bigint DEFAULT 0 NOT NULL,
    event_id uuid NOT NULL
);

-- Name: task_state_journal_id_seq; Type: SEQUENCE; Schema: public; Owner: -

ALTER TABLE task_state_journal ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME task_state_journal_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

-- Name: write_commit_journal; Type: TABLE; Schema: public; Owner: -

CREATE TABLE write_commit_journal (
    query_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    table_name character varying(1024) NOT NULL,
    state character varying(32) NOT NULL,
    timestamp_ms bigint NOT NULL,
    fragments text,
    write_mode character varying(64),
    idempotency_key character varying(512) NOT NULL,
    owner_id character varying(255),
    owner_incarnation character varying(255),
    execution_fencing_token bigint NOT NULL,
    tenant_id character varying(255) NOT NULL,
    base_snapshot_id bigint NOT NULL,
    write_output_binding text NOT NULL,
    commit_fingerprint character varying(64) NOT NULL,
    published_version bigint,
    manifest_sha256 bytea,
    principal character varying(255),
    deferred_authorization_scope bytea,
    deferred_work_acknowledged boolean DEFAULT false NOT NULL,
    copy_claim_key character(64),
    copy_load_generation bigint,
    copy_claim_digest character(64),
    copy_table_uuid uuid,
    CONSTRAINT chk_write_commit_authenticated_authority CHECK ((((principal IS NULL) AND (deferred_authorization_scope IS NULL)) OR ((principal IS NOT NULL) AND (btrim((principal)::text) <> ''::text) AND (deferred_authorization_scope IS NOT NULL)))),
    CONSTRAINT chk_write_commit_base_snapshot CHECK ((base_snapshot_id >= '-1'::integer)),
    CONSTRAINT chk_write_commit_fingerprint CHECK (((commit_fingerprint)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_write_commit_owner CHECK ((((owner_id IS NULL) AND (owner_incarnation IS NULL) AND (execution_fencing_token = 0)) OR ((owner_id IS NOT NULL) AND (owner_incarnation IS NOT NULL) AND (execution_fencing_token > 0)))),
    CONSTRAINT chk_write_commit_publication CHECK ((((published_version IS NULL) AND (manifest_sha256 IS NULL)) OR ((published_version IS NOT NULL) AND (published_version > 0) AND (manifest_sha256 IS NOT NULL) AND (octet_length(manifest_sha256) = 32)))),
    CONSTRAINT chk_write_commit_terminal_state CHECK ((((state)::text = ANY ((ARRAY['PENDING'::character varying, 'COMMITTED'::character varying, 'ABORTED'::character varying])::text[])) AND (((state)::text <> 'ABORTED'::text) OR ((published_version IS NULL) AND (manifest_sha256 IS NULL))))),
    CONSTRAINT chk_write_copy_binding CHECK ((((copy_claim_key IS NULL) AND (copy_load_generation IS NULL) AND (copy_claim_digest IS NULL) AND (copy_table_uuid IS NULL)) OR ((copy_claim_key ~ '^[0-9a-f]{64}$'::text) AND (copy_load_generation >= 0) AND (copy_claim_digest ~ '^[0-9a-f]{64}$'::text) AND (copy_table_uuid IS NOT NULL) AND ((write_mode)::text = 'INSERT'::text))))
);

-- Name: write_commit_quarantine; Type: TABLE; Schema: public; Owner: -

CREATE TABLE write_commit_quarantine (
    query_id character varying(255) NOT NULL,
    commit_fingerprint character varying(64) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    table_name character varying(1024) NOT NULL,
    reason character varying(1024) NOT NULL,
    quarantined_at timestamp with time zone NOT NULL,
    CONSTRAINT chk_write_commit_quarantine_fingerprint CHECK (((commit_fingerprint)::text ~ '^[0-9a-f]{64}$'::text))
);

-- Name: checkpoints checkpoints_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY checkpoints
    ADD CONSTRAINT checkpoints_pkey PRIMARY KEY (component_id);

-- Name: copy_file_claims copy_file_claims_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY copy_file_claims
    ADD CONSTRAINT copy_file_claims_pkey PRIMARY KEY (claim_key, load_generation);

-- Name: copy_file_publications copy_file_publications_commit_fingerprint_key; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY copy_file_publications
    ADD CONSTRAINT copy_file_publications_commit_fingerprint_key UNIQUE (commit_fingerprint);

-- Name: copy_file_publications copy_file_publications_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY copy_file_publications
    ADD CONSTRAINT copy_file_publications_pkey PRIMARY KEY (claim_key, load_generation);

-- Name: copy_source_read_admissions copy_source_read_admissions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY copy_source_read_admissions
    ADD CONSTRAINT copy_source_read_admissions_pkey PRIMARY KEY (nonce_sha256);

-- Name: execution_anchor execution_anchor_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY execution_anchor
    ADD CONSTRAINT execution_anchor_pkey PRIMARY KEY (execution_id);

-- Name: query_lineage_outbox query_lineage_outbox_namespace_query_id_category_key; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY query_lineage_outbox
    ADD CONSTRAINT query_lineage_outbox_namespace_query_id_category_key UNIQUE (namespace, query_id, category);

-- Name: query_lineage_outbox query_lineage_outbox_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY query_lineage_outbox
    ADD CONSTRAINT query_lineage_outbox_pkey PRIMARY KEY (event_id);

-- Name: query_state_journal query_state_journal_event_identity; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY query_state_journal
    ADD CONSTRAINT query_state_journal_event_identity UNIQUE (event_id);

-- Name: query_state_journal query_state_journal_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY query_state_journal
    ADD CONSTRAINT query_state_journal_pkey PRIMARY KEY (id);

-- Name: task_state_journal task_state_journal_event_identity; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY task_state_journal
    ADD CONSTRAINT task_state_journal_event_identity UNIQUE (event_id);

-- Name: task_state_journal task_state_journal_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY task_state_journal
    ADD CONSTRAINT task_state_journal_pkey PRIMARY KEY (id);

-- Name: write_commit_journal uq_write_commit_fingerprint; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY write_commit_journal
    ADD CONSTRAINT uq_write_commit_fingerprint UNIQUE (commit_fingerprint);

-- Name: write_commit_journal write_commit_journal_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY write_commit_journal
    ADD CONSTRAINT write_commit_journal_pkey PRIMARY KEY (query_id);

-- Name: write_commit_quarantine write_commit_quarantine_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY write_commit_quarantine
    ADD CONSTRAINT write_commit_quarantine_pkey PRIMARY KEY (query_id, commit_fingerprint);

-- Name: copy_file_claims_execution; Type: INDEX; Schema: public; Owner: -

CREATE INDEX copy_file_claims_execution ON copy_file_claims USING btree (tenant_id, execution_id);

-- Name: copy_source_read_admissions_execution; Type: INDEX; Schema: public; Owner: -

CREATE INDEX copy_source_read_admissions_execution ON copy_source_read_admissions USING btree (tenant_id, execution_id);

-- Name: copy_source_read_admissions_input_attempt; Type: INDEX; Schema: public; Owner: -

CREATE INDEX copy_source_read_admissions_input_attempt ON copy_source_read_admissions USING btree (tenant_id, execution_id, staging_attempt_id);

-- Name: copy_source_read_admissions_root_attempt; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX copy_source_read_admissions_root_attempt ON copy_source_read_admissions USING btree (tenant_id, execution_id, staging_attempt_id) WHERE (parent_nonce_sha256 IS NULL);

-- Name: idx_execution_anchor_expiry; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_execution_anchor_expiry ON execution_anchor USING btree (expires_at, execution_id);

-- Name: idx_execution_anchor_owner; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_execution_anchor_owner ON execution_anchor USING btree (owner_id, owner_incarnation, expires_at);

-- Name: idx_qsj_owner; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_qsj_owner ON query_state_journal USING btree (coordinator_id);

-- Name: idx_qsj_query_state; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_qsj_query_state ON query_state_journal USING btree (query_id, state);

-- Name: idx_tsj_owner; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_tsj_owner ON task_state_journal USING btree (coordinator_id);

-- Name: idx_tsj_task_query; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_tsj_task_query ON task_state_journal USING btree (query_id, task_id);

-- Name: idx_write_commit_quarantine_table; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_write_commit_quarantine_table ON write_commit_quarantine USING btree (catalog_name, table_name);

-- Name: idx_write_commit_recovery; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_write_commit_recovery ON write_commit_journal USING btree (state, query_id, timestamp_ms);

-- Name: query_lineage_outbox_pending; Type: INDEX; Schema: public; Owner: -

CREATE INDEX query_lineage_outbox_pending ON query_lineage_outbox USING btree (created_at_ms, event_id) WHERE (delivered_at IS NULL);

-- Name: copy_file_claims copy_file_claims_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE OR REPLACE FUNCTION guard_copy_source_input_lineage()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE root copy_source_read_admissions%ROWTYPE;
BEGIN
    -- Hash collisions only serialize unrelated attempts; the immutable row
    -- comparison below still checks their full identities.
    PERFORM pg_advisory_xact_lock(207578818,
        hashtext(json_build_array(NEW.tenant_id,NEW.execution_id,NEW.staging_attempt_id)::text));
    IF EXISTS (SELECT 1 FROM copy_source_read_admissions a
            WHERE a.tenant_id=NEW.tenant_id AND a.execution_id=NEW.execution_id
                AND a.staging_attempt_id=NEW.staging_attempt_id
                AND (NEW.parent_nonce_sha256 IS NULL OR a.parent_nonce_sha256 IS NULL
                    OR a.parent_nonce_sha256 IS DISTINCT FROM NEW.parent_nonce_sha256
                    OR a.staging_command_sha256 IS DISTINCT FROM NEW.staging_command_sha256)) THEN
        RAISE EXCEPTION 'COPY task attempt is already bound to another input scope';
    END IF;
    IF NEW.parent_nonce_sha256 IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO root FROM copy_source_read_admissions WHERE nonce_sha256=NEW.parent_nonce_sha256;
    IF NOT FOUND OR root.parent_nonce_sha256 IS NOT NULL OR root.redeemed_at IS NULL
            OR root.staged_at IS NULL
            OR (NEW.tenant_id,NEW.principal,NEW.identity_groups,NEW.identity_attributes,
                NEW.authenticated_at,NEW.authorization_revision,NEW.execution_id,
                NEW.profile_name,NEW.profile_incarnation_id,NEW.profile_revision,
                NEW.canonical_source_url,NEW.managed_storage_root_uri,NEW.managed_backend_identity_sha256,
                NEW.staged_source_version_id,NEW.staged_managed_object_uri,NEW.staged_content_sha256,
                NEW.staged_content_size,NEW.staged_tenant_scope)
            IS DISTINCT FROM
               (root.tenant_id,root.principal,root.identity_groups,root.identity_attributes,
                root.authenticated_at,root.authorization_revision,root.execution_id,
                root.profile_name,root.profile_incarnation_id,root.profile_revision,
                root.canonical_source_url,root.managed_storage_root_uri,root.managed_backend_identity_sha256,
                root.staged_source_version_id,root.staged_managed_object_uri,root.staged_content_sha256,
                root.staged_content_size,root.staged_tenant_scope) THEN
        RAISE EXCEPTION 'COPY input must inherit one exact frozen root proof';
    END IF;
    RETURN NEW;
END;
$function$;
CREATE OR REPLACE FUNCTION guard_copy_source_read_admission_mutation()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'TRUNCATE' OR TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'COPY source read admissions cannot be deleted or truncated';
    END IF;
    IF (to_jsonb(NEW) - 'redeemed_at' - 'staged_source_version_id' - 'staged_managed_object_uri'
            - 'staged_content_sha256' - 'staged_content_size' - 'staged_tenant_scope' - 'staged_at')
        IS DISTINCT FROM (to_jsonb(OLD) - 'redeemed_at' - 'staged_source_version_id'
            - 'staged_managed_object_uri'
            - 'staged_content_sha256' - 'staged_content_size' - 'staged_tenant_scope' - 'staged_at')
            OR (OLD.redeemed_at IS NULL AND NEW.redeemed_at IS NULL)
            OR (OLD.redeemed_at IS NOT NULL AND NEW.redeemed_at IS DISTINCT FROM OLD.redeemed_at)
            OR (OLD.staged_at IS NOT NULL AND (
                NEW.staged_source_version_id IS DISTINCT FROM OLD.staged_source_version_id
                OR NEW.staged_managed_object_uri IS DISTINCT FROM OLD.staged_managed_object_uri
                OR NEW.staged_content_sha256 IS DISTINCT FROM OLD.staged_content_sha256
                OR NEW.staged_content_size IS DISTINCT FROM OLD.staged_content_size
                OR NEW.staged_tenant_scope IS DISTINCT FROM OLD.staged_tenant_scope
                OR NEW.staged_at IS DISTINCT FROM OLD.staged_at)) THEN
        RAISE EXCEPTION 'COPY source read admission identity is immutable';
    END IF;
    RETURN NEW;
END;
$function$;
CREATE OR REPLACE FUNCTION reject_copy_file_claim_mutation()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'COPY file claims are immutable; reconcile the Iceberg snapshot marker';
END;
$function$;
CREATE OR REPLACE FUNCTION reject_write_commit_journal_truncate()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'Write commit journal cannot be truncated';
END;
$function$;
CREATE OR REPLACE FUNCTION validate_copy_file_publication()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    intent write_commit_journal%ROWTYPE;
BEGIN
    SELECT * INTO intent FROM write_commit_journal
        WHERE query_id = NEW.execution_id FOR KEY SHARE;
    IF NOT FOUND OR intent.state NOT IN ('PENDING', 'COMMITTED')
            OR intent.copy_claim_key <> NEW.claim_key
            OR intent.copy_load_generation <> NEW.load_generation
            OR intent.copy_claim_digest <> NEW.claim_digest
            OR intent.copy_table_uuid <> NEW.table_uuid
            OR intent.tenant_id <> NEW.tenant_id
            OR intent.commit_fingerprint <> NEW.commit_fingerprint THEN
        RAISE EXCEPTION 'COPY publication differs from its durable write intent';
    END IF;
    RETURN NEW;
END;
$function$;
CREATE OR REPLACE FUNCTION validate_copy_write_journal_delete()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    IF OLD.copy_claim_key IS NOT NULL AND (
            OLD.state <> 'COMMITTED' OR NOT OLD.deferred_work_acknowledged
            OR NOT EXISTS (SELECT 1 FROM copy_file_publications p
                WHERE p.claim_key = OLD.copy_claim_key
                    AND p.load_generation = OLD.copy_load_generation
                    AND p.commit_fingerprint = OLD.commit_fingerprint)
            OR EXISTS (SELECT 1 FROM index_commit_log e
                WHERE e.commit_fingerprint = OLD.commit_fingerprint AND NOT e.processed)) THEN
        RAISE EXCEPTION 'COPY journal redo cannot be deleted before durable completion';
    END IF;
    RETURN OLD;
END;
$function$;
CREATE OR REPLACE FUNCTION validate_write_copy_binding()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    admitted copy_file_claims%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' AND (
            OLD.copy_claim_key IS DISTINCT FROM NEW.copy_claim_key
            OR OLD.copy_load_generation IS DISTINCT FROM NEW.copy_load_generation
            OR OLD.copy_claim_digest IS DISTINCT FROM NEW.copy_claim_digest
            OR OLD.copy_table_uuid IS DISTINCT FROM NEW.copy_table_uuid) THEN
        RAISE EXCEPTION 'COPY journal binding is immutable';
    END IF;
    IF NEW.copy_claim_key IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.state = 'ABORTED' THEN
        RAISE EXCEPTION 'COPY outcome cannot be declared aborted without catalog proof';
    END IF;
    IF TG_OP = 'UPDATE' AND (
            OLD.query_id IS DISTINCT FROM NEW.query_id
            OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
            OR OLD.commit_fingerprint IS DISTINCT FROM NEW.commit_fingerprint
            OR OLD.write_output_binding IS DISTINCT FROM NEW.write_output_binding) THEN
        RAISE EXCEPTION 'COPY write identity is immutable';
    END IF;
    SELECT * INTO admitted FROM copy_file_claims
        WHERE claim_key = NEW.copy_claim_key
            AND load_generation = NEW.copy_load_generation FOR KEY SHARE;
    IF NOT FOUND OR admitted.claim_digest <> NEW.copy_claim_digest
            OR admitted.execution_id <> NEW.query_id
            OR admitted.tenant_id <> NEW.tenant_id
            OR admitted.table_uuid <> NEW.copy_table_uuid THEN
        RAISE EXCEPTION 'COPY journal intent differs from durable file admission';
    END IF;
    IF NEW.state = 'COMMITTED' AND NOT EXISTS (
        SELECT 1 FROM copy_file_publications p
        WHERE p.claim_key = NEW.copy_claim_key
            AND p.load_generation = NEW.copy_load_generation
            AND p.commit_fingerprint = NEW.commit_fingerprint
            AND p.claim_digest = NEW.copy_claim_digest) THEN
        RAISE EXCEPTION 'COPY journal cannot commit before durable publication proof';
    END IF;
    RETURN NEW;
END;
$function$;
CREATE TRIGGER copy_file_claims_no_truncate BEFORE TRUNCATE ON copy_file_claims FOR EACH STATEMENT EXECUTE FUNCTION reject_copy_file_claim_mutation();

-- Name: copy_file_claims copy_file_claims_no_update_or_delete; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_file_claims_no_update_or_delete BEFORE DELETE OR UPDATE ON copy_file_claims FOR EACH ROW EXECUTE FUNCTION reject_copy_file_claim_mutation();

-- Name: copy_file_publications copy_file_publication_guard; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_file_publication_guard BEFORE INSERT ON copy_file_publications FOR EACH ROW EXECUTE FUNCTION validate_copy_file_publication();

-- Name: copy_file_publications copy_file_publications_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_file_publications_no_truncate BEFORE TRUNCATE ON copy_file_publications FOR EACH STATEMENT EXECUTE FUNCTION reject_copy_file_claim_mutation();

-- Name: copy_file_publications copy_file_publications_no_update_or_delete; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_file_publications_no_update_or_delete BEFORE DELETE OR UPDATE ON copy_file_publications FOR EACH ROW EXECUTE FUNCTION reject_copy_file_claim_mutation();

-- Name: copy_source_read_admissions copy_source_read_admissions_lineage_guard; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_source_read_admissions_lineage_guard BEFORE INSERT ON copy_source_read_admissions FOR EACH ROW EXECUTE FUNCTION guard_copy_source_input_lineage();

-- Name: copy_source_read_admissions copy_source_read_admissions_no_delete; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_source_read_admissions_no_delete BEFORE DELETE ON copy_source_read_admissions FOR EACH ROW EXECUTE FUNCTION guard_copy_source_read_admission_mutation();

-- Name: copy_source_read_admissions copy_source_read_admissions_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_source_read_admissions_no_truncate BEFORE TRUNCATE ON copy_source_read_admissions FOR EACH STATEMENT EXECUTE FUNCTION guard_copy_source_read_admission_mutation();

-- Name: copy_source_read_admissions copy_source_read_admissions_update_guard; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER copy_source_read_admissions_update_guard BEFORE UPDATE ON copy_source_read_admissions FOR EACH ROW EXECUTE FUNCTION guard_copy_source_read_admission_mutation();

-- Name: write_commit_journal write_commit_copy_binding_guard; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER write_commit_copy_binding_guard BEFORE INSERT OR UPDATE ON write_commit_journal FOR EACH ROW EXECUTE FUNCTION validate_write_copy_binding();

-- Name: write_commit_journal write_commit_copy_delete_guard; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER write_commit_copy_delete_guard BEFORE DELETE ON write_commit_journal FOR EACH ROW EXECUTE FUNCTION validate_copy_write_journal_delete();

-- Name: write_commit_journal write_commit_journal_no_truncate; Type: TRIGGER; Schema: public; Owner: -

CREATE TRIGGER write_commit_journal_no_truncate BEFORE TRUNCATE ON write_commit_journal FOR EACH STATEMENT EXECUTE FUNCTION reject_write_commit_journal_truncate();

-- Name: copy_file_publications copy_file_publications_claim_key_load_generation_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY copy_file_publications
    ADD CONSTRAINT copy_file_publications_claim_key_load_generation_fkey FOREIGN KEY (claim_key, load_generation) REFERENCES copy_file_claims(claim_key, load_generation);

-- Name: copy_source_read_admissions copy_source_read_admissions_parent_nonce_sha256_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY copy_source_read_admissions
    ADD CONSTRAINT copy_source_read_admissions_parent_nonce_sha256_fkey FOREIGN KEY (parent_nonce_sha256) REFERENCES copy_source_read_admissions(nonce_sha256);

-- Name: copy_source_read_admissions copy_source_read_admissions_profile_name_profile_incarnati_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY copy_source_read_admissions
    ADD CONSTRAINT copy_source_read_admissions_profile_name_profile_incarnati_fkey FOREIGN KEY (profile_name, profile_incarnation_id, profile_revision) REFERENCES storage_profile_revisions(name, incarnation_id, revision);

-- Name: write_commit_journal fk_write_copy_claim; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY write_commit_journal
    ADD CONSTRAINT fk_write_copy_claim FOREIGN KEY (copy_claim_key, copy_load_generation) REFERENCES copy_file_claims(claim_key, load_generation);

