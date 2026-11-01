-- streaming baseline: single migration owning the streaming durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: ingestion stream state/offsets, cdc_commit_state, watermark_positions.

-- PostgreSQL database dump

-- Name: cdc_commit_state; Type: TABLE; Schema: public; Owner: -

CREATE TABLE cdc_commit_state (
    id character varying(255) NOT NULL,
    source_id character varying(255) NOT NULL,
    partition_num integer,
    start_offset bigint,
    end_offset bigint,
    state character varying(32) NOT NULL,
    deadline timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL,
    batch_key character varying(255),
    offset_ranges jsonb
);

-- Name: ingestion_stream_offsets; Type: TABLE; Schema: public; Owner: -

CREATE TABLE ingestion_stream_offsets (
    stream_name character varying(255) NOT NULL,
    topic character varying(512) NOT NULL,
    partition_id integer NOT NULL,
    committed_offset bigint NOT NULL,
    updated_at timestamp without time zone NOT NULL
);

-- Name: ingestion_stream_state; Type: TABLE; Schema: public; Owner: -

CREATE TABLE ingestion_stream_state (
    stream_name character varying(255) NOT NULL,
    tenant_id character varying(255) NOT NULL,
    lifecycle_state character varying(32) NOT NULL,
    target_table character varying(512) NOT NULL,
    source_properties text NOT NULL,
    source_format character varying(64) NOT NULL,
    stream_properties text,
    creator_identity character varying(255),
    coordinator_id character varying(255),
    epoch bigint DEFAULT 0 NOT NULL,
    schema_version integer DEFAULT 0 NOT NULL,
    error_message text,
    records_consumed bigint DEFAULT 0 NOT NULL,
    records_written bigint DEFAULT 0 NOT NULL,
    records_dead_lettered bigint DEFAULT 0 NOT NULL,
    last_offset_commit_time timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    checkpoint_id character varying(255) NOT NULL
);

-- Name: watermark_positions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE watermark_positions (
    source_id character varying(512) NOT NULL,
    "position" timestamp without time zone NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Name: cdc_commit_state cdc_commit_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY cdc_commit_state
    ADD CONSTRAINT cdc_commit_state_pkey PRIMARY KEY (id);

-- Name: cdc_commit_state cdc_complete_checkpoint; Type: CHECK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE cdc_commit_state
    ADD CONSTRAINT cdc_complete_checkpoint CHECK (((offset_ranges IS NOT NULL) AND (jsonb_typeof(offset_ranges) = 'array'::text) AND (jsonb_array_length(offset_ranges) > 0))) NOT VALID;

-- Name: ingestion_stream_offsets ingestion_stream_offsets_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY ingestion_stream_offsets
    ADD CONSTRAINT ingestion_stream_offsets_pkey PRIMARY KEY (stream_name, topic, partition_id);

-- Name: ingestion_stream_state ingestion_stream_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY ingestion_stream_state
    ADD CONSTRAINT ingestion_stream_state_pkey PRIMARY KEY (stream_name);

-- Name: ingestion_stream_state stream_checkpoint_identity_unique; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY ingestion_stream_state
    ADD CONSTRAINT stream_checkpoint_identity_unique UNIQUE (checkpoint_id);

-- Name: watermark_positions watermark_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY watermark_positions
    ADD CONSTRAINT watermark_positions_pkey PRIMARY KEY (source_id);

-- Name: idx_cdc_commit_state_dedup; Type: INDEX; Schema: public; Owner: -

CREATE UNIQUE INDEX idx_cdc_commit_state_dedup ON cdc_commit_state USING btree (source_id, batch_key) WHERE ((state)::text <> 'ROLLED_BACK'::text);

-- Name: idx_cdc_commit_state_source_state; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_cdc_commit_state_source_state ON cdc_commit_state USING btree (source_id, state);

-- Name: idx_stream_state; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_stream_state ON ingestion_stream_state USING btree (lifecycle_state);

-- Name: idx_stream_tenant; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_stream_tenant ON ingestion_stream_state USING btree (tenant_id);

-- Name: ingestion_stream_offsets ingestion_stream_offsets_stream_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY ingestion_stream_offsets
    ADD CONSTRAINT ingestion_stream_offsets_stream_name_fkey FOREIGN KEY (stream_name) REFERENCES ingestion_stream_state(stream_name) ON DELETE CASCADE;

