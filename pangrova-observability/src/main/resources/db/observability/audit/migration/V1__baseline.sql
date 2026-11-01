-- observability.audit baseline: single migration owning the observability.audit durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: query_audit_records.

-- PostgreSQL database dump

-- Name: query_audit_records; Type: TABLE; Schema: public; Owner: -

CREATE TABLE query_audit_records (
    query_id character varying(64) NOT NULL,
    user_id character varying(255),
    sql_text text,
    tenant_id character varying(255),
    stage_count integer DEFAULT 0 NOT NULL,
    task_count integer DEFAULT 0 NOT NULL,
    cpu_time_ms bigint DEFAULT 0 NOT NULL,
    wall_time_ms bigint DEFAULT 0 NOT NULL,
    peak_memory_bytes bigint DEFAULT 0 NOT NULL,
    spilled_bytes bigint DEFAULT 0 NOT NULL,
    input_rows bigint DEFAULT 0 NOT NULL,
    output_rows bigint DEFAULT 0 NOT NULL,
    input_bytes bigint DEFAULT 0 NOT NULL,
    output_bytes bigint DEFAULT 0 NOT NULL,
    shuffle_read_bytes bigint DEFAULT 0 NOT NULL,
    shuffle_write_bytes bigint DEFAULT 0 NOT NULL,
    completed_at timestamp without time zone,
    success boolean DEFAULT false NOT NULL
);

-- Name: query_audit_records query_audit_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY query_audit_records
    ADD CONSTRAINT query_audit_records_pkey PRIMARY KEY (query_id);

-- Name: idx_qar_completed_at; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_qar_completed_at ON query_audit_records USING btree (completed_at);

-- Name: idx_qar_tenant_id; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_qar_tenant_id ON query_audit_records USING btree (tenant_id);

-- Name: idx_qar_user_id; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_qar_user_id ON query_audit_records USING btree (user_id);

