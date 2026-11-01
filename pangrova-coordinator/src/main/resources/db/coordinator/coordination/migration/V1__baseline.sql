-- coordinator.coordination baseline: single migration owning the coordinator.coordination durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: coordinator_registry, worker_registry, distributed_lease.

-- PostgreSQL database dump

-- Name: coordinator_registry; Type: TABLE; Schema: public; Owner: -

CREATE TABLE coordinator_registry (
    coordinator_id character varying(128) NOT NULL,
    host character varying(256) NOT NULL,
    http_port integer NOT NULL,
    role character varying(32) DEFAULT 'STANDBY'::character varying NOT NULL,
    epoch bigint DEFAULT 0 NOT NULL,
    last_heartbeat_ms bigint NOT NULL,
    registered_at_ms bigint NOT NULL,
    status character varying(32) DEFAULT 'ONLINE'::character varying NOT NULL,
    last_processed_invalidation_id bigint,
    capabilities text,
    version bigint DEFAULT 0 NOT NULL
);

-- Name: distributed_lease; Type: TABLE; Schema: public; Owner: -

CREATE TABLE distributed_lease (
    lease_key character varying(255) NOT NULL,
    holder_id character varying(255) NOT NULL,
    epoch bigint DEFAULT 0 NOT NULL,
    acquired_at timestamp(6) with time zone NOT NULL,
    expires_at timestamp(6) with time zone NOT NULL,
    version bigint DEFAULT 0 NOT NULL
);

-- Name: worker_registry; Type: TABLE; Schema: public; Owner: -

CREATE TABLE worker_registry (
    worker_id character varying(128) NOT NULL,
    host character varying(256) NOT NULL,
    http_port integer NOT NULL,
    transport_port integer NOT NULL,
    protocol_version integer DEFAULT 2 NOT NULL,
    rack_id character varying(128) DEFAULT NULL::character varying,
    data_locality text,
    memory_pressure character varying(32) DEFAULT 'NONE'::character varying NOT NULL,
    cpu_utilization double precision DEFAULT 0.0 NOT NULL,
    running_task_count integer DEFAULT 0 NOT NULL,
    available_memory_bytes bigint DEFAULT 0 NOT NULL,
    last_heartbeat_ms bigint NOT NULL,
    registered_at_ms bigint NOT NULL,
    result_server_port integer DEFAULT 0 NOT NULL,
    capability_inventory_proto text NOT NULL,
    copy_task_tls_port integer DEFAULT 0 NOT NULL,
    admission_domain_id character varying(36),
    incarnation_id character varying(36),
    CONSTRAINT chk_worker_copy_task_tls_port CHECK (((copy_task_tls_port >= 0) AND (copy_task_tls_port <= 65535))),
    CONSTRAINT chk_worker_registry_protocol_v2 CHECK ((protocol_version = 2))
);

-- Name: coordinator_registry coordinator_registry_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY coordinator_registry
    ADD CONSTRAINT coordinator_registry_pkey PRIMARY KEY (coordinator_id);

-- Name: distributed_lease distributed_lease_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY distributed_lease
    ADD CONSTRAINT distributed_lease_pkey PRIMARY KEY (lease_key);

-- Name: worker_registry worker_registry_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY worker_registry
    ADD CONSTRAINT worker_registry_pkey PRIMARY KEY (worker_id);

-- Name: idx_cr_heartbeat; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_cr_heartbeat ON coordinator_registry USING btree (last_heartbeat_ms);

-- Name: idx_cr_role; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_cr_role ON coordinator_registry USING btree (role);

-- Name: idx_dl_expires; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_dl_expires ON distributed_lease USING btree (expires_at);

-- Name: idx_wr_heartbeat; Type: INDEX; Schema: public; Owner: -

CREATE INDEX idx_wr_heartbeat ON worker_registry USING btree (last_heartbeat_ms);

