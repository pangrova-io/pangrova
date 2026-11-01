-- semantic baseline: single migration owning the semantic durable schema.
-- Clean-cut shape folded from the final V1-V101 chain state (2026-11-01).
-- No legacy columns, compatibility views, dual-write tables or pre-split history.
-- Tables: semantic_view_versions, semantic_view_active_versions.

-- PostgreSQL database dump

-- Name: semantic_view_active_versions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE semantic_view_active_versions (
    tenant_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    view_name character varying(255) NOT NULL,
    active_version bigint NOT NULL,
    publication_epoch bigint NOT NULL,
    published_by character varying(512) NOT NULL,
    published_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_semantic_view_publication_epoch CHECK ((publication_epoch > 0))
);

-- Name: semantic_view_versions; Type: TABLE; Schema: public; Owner: -

CREATE TABLE semantic_view_versions (
    tenant_id character varying(255) NOT NULL,
    catalog_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    view_name character varying(255) NOT NULL,
    version bigint NOT NULL,
    owner_principal character varying(512) NOT NULL,
    definition_json jsonb NOT NULL,
    definition_sha256 character varying(64) NOT NULL,
    validation_state character varying(16) NOT NULL,
    validation_message character varying(2048),
    created_at timestamp(6) with time zone NOT NULL,
    CONSTRAINT chk_semantic_view_sha256 CHECK (((definition_sha256)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT chk_semantic_view_validation CHECK (((validation_state)::text = ANY ((ARRAY['VALIDATED'::character varying, 'INVALID'::character varying])::text[]))),
    CONSTRAINT chk_semantic_view_validation_message CHECK (((((validation_state)::text = 'VALIDATED'::text) AND (validation_message IS NULL)) OR (((validation_state)::text = 'INVALID'::text) AND (validation_message IS NOT NULL)))),
    CONSTRAINT chk_semantic_view_version CHECK ((version > 0))
);

-- Name: semantic_view_active_versions semantic_view_active_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY semantic_view_active_versions
    ADD CONSTRAINT semantic_view_active_versions_pkey PRIMARY KEY (tenant_id, catalog_name, schema_name, view_name);

-- Name: semantic_view_versions semantic_view_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY semantic_view_versions
    ADD CONSTRAINT semantic_view_versions_pkey PRIMARY KEY (tenant_id, catalog_name, schema_name, view_name, version);

-- Name: semantic_view_versions uq_semantic_view_definition; Type: CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY semantic_view_versions
    ADD CONSTRAINT uq_semantic_view_definition UNIQUE (tenant_id, catalog_name, schema_name, view_name, definition_sha256);

-- Name: semantic_view_active_versions fk_semantic_view_active_version; Type: FK CONSTRAINT; Schema: public; Owner: -

ALTER TABLE ONLY semantic_view_active_versions
    ADD CONSTRAINT fk_semantic_view_active_version FOREIGN KEY (tenant_id, catalog_name, schema_name, view_name, active_version) REFERENCES semantic_view_versions(tenant_id, catalog_name, schema_name, view_name, version);

