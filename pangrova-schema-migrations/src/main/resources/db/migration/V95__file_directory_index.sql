-- Current directory observations, separate from the namespace registry and file bytes.
-- An incomplete refresh never modifies published observations.
CREATE TABLE file_directory_index (
    file_space_id UUID NOT NULL REFERENCES file_space_registry(file_space_id) ON DELETE CASCADE,
    relative_path TEXT COLLATE "C" NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    last_modified TIMESTAMPTZ NOT NULL,
    etag TEXT,
    content_type TEXT,
    provider_version TEXT,
    md5 TEXT,
    PRIMARY KEY (file_space_id, relative_path)
);

CREATE TABLE file_directory_refresh (
    file_space_id UUID PRIMARY KEY REFERENCES file_space_registry(file_space_id) ON DELETE CASCADE,
    refresh_id UUID NOT NULL,
    space_updated_at TIMESTAMPTZ NOT NULL,
    subpath TEXT NOT NULL,
    next_page_token TEXT,
    page_number BIGINT NOT NULL DEFAULT 0,
    scan_complete BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE file_directory_pending (
    file_space_id UUID NOT NULL REFERENCES file_directory_refresh(file_space_id) ON DELETE CASCADE,
    relative_path TEXT COLLATE "C" NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    last_modified TIMESTAMPTZ NOT NULL,
    etag TEXT,
    content_type TEXT,
    provider_version TEXT,
    md5 TEXT,
    PRIMARY KEY (file_space_id, relative_path)
);
