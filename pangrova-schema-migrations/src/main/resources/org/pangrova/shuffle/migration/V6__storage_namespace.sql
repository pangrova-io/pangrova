CREATE TABLE shuffle_storage_namespace (
    singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
    bucket VARCHAR(512) NOT NULL,
    prefix VARCHAR(2048) NOT NULL,
    region VARCHAR(256) NOT NULL,
    endpoint VARCHAR(2048) NOT NULL
);
