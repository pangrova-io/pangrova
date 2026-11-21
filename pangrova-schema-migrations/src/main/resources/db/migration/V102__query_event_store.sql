CREATE TABLE IF NOT EXISTS query_events (
    query_id        VARCHAR(128) PRIMARY KEY,
    sql_text        TEXT,
    principal       VARCHAR(256),
    final_state     VARCHAR(32) NOT NULL,
    create_time     TIMESTAMP,
    end_time        TIMESTAMP,
    wall_time_ms    BIGINT,
    cpu_time_ms     BIGINT,
    total_rows      BIGINT,
    total_bytes     BIGINT,
    failure_message TEXT,
    success         BOOLEAN
);

CREATE TABLE IF NOT EXISTS query_split_profiles (
    query_id            VARCHAR(128) NOT NULL,
    stage_id            VARCHAR(128) NOT NULL,
    split_id            VARCHAR(256) NOT NULL,
    catalog_name        VARCHAR(256),
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_ms         BIGINT,
    processed_rows      BIGINT,
    processed_bytes     BIGINT,
    failure_message     TEXT,
    success             BOOLEAN,
    PRIMARY KEY (query_id, stage_id, split_id)
);
