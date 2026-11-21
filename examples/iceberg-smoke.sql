-- Run once against an isolated local standalone Pangrova instance.
-- Expected: click | 2, purchase | 1. See Quickstart for separate cleanup.

CREATE CATALOG local_iceberg
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = 'file:///tmp/iceberg-warehouse'
);

CREATE SCHEMA local_iceberg.demo;

CREATE TABLE local_iceberg.demo.events (
  event_id BIGINT,
  event_type VARCHAR,
  user_id VARCHAR,
  event_time TIMESTAMP,
  payload VARCHAR
) PARTITIONED BY (event_type);

INSERT INTO local_iceberg.demo.events VALUES
  (1, 'click', 'user-001', TIMESTAMP '2024-01-15 10:30:00', '{"page": "/home"}'),
  (2, 'purchase', 'user-002', TIMESTAMP '2024-01-15 11:00:00', '{"item": "widget"}'),
  (3, 'click', 'user-001', TIMESTAMP '2024-01-15 11:30:00', '{"page": "/products"}');

SELECT event_type, COUNT(*) AS cnt
FROM local_iceberg.demo.events
GROUP BY event_type
ORDER BY cnt DESC;
