-- Run against an isolated Pangrova Standalone instance with catalog and table privileges.
-- Use interactive mode so each statement is executed separately:
--   bin/pangrova-cli.sh --host localhost --port 8080 < examples/cli-workflow.sql

CREATE CATALOG demo_iceberg
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = 'file:///tmp/pangrova-cli-warehouse'
);

CREATE SCHEMA demo_iceberg.analytics;

CREATE TABLE demo_iceberg.analytics.events (
  event_id BIGINT,
  event_type VARCHAR,
  region VARCHAR,
  user_id VARCHAR,
  event_time TIMESTAMP
) PARTITIONED BY (event_type);

DESCRIBE TABLE demo_iceberg.analytics.events;

INSERT INTO demo_iceberg.analytics.events VALUES
  (1, 'click',    'east',  'user-001', TIMESTAMP '2026-11-26 09:30:00'),
  (2, 'purchase', 'west',  'user-002', TIMESTAMP '2026-11-26 10:00:00'),
  (3, 'click',    'east',  'user-003', TIMESTAMP '2026-11-26 10:15:00'),
  (4, 'search',   'north', 'user-004', TIMESTAMP '2026-11-26 11:00:00'),
  (5, 'purchase', 'west',  'user-005', TIMESTAMP '2026-11-26 11:30:00'),
  (6, 'click',    'south', 'user-006', TIMESTAMP '2026-11-26 12:00:00');

SELECT event_id, event_type, region, event_time
FROM demo_iceberg.analytics.events
ORDER BY event_id;

SELECT region, COUNT(*) AS event_count
FROM demo_iceberg.analytics.events
GROUP BY region
ORDER BY region;

SELECT event_type, COUNT(*) AS event_count
FROM demo_iceberg.analytics.events
GROUP BY event_type
ORDER BY event_count DESC, event_type;

-- Optional cleanup:
-- DROP TABLE demo_iceberg.analytics.events;
-- DROP CATALOG demo_iceberg;
