-- Pangrova CLI end-to-end workflow.
-- Run against an isolated Standalone instance with catalog/table/index privileges,
-- indexing enabled, and local governed FILE storage configured.
-- Interactive replay sends each statement separately:
--   bin/pangrova-cli.sh --host localhost --port 8080 < examples/cli-workflow.sql

-- Core SQL: catalog, schema, table, insert, and queries
-- Create catalog
CREATE CATALOG pangrova_final_demo2
USING iceberg
WITH (
  'type' = 'hadoop',
  'warehouse' = '/tmp/pangrova-cli-warehouse'
);

-- Create schema
CREATE SCHEMA pangrova_final_demo2.analytics;

-- Create events table
CREATE TABLE pangrova_final_demo2.analytics.events (
  event_id BIGINT,
  event_type VARCHAR,
  region VARCHAR,
  user_id VARCHAR,
  event_time TIMESTAMP
) PARTITIONED BY (event_type);

-- Insert six rows
INSERT INTO pangrova_final_demo2.analytics.events VALUES
  (1, 'click', 'east', 'user-001', TIMESTAMP '2026-11-26 09:30:00'),
  (2, 'purchase', 'west', 'user-002', TIMESTAMP '2026-11-26 10:00:00'),
  (3, 'click', 'east', 'user-003', TIMESTAMP '2026-11-26 10:15:00'),
  (4, 'search', 'north', 'user-004', TIMESTAMP '2026-11-26 11:00:00'),
  (5, 'purchase', 'west', 'user-005', TIMESTAMP '2026-11-26 11:30:00'),
  (6, 'click', 'south', 'user-006', TIMESTAMP '2026-11-26 12:00:00');

-- Describe table
DESCRIBE TABLE pangrova_final_demo2.analytics.events;

-- Query details
SELECT event_id, event_type, region, event_time
FROM pangrova_final_demo2.analytics.events
ORDER BY event_id;

-- Aggregate by region
SELECT region, COUNT(*) AS event_count
FROM pangrova_final_demo2.analytics.events
GROUP BY region
ORDER BY region;

-- Aggregate by type
SELECT event_type, COUNT(*) AS event_count
FROM pangrova_final_demo2.analytics.events
GROUP BY event_type
ORDER BY event_count DESC, event_type;


-- Governed multimodal FILE references
-- Create documents table
CREATE TABLE pangrova_final_demo2.analytics.documents (
  id BIGINT,
  title VARCHAR,
  attachment FILE
);

-- Publish three modalities
INSERT INTO pangrova_final_demo2.analytics.documents VALUES
  (1, 'Pixel image', FILE_FROM_BYTES(X'89504e470d0a1a0a0000000d4948445200000001000000010804000000b51c0c020000000b4944415478da63fcff1f0002eb01f569769d770000000049454e44ae426082', 'image/png')),
  (2, 'Plain text', FILE_FROM_BYTES(X'68656c6c6f2070616e67726f7661', 'text/plain')),
  (3, 'PDF document', FILE_FROM_BYTES(X'255044462d312e340a312030206f626a3c3c2f547970652f436174616c6f673e3e656e646f626a0a747261696c65723c3c2f526f6f742031203020523e3e0a2525454f460a', 'application/pdf'));

-- Read FILE metadata
SELECT id, title, attachment.mime_type AS mime, attachment.size AS bytes
FROM pangrova_final_demo2.analytics.documents
ORDER BY id;

-- Aggregate modalities
SELECT attachment.mime_type AS mime, COUNT(*) AS documents
FROM pangrova_final_demo2.analytics.documents
GROUP BY attachment.mime_type
ORDER BY mime;


-- Scalar and full-text indexes
-- Create index lab
CREATE TABLE pangrova_final_demo2.analytics.index_lab_v4 (
  user_id BIGINT, name VARCHAR, amount DOUBLE, status VARCHAR,
  description VARCHAR, session_id VARCHAR
);

-- Load batch one
INSERT INTO pangrova_final_demo2.analytics.index_lab_v4 VALUES
  (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001'),
  (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002');

-- Load batch two
INSERT INTO pangrova_final_demo2.analytics.index_lab_v4 VALUES
  (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010'),
  (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020');

-- Load batch three
INSERT INTO pangrova_final_demo2.analytics.index_lab_v4 VALUES
  (100, 'Eve', 250.0, 'ACTIVE', 'distributed sql', 'sess-100'),
  (200, 'Frank', 275.0, 'PENDING', 'lakehouse format', 'sess-200');

-- Load batch four
INSERT INTO pangrova_final_demo2.analytics.index_lab_v4 VALUES
  (1000, 'Grace', 350.0, 'ACTIVE', 'iceberg table', 'sess-1000'),
  (2000, 'Hank', 375.0, 'INACTIVE', 'paimon store', 'sess-2000');

-- Create Bloom filter
CREATE INDEX idx_v4_bloom ON pangrova_final_demo2.analytics.index_lab_v4 (user_id) USING BLOOM_FILTER;

-- Create B-tree
CREATE INDEX idx_v4_btree ON pangrova_final_demo2.analytics.index_lab_v4 (amount) USING BTREE FORCE;

-- Create bitmap
CREATE INDEX idx_v4_bitmap ON pangrova_final_demo2.analytics.index_lab_v4 (status) USING BITMAP;

-- Create hash
CREATE INDEX idx_v4_hash ON pangrova_final_demo2.analytics.index_lab_v4 (session_id) USING HASH;

-- Create sparse
CREATE INDEX idx_v4_sparse ON pangrova_final_demo2.analytics.index_lab_v4 (user_id) USING SPARSE FORCE;

-- Create prefix
CREATE INDEX idx_v4_prefix ON pangrova_final_demo2.analytics.index_lab_v4 (name) USING PREFIX;

-- Create n-gram Bloom
CREATE INDEX idx_v4_ngram ON pangrova_final_demo2.analytics.index_lab_v4 (name) USING NGRAM_BLOOM_FILTER;

-- Create inverted text
CREATE INDEX idx_v4_inverted ON pangrova_final_demo2.analytics.index_lab_v4 (description) USING INVERTED;

-- Inspect index descriptors. Repeat until all are READY before running the predicates.
SHOW INDEXES FROM pangrova_final_demo2.analytics.index_lab_v4;

-- Range query
SELECT name, amount FROM pangrova_final_demo2.analytics.index_lab_v4 WHERE amount BETWEEN 100.0 AND 200.0 ORDER BY amount;

-- Low-cardinality query
SELECT name, status FROM pangrova_final_demo2.analytics.index_lab_v4 WHERE status = 'ACTIVE' ORDER BY name;

-- Prefix query
SELECT name FROM pangrova_final_demo2.analytics.index_lab_v4 WHERE name LIKE 'Ali%';

-- Substring query
SELECT name FROM pangrova_final_demo2.analytics.index_lab_v4 WHERE name LIKE '%lic%';

-- Full-text filter
SELECT name FROM pangrova_final_demo2.analytics.index_lab_v4 WHERE MATCH(description, 'pangrova');


-- Containment, spatial, and vector indexes
-- Create containment lab
CREATE TABLE pangrova_final_demo2.analytics.gin_lab_v4 (id BIGINT, tags VARCHAR, data VARCHAR);

-- Load containment rows
INSERT INTO pangrova_final_demo2.analytics.gin_lab_v4 VALUES
  (1, 'alpha,beta', 'd1'), (2, 'beta,alpha', 'd2'),
  (3, 'gamma,delta', 'd3'), (4, 'delta,gamma', 'd4');

-- Create GIN
CREATE INDEX idx_v4_gin ON pangrova_final_demo2.analytics.gin_lab_v4 (tags) USING GIN;

-- Inspect GIN descriptor; wait until READY before running the query.
SHOW INDEXES FROM pangrova_final_demo2.analytics.gin_lab_v4;

-- Text predicate (not evidence that GIN accelerated the query)
SELECT id, tags FROM pangrova_final_demo2.analytics.gin_lab_v4 WHERE tags LIKE '%alpha%' ORDER BY id;

-- Create spatial lab
CREATE TABLE pangrova_final_demo2.analytics.spatial_lab_v4 (id BIGINT, x DOUBLE, y DOUBLE, label VARCHAR);

-- Load spatial rows
INSERT INTO pangrova_final_demo2.analytics.spatial_lab_v4 VALUES
  (1, 2.0, 3.0, 'q1_a'), (2, 8.0, 7.0, 'q1_b'),
  (3, 102.0, 105.0, 'q2_a'), (4, 108.0, 109.0, 'q2_b');

-- Create R-Tree
CREATE INDEX idx_v4_rtree ON pangrova_final_demo2.analytics.spatial_lab_v4 (x, y) USING R_TREE;

-- Inspect R-Tree descriptor; wait until READY before running the query.
SHOW INDEXES FROM pangrova_final_demo2.analytics.spatial_lab_v4;

-- Spatial query
SELECT id, label FROM pangrova_final_demo2.analytics.spatial_lab_v4
WHERE x BETWEEN 0.0 AND 10.0 AND y BETWEEN 0.0 AND 10.0
ORDER BY id;

-- Create vector lab
CREATE TABLE pangrova_final_demo2.analytics.vector_lab_v4 (id BIGINT, embedding VECTOR(4), label VARCHAR);

-- Load vectors
INSERT INTO pangrova_final_demo2.analytics.vector_lab_v4 VALUES
  (1, ARRAY[1.0, 0.0, 0.0, 0.0], 'north'),
  (2, ARRAY[0.0, 1.0, 0.0, 0.0], 'east'),
  (3, ARRAY[0.0, 0.0, 1.0, 0.0], 'south'),
  (4, ARRAY[0.0, 0.0, 0.0, 1.0], 'west'),
  (5, ARRAY[0.5, 0.5, 0.0, 0.0], 'northeast'),
  (6, ARRAY[0.0, 0.5, 0.5, 0.0], 'southeast');

-- Create HNSW
CREATE INDEX idx_v4_hnsw ON pangrova_final_demo2.analytics.vector_lab_v4 (embedding)
USING VECTOR WITH ('algorithm'='HNSW', 'metric'='L2');

-- Create IVF-PQ
CREATE INDEX idx_v4_ivfpq ON pangrova_final_demo2.analytics.vector_lab_v4 (embedding)
USING VECTOR WITH ('algorithm'='IVFPQ', 'metric'='L2', 'ivfpq.nlist'='2', 'ivfpq.m'='2', 'ivfpq.nbits'='8');

-- Inspect vector descriptors. Repeat until both are READY before the query.
SHOW INDEXES FROM pangrova_final_demo2.analytics.vector_lab_v4;

-- Nearest neighbors
SELECT id, label, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS distance
FROM pangrova_final_demo2.analytics.vector_lab_v4
ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 3 OFFSET 1;


-- Joins, window functions, CTEs, and maintenance
-- Create customers
CREATE TABLE pangrova_final_demo2.analytics.customers (customer_id BIGINT, customer_name VARCHAR, region VARCHAR);

-- Load customers
INSERT INTO pangrova_final_demo2.analytics.customers VALUES
  (1, 'Alice', 'east'), (2, 'Bob', 'west'), (3, 'Carol', 'north');

-- Create orders
CREATE TABLE pangrova_final_demo2.analytics.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR);

-- Load orders
INSERT INTO pangrova_final_demo2.analytics.orders VALUES
  (101, 1, 120.0, 'PAID'), (102, 1, 80.0, 'PAID'),
  (103, 2, 250.0, 'PAID'), (104, 2, 50.0, 'PENDING'),
  (105, 3, 175.0, 'PAID');

-- Join and aggregate
SELECT c.customer_name, c.region, COUNT(o.order_id) AS orders, SUM(o.amount) AS total
FROM pangrova_final_demo2.analytics.customers c
JOIN pangrova_final_demo2.analytics.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.region
ORDER BY total DESC;

-- Window ranking
SELECT order_id, customer_id, amount,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rank
FROM pangrova_final_demo2.analytics.orders
ORDER BY customer_id, rank;

-- CTE aggregation
WITH regional AS (
  SELECT c.region, SUM(o.amount) AS total
  FROM pangrova_final_demo2.analytics.customers c
  JOIN pangrova_final_demo2.analytics.orders o ON c.customer_id = o.customer_id
  GROUP BY c.region
)
SELECT region, total FROM regional WHERE total > 200 ORDER BY total DESC;

-- Compact table
COMPACT pangrova_final_demo2.analytics.orders;

-- Vacuum table
VACUUM pangrova_final_demo2.analytics.orders;

-- Final row count
SELECT COUNT(*) AS orders_after_maintenance FROM pangrova_final_demo2.analytics.orders;

-- Optional cleanup:
-- DROP TABLE pangrova_final_demo2.analytics.orders;
-- DROP TABLE pangrova_final_demo2.analytics.customers;
-- DROP TABLE pangrova_final_demo2.analytics.vector_lab_v4;
-- DROP TABLE pangrova_final_demo2.analytics.spatial_lab_v4;
-- DROP TABLE pangrova_final_demo2.analytics.gin_lab_v4;
-- DROP TABLE pangrova_final_demo2.analytics.index_lab_v4;
-- DROP TABLE pangrova_final_demo2.analytics.documents;
-- DROP TABLE pangrova_final_demo2.analytics.events;
-- DROP SCHEMA pangrova_final_demo2.analytics;
-- DROP CATALOG pangrova_final_demo2;
