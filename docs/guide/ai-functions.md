Pangrova > Guide > AI Functions

# AI Functions

Pangrova is an AI-native distributed SQL query engine. AI capabilities are exposed as standard SQL functions, enabling seamless integration of LLM inference, embedding generation, and multimodal processing directly within SQL queries.

> **Design Philosophy**: Every AI function follows the `(model, input...) → output` calling convention. Models are referenced by name (registered via `CREATE EMBEDDING MODEL` or configured inference endpoints). All inference functions support batch execution for throughput optimization.

---

## 1. Inference Functions

### AI_COMPLETE

**Synopsis:**

```sql
AI_COMPLETE(model VARCHAR, prompt VARCHAR) → VARCHAR
AI_COMPLETE(model VARCHAR, prompt VARCHAR, file FILE) → VARCHAR       -- Multimodal
AI_COMPLETE(model VARCHAR, prompt VARCHAR, json_mode VARCHAR) → VARCHAR -- JSON output
```

**Description:** General-purpose text generation and completion. Sends a prompt to the specified model and returns the generated text. Supports multimodal input (text + file) and structured JSON output mode.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name (e.g., `'llama3'`, `'gpt-4o'`) |
| `prompt` | VARCHAR | Text prompt for the model |
| `file` | FILE (optional) | Multimodal file input (image/document) |
| `json_mode` | VARCHAR (optional) | Set to `'true'` for JSON-structured output |

**Example:**

```sql
-- Basic text completion
SELECT AI_COMPLETE('llama3', 'Explain quantum computing in one sentence') AS answer;

-- Multimodal: describe an image
SELECT AI_COMPLETE('gpt-4o', 'Describe this image', FILE_FROM_URL('s3://bucket/photo.jpg')) AS description;

-- JSON mode output
SELECT AI_COMPLETE('llama3', 'List 3 colors as JSON array', 'true') AS json_result;
```

**Notes:**
- Supports batch execution for multiple rows
- When the third parameter is a FILE (ROW type), multimodal inference is used
- When the third parameter is the string `'true'`, output is constrained to valid JSON

**See Also:** [AI_SUMMARIZE](#ai_summarize), [AI_EXTRACT](#ai_extract)

---

### AI_CLASSIFY

**Synopsis:**

```sql
AI_CLASSIFY(model VARCHAR, text VARCHAR, labels VARCHAR) → VARCHAR
```

**Description:** Classifies the input text into one of the provided labels. Returns the most appropriate label as determined by the model.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name |
| `text` | VARCHAR | Text to classify |
| `labels` | VARCHAR | Comma-separated list of possible labels |

**Example:**

```sql
SELECT AI_CLASSIFY('llama3', 'I love this product!', 'positive,negative,neutral') AS sentiment;
-- Result: 'positive'

-- Classify support tickets
SELECT ticket_id,
       AI_CLASSIFY('llama3', description, 'bug,feature_request,question') AS category
FROM support_tickets;
```

**Notes:**
- The model is instructed to respond with exactly one of the provided labels
- Supports batch execution for throughput optimization

**See Also:** [AI_FILTER](#ai_filter), [AI_GUARD](#ai_guard)

---

### AI_SUMMARIZE

**Synopsis:**

```sql
AI_SUMMARIZE(model VARCHAR, text VARCHAR) → VARCHAR
```

**Description:** Generates a concise summary of the input text.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name |
| `text` | VARCHAR | Text to summarize |

**Example:**

```sql
SELECT AI_SUMMARIZE('llama3', content) AS summary
FROM articles
WHERE published_date >= DATE '2024-01-01';
```

**Notes:**
- Supports batch execution
- Output length is determined by the model's default behavior

**See Also:** [AI_COMPLETE](#ai_complete), [AI_EXTRACT](#ai_extract)

---

### AI_FILTER

**Synopsis:**

```sql
AI_FILTER(model VARCHAR, text VARCHAR, criteria VARCHAR) → BOOLEAN
```

**Description:** Evaluates whether the input text matches the specified criteria. Returns `TRUE` if the text satisfies the criteria, `FALSE` otherwise.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name |
| `text` | VARCHAR | Text to evaluate |
| `criteria` | VARCHAR | Natural language filter criteria |

**Example:**

```sql
-- Filter reviews mentioning product quality
SELECT * FROM reviews
WHERE AI_FILTER('llama3', review_text, 'mentions product quality or durability');

-- Content moderation
DELETE FROM comments
WHERE AI_FILTER('llama3', content, 'contains hate speech or harassment');
```

**Notes:**
- Returns a BOOLEAN value (not VARCHAR)
- Does not support batch execution (evaluated per-row)
- Uses deterministic generation parameters for consistent results

**See Also:** [AI_GUARD](#ai_guard), [AI_CLASSIFY](#ai_classify)

---

### AI_EXTRACT

**Synopsis:**

```sql
AI_EXTRACT(model VARCHAR, text VARCHAR, schema_json VARCHAR) → VARCHAR
```

**Description:** Extracts structured information from text according to a JSON schema specification. Returns a JSON string conforming to the provided schema.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name |
| `text` | VARCHAR | Source text to extract from |
| `schema_json` | VARCHAR | JSON schema defining the extraction target |

**Example:**

```sql
-- Extract contact information
SELECT AI_EXTRACT('llama3', email_body,
    '{"name": "string", "email": "string", "phone": "string"}') AS contact_info
FROM emails;

-- Extract product attributes
SELECT product_id,
       AI_EXTRACT('llama3', description,
           '{"color": "string", "size": "string", "material": "string"}') AS attributes
FROM products;
```

**Notes:**
- Uses deterministic JSON generation parameters for reliable structured output
- Output is always valid JSON conforming to the provided schema
- Supports batch execution

**See Also:** [AI_COMPLETE](#ai_complete), [AI_CLASSIFY](#ai_classify)

---

### AI_GUARD

**Synopsis:**

```sql
AI_GUARD(model VARCHAR, text VARCHAR, policy VARCHAR) → VARCHAR
```

**Description:** Content safety evaluation. Checks whether the input text complies with the specified policy. Returns `'PASS'` if compliant, or `'FAIL: <reason>'` with an explanation if non-compliant.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name |
| `text` | VARCHAR | Text to evaluate |
| `policy` | VARCHAR | Safety policy description |

**Example:**

```sql
-- Check content safety
SELECT AI_GUARD('llama3', user_message,
    'No personal information, no offensive language') AS safety_check
FROM messages;

-- Filter unsafe content
SELECT * FROM posts
WHERE AI_GUARD('llama3', content, 'appropriate for all ages') = 'PASS';
```

**Notes:**
- Returns `'PASS'` or `'FAIL: <explanation>'`
- Uses deterministic generation parameters
- Supports batch execution

**See Also:** [AI_FILTER](#ai_filter), [AI_CLASSIFY](#ai_classify)

---

### AI_PREDICT

**Synopsis:**

```sql
AI_PREDICT(model VARCHAR, features VARCHAR) → VARCHAR
```

**Description:** Makes predictions based on input features using the specified model. Suitable for regression, classification, and other prediction tasks.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name |
| `features` | VARCHAR | Input features (typically JSON-formatted) |

**Example:**

```sql
-- Predict customer churn risk
SELECT customer_id,
       AI_PREDICT('churn_model', CONCAT('{"tenure":', tenure, ',"monthly_charge":', charge, '}')) AS risk
FROM customers;
```

**Notes:**
- Supports batch execution
- Feature format depends on the target model's requirements

**See Also:** [AI_CLASSIFY](#ai_classify), [AI_COMPLETE](#ai_complete)

---

### AI_TRANSLATE

**Synopsis:**

```sql
AI_TRANSLATE(model VARCHAR, text VARCHAR, target_language VARCHAR) → VARCHAR
```

**Description:** Translates the input text to the specified target language.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name |
| `text` | VARCHAR | Text to translate |
| `target_language` | VARCHAR | Target language (e.g., `'English'`, `'Chinese'`, `'French'`) |

**Example:**

```sql
-- Translate product descriptions
SELECT product_id,
       AI_TRANSLATE('llama3', description, 'English') AS description_en
FROM products
WHERE locale = 'zh-CN';
```

**Notes:**
- Supports batch execution
- Target language is specified in natural language

**See Also:** [AI_SUMMARIZE](#ai_summarize), [AI_COMPLETE](#ai_complete)

---

### AI_EMBED

**Synopsis:**

```sql
AI_EMBED(model VARCHAR, text VARCHAR) → VARCHAR
AI_EMBED(model VARCHAR, file FILE) → VARCHAR
```

**Description:** Generates a vector embedding for the input text or file. Returns a JSON-formatted float array representing the embedding vector. Used for building vector search indexes and semantic similarity queries.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Registered embedding model name |
| `text` | VARCHAR | Text to embed |
| `file` | FILE (alternative) | FILE struct for multimodal embedding |

**Example:**

```sql
-- Generate text embedding
SELECT AI_EMBED('bge_m3', 'Introduction to machine learning') AS embedding;

-- Embed document content for similarity search
INSERT INTO articles (id, title, content, embedding)
SELECT id, title, content,
       CAST(AI_EMBED('bge_m3', content) AS VECTOR(1024))
FROM raw_articles;

-- Embed a file (image/document)
SELECT AI_EMBED('clip_vit', FILE_FROM_URL('s3://bucket/image.jpg')) AS image_embedding;
```

**Notes:**
- Returns a JSON array of floats (cast to VECTOR for storage)
- For text input, uses `EmbeddingProvider.embedBatch()` for efficient batch processing
- For FILE input, processes per-row via `EmbeddingProvider.embed(BlobReference)`
- The model must be registered via `CREATE EMBEDDING MODEL`

**See Also:** [Vector Search](vector-search.md), [CREATE EMBEDDING MODEL](#create-embedding-model)

---

## 2. Multimodal Functions

Multimodal functions process non-text data (images, documents, audio) using AI models that support the corresponding modalities.

### AI_DESCRIBE_IMAGE

**Synopsis:**

```sql
AI_DESCRIBE_IMAGE(model VARCHAR, file FILE) → VARCHAR
```

**Description:** Generates a natural language description of an image. Requires a model that supports the IMAGE modality.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Vision-capable model name |
| `file` | FILE | Image file (JPEG, PNG, WebP, etc.) |

**Example:**

```sql
SELECT id, AI_DESCRIBE_IMAGE('gpt-4o', photo) AS description
FROM product_photos;
```

**Notes:**
- The model must support IMAGE modality
- Supported formats: JPEG, PNG, GIF, WebP, BMP, TIFF, SVG

**See Also:** [AI_OCR](#ai_ocr), [FILE_FROM_URL](#file_from_url)

---

### AI_PARSE_DOCUMENT

**Synopsis:**

```sql
AI_PARSE_DOCUMENT(model VARCHAR, file FILE) → VARCHAR
```

**Description:** Extracts and structures the content of a document file (PDF, DOCX, etc.) into text. Requires a model that supports the DOCUMENT or IMAGE modality.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Document-capable model name |
| `file` | FILE | Document file (PDF, DOCX, etc.) |

**Example:**

```sql
-- Parse PDF documents
SELECT doc_id, AI_PARSE_DOCUMENT('gpt-4o', document_file) AS parsed_text
FROM contracts;
```

**Notes:**
- Supports PDF, DOCX, XLS/XLSX, TXT, CSV, XML
- The model must support DOCUMENT or IMAGE modality

**See Also:** [AI_DESCRIBE_IMAGE](#ai_describe_image), [AI_OCR](#ai_ocr)

---

### AI_TRANSCRIBE

**Synopsis:**

```sql
AI_TRANSCRIBE(model VARCHAR, file FILE) → VARCHAR
```

**Description:** Transcribes audio content to text. Requires a model that supports the AUDIO modality.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Audio-capable model name |
| `file` | FILE | Audio file (MP3, WAV, OGG, FLAC) |

**Example:**

```sql
-- Transcribe meeting recordings
SELECT meeting_id, AI_TRANSCRIBE('whisper', audio_file) AS transcript
FROM recordings;
```

**Notes:**
- Supported audio formats: MP3, WAV, OGG, FLAC
- The model must support AUDIO modality

**See Also:** [AI_SUMMARIZE](#ai_summarize), [FILE_FROM_URL](#file_from_url)

---

### AI_OCR

**Synopsis:**

```sql
AI_OCR(file FILE) → VARCHAR
```

**Description:** Performs Optical Character Recognition on an image, extracting text content. Uses the system's default vision model (no model parameter required).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `file` | FILE | Image file containing text |

**Example:**

```sql
-- Extract text from scanned documents
SELECT doc_id, AI_OCR(scan_image) AS extracted_text
FROM scanned_documents;

-- OCR on a remote image
SELECT AI_OCR(FILE_FROM_URL('s3://bucket/receipt.jpg')) AS receipt_text;
```

**Notes:**
- Does NOT require a model parameter — uses the system default vision model
- Optimized for text extraction from images

**See Also:** [AI_DESCRIBE_IMAGE](#ai_describe_image), [AI_PARSE_DOCUMENT](#ai_parse_document)

---

## 3. File Helper Functions

The FILE type is a struct (ROW type) representing an external file reference using the claim-check pattern.

**FILE struct fields:**

| Field | Type | Description |
|-------|------|-------------|
| `uri` | VARCHAR | Storage URI (s3://, oss://, gs://, http://, etc.) |
| `mime_type` | VARCHAR | MIME content type |
| `size` | BIGINT | File size in bytes |
| `checksum` | VARCHAR | SHA-256 checksum |
| `created_at` | TIMESTAMP | Creation timestamp |

### FILE_FROM_URL

**Synopsis:**

```sql
FILE_FROM_URL(url VARCHAR) → FILE
```

**Description:** Creates a FILE struct from a URL. The MIME type is automatically inferred from the URL file extension.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | VARCHAR | File URL (s3://, oss://, gs://, http://, https://, etc.) |

**Example:**

```sql
-- Create FILE from S3 URL
SELECT FILE_FROM_URL('s3://my-bucket/images/photo.jpg') AS photo;

-- Insert with FILE reference
INSERT INTO documents (id, attachment)
VALUES (1, FILE_FROM_URL('s3://docs/contract.pdf'));
```

**Notes:**
- Supported URL schemes: `s3`, `oss`, `gs`, `gcs`, `adls`, `abfs`, `abfss`, `hdfs`, `http`, `https`, `blob`
- MIME type is inferred from file extension (e.g., `.jpg` → `image/jpeg`, `.pdf` → `application/pdf`)

**See Also:** [FILE_FROM_BYTES](#file_from_bytes), [AI_DESCRIBE_IMAGE](#ai_describe_image)

---

### FILE_FROM_BYTES

**Synopsis:**

```sql
FILE_FROM_BYTES(content VARBINARY, mime_type VARCHAR) → FILE
```

**Description:** Creates a FILE struct from raw byte content. Generates a content-addressable URI using SHA-256 hash of the content.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `content` | VARBINARY | Raw file content |
| `mime_type` | VARCHAR | MIME type (e.g., `'image/png'`, `'application/pdf'`) |

**Example:**

```sql
-- Create FILE from inline bytes
SELECT FILE_FROM_BYTES(X'89504E47...', 'image/png') AS image;
```

**Notes:**
- URI is generated as a content-addressable reference using SHA-256 hash
- Useful for programmatically constructed file content

**See Also:** [FILE_FROM_URL](#file_from_url)

---

## 4. Embedding Model DDL

Embedding models must be registered before use with `AI_EMBED` or auto-embedding columns.

### CREATE EMBEDDING MODEL

**Synopsis:**

```sql
CREATE EMBEDDING MODEL model_name (
    provider = 'provider_type',
    endpoint = 'endpoint_url',
    dimension = dimension_value
    [, api_key_secret = 'secret_ref']
    [, max_batch_size = batch_size]
    [, max_rps = rate_limit]
    [, query_prefix = 'prefix']
    [, document_prefix = 'prefix']
);
```

**Description:** Registers a new embedding model configuration in the catalog.

**Parameters:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `provider` | VARCHAR | Yes | Provider type (e.g., `'openai-compatible'`) |
| `endpoint` | VARCHAR | Yes | Inference endpoint URL |
| `dimension` | INTEGER | Yes | Output vector dimensionality |
| `api_key_secret` | VARCHAR | No | Opaque secret reference for authentication |
| `max_batch_size` | INTEGER | No | Maximum batch size for embedding requests |
| `max_rps` | INTEGER | No | Rate limit (requests per second) |
| `query_prefix` | VARCHAR | No | Prefix prepended to query text |
| `document_prefix` | VARCHAR | No | Prefix prepended to document text |

**Example:**

```sql
CREATE EMBEDDING MODEL bge_m3 (
    provider = 'openai-compatible',
    endpoint = 'http://localhost:11434/api/embed',
    dimension = '1024',
    max_batch_size = '64'
);
```

**Notes:**
- Requires ADMIN privilege
- Model names must be unique across the catalog
- Credentials are stored as opaque secret references (never plaintext)

---

### ALTER EMBEDDING MODEL

**Synopsis:**

```sql
ALTER EMBEDDING MODEL model_name SET (
    property = value [, ...]
);
```

**Description:** Updates properties of an existing embedding model. Uses optimistic concurrency (version check).

**Example:**

```sql
ALTER EMBEDDING MODEL bge_m3 SET (
    endpoint = 'http://new-host:11434/api/embed',
    max_batch_size = '128'
);
```

**Notes:**
- Requires ADMIN privilege
- In-flight requests complete with the previous configuration
- New requests use the updated configuration immediately

---

### DROP EMBEDDING MODEL

**Synopsis:**

```sql
DROP EMBEDDING MODEL model_name [CASCADE];
```

**Description:** Removes a registered embedding model from the catalog.

**Example:**

```sql
-- Drop model (fails if auto-embedding columns depend on it)
DROP EMBEDDING MODEL bge_m3;

-- Force drop with dependent column definitions
DROP EMBEDDING MODEL bge_m3 CASCADE;
```

**Notes:**
- Without CASCADE: fails if any auto-embedding columns reference this model
- With CASCADE: drops dependent column definitions (existing vector data is retained)
- Requires ADMIN privilege

---

### SHOW EMBEDDING MODELS

**Synopsis:**

```sql
SHOW EMBEDDING MODELS;
```

**Description:** Lists all registered embedding models visible to the current user.

**Example:**

```sql
SHOW EMBEDDING MODELS;
-- Returns: model_name, provider, endpoint, dimension, max_batch_size, max_rps, version
```

---

## 5. Auto-Embedding (Generated Columns)

Auto-embedding columns automatically compute vector embeddings when data is inserted or updated.

### Syntax

```sql
CREATE TABLE table_name (
    id BIGINT,
    content VARCHAR,
    embedding VECTOR(dim) GENERATED ALWAYS AS (
        ai_embed('model_name', source_col1 [, source_col2, ...])
    ) EMBEDDING MODE { SYNC | ASYNC }
);
```

**Modes:**

| Mode | Behavior |
|------|----------|
| `SYNC` | Embedding is computed before the DML commits. Row is committed atomically with its vector. |
| `ASYNC` | Row is committed with NULL embedding immediately. Background process computes and backfills vectors. |

**Example:**

```sql
-- Sync mode: embedding computed on insert
CREATE TABLE articles (
    id BIGINT,
    title VARCHAR,
    content VARCHAR,
    embedding VECTOR(1024) GENERATED ALWAYS AS (
        ai_embed('bge_m3', title, content)
    ) EMBEDDING MODE SYNC
);

-- Async mode: non-blocking insert
CREATE TABLE products (
    id BIGINT,
    description VARCHAR,
    image_vec VECTOR(512) GENERATED ALWAYS AS (
        ai_embed('clip_vit', description)
    ) EMBEDDING MODE ASYNC
);
```

**Notes:**
- Source columns are concatenated for text embedding
- On UPDATE: re-embedding triggers only when source columns are modified
- SYNC mode adds latency to DML operations proportional to embedding model response time
- ASYNC mode uses a background drainer queue for eventual consistency

**See Also:** [ALTER TABLE BACKFILL COLUMN](#alter-table-backfill-column), [Vector Search](vector-search.md)

---

## 6. Backfill Operations

Backfill operations populate embedding vectors for existing rows that have NULL embeddings.

### ALTER TABLE BACKFILL COLUMN

**Synopsis:**

```sql
ALTER TABLE table_name BACKFILL COLUMN column_name
    [WHERE condition]
    [BATCH_SIZE batch_size]
    [CONCURRENCY concurrency];
```

**Description:** Starts a distributed embedding backfill operation for the specified column. Takes a snapshot at invocation time and processes only rows visible at that snapshot.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `WHERE` | Expression | All NULL rows | Filter condition for rows to backfill |
| `BATCH_SIZE` | INTEGER | 1000 | Number of rows per Worker batch |
| `CONCURRENCY` | INTEGER | Config default | Number of parallel Worker tasks |

**Example:**

```sql
-- Backfill all NULL embeddings
ALTER TABLE articles BACKFILL COLUMN embedding;

-- Backfill with filter and tuning
ALTER TABLE articles BACKFILL COLUMN embedding
WHERE embedding IS NULL AND category = 'tech'
BATCH_SIZE 500
CONCURRENCY 8;
```

---

### CANCEL BACKFILL

**Synopsis:**

```sql
CANCEL BACKFILL ON table_name COLUMN column_name;
```

**Description:** Cancels an in-progress backfill operation. In-flight Worker tasks complete; no new partitions are dispatched. Already committed batches are retained.

**Example:**

```sql
CANCEL BACKFILL ON articles COLUMN embedding;
```

---

### SHOW BACKFILL STATUS

**Synopsis:**

```sql
SHOW BACKFILL STATUS ON table_name;
```

**Description:** Displays the current status of backfill operations on the specified table.

**Example:**

```sql
SHOW BACKFILL STATUS ON articles;
-- Returns: column_name, state, total_rows, processed_rows, failed_rows, start_time
```

**Backfill States:**

| State | Description |
|-------|-------------|
| `SCANNING` | Scanning rows and partitioning work |
| `RUNNING` | Dispatching partitions to Workers |
| `CANCELLING` | Cancellation requested; draining in-flight tasks |
| `COMPLETED` | All partitions processed |
| `FAILED` | Unrecoverable error occurred |

**See Also:** [Auto-Embedding](#5-auto-embedding-generated-columns), [AI_EMBED](#ai_embed)

---

## 7. Configuration

### Inference Endpoint Configuration

AI functions require inference endpoints to be configured. Configuration is typically set at the cluster level:

| Property | Description |
|----------|-------------|
| `pangrova.ai.inference.provider` | Default inference provider |
| `pangrova.ai.inference.endpoint` | Default inference endpoint URL |
| `pangrova.ai.inference.api-key-secret` | Secret reference for API authentication |
| `pangrova.ai.inference.default-model` | Default model for functions that don't specify one |
| `pangrova.ai.inference.timeout-ms` | Request timeout in milliseconds |
| `pangrova.ai.inference.max-retries` | Maximum retry attempts for transient failures |

### Embedding Configuration

| Property | Description |
|----------|-------------|
| `pangrova.ai.embedding.default-provider` | Default embedding provider type |
| `pangrova.ai.embedding.max-batch-size` | Global maximum batch size |
| `pangrova.ai.embedding.async.queue-size` | ASYNC mode background queue capacity |
| `pangrova.ai.embedding.async.drain-interval-ms` | Background drainer poll interval |

### FILE Storage Configuration

| Property | Description |
|----------|-------------|
| `pangrova.file.allowed-schemes` | Allowed URI schemes for FILE columns |
| `pangrova.file.max-size-bytes` | Maximum file size limit |
| `pangrova.file.allowed-types` | Allowed MIME types (table property) |

---

## Cross-References

| Topic | Guide |
|-------|-------|
| Vector distance functions & search | [Vector Search](vector-search.md) |
| Data types (VECTOR, FILE) | [Data Types](data-types.md) |
| SQL DDL for tables with vector columns | [SQL Reference](sql-reference.md) |
| Client connectivity | [Clients](clients.md) |
