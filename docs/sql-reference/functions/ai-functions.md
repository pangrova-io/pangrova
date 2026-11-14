---
title: "AI Functions"
description: "AI_COMPLETE, AI_CLASSIFY, AI_EMBED, multimodal functions, model DDL, and backfill"
---

# AI Functions

Pangrova is an AI-native distributed SQL query engine. AI capabilities are exposed as standard SQL functions, enabling seamless integration of LLM inference, embedding generation, and multimodal processing directly within SQL queries.

> **Design Philosophy**: Every AI function follows the `(model, input...) → output` calling convention. Models are referenced by name (registered via `CREATE EMBEDDING MODEL` or configured inference endpoints). All inference functions support batch execution for throughput optimization.

---

## Inference Functions

### AI_COMPLETE

**Synopsis:**

```sql
AI_COMPLETE(model VARCHAR, prompt VARCHAR) → VARCHAR
AI_COMPLETE(model VARCHAR, prompt VARCHAR, file FILE) → VARCHAR       -- Multimodal
AI_COMPLETE(model VARCHAR, prompt VARCHAR, json_mode VARCHAR) → VARCHAR -- JSON output
```

**Description:** General-purpose text generation and completion. Sends a prompt to the exact configured model deployment and returns generated text. Multimodal input is admitted only when that model's exact provider dialect advertises the FILE modality.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Model name (e.g., `'llama3'`, `'gpt-4o'`) |
| `prompt` | VARCHAR | Text prompt for the model |
| `file` | FILE (optional) | Governed FILE input supported by the exact model dialect; chat-completions accepts images only |
| `json_mode` | VARCHAR (optional) | Set to `'true'` for JSON-structured output |

**Example:**

```sql
-- Basic text completion
SELECT AI_COMPLETE('llama3', 'Explain quantum computing in one sentence') AS answer;

-- Multimodal: describe an image
SELECT AI_COMPLETE('gpt-4o', 'Describe this image',
       FILE_FROM_URL('https://media.example/photo.jpg')) AS description;

-- JSON mode output
SELECT AI_COMPLETE('llama3', 'List 3 colors as JSON array', 'true') AS json_result;
```

**Notes:**
- Supports batch execution for multiple rows
- When the third parameter is a FILE, its MIME must match the model's advertised modality; an unsupported dialect fails before provider I/O
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
AI_EMBED(model VARCHAR, text VARCHAR) → VECTOR(N)
AI_EMBED(model VARCHAR, file FILE) → VECTOR(N) -- only for a binary-capable provider
```

**Description:** Generates a dimensioned vector embedding for the input. The output dimension is bound from the exact model capability and must match the destination VECTOR type.

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

-- Binary embedding requires a provider that explicitly advertises IMAGE.
SELECT AI_EMBED('clip_vit',
       FILE_FROM_URL('https://media.example/image.jpg')) AS image_embedding;
```

**Notes:**
- Returns a first-class `VECTOR(N)`; dimension mismatch is rejected before execution
- For text input, uses `EmbeddingProvider.embedBatch()` for efficient batch processing
- The built-in OpenAI-compatible `/v1/embeddings` dialect is text-only; it never base64-encodes media as ordinary text
- FILE input requires a separately installed provider implementation that advertises the exact binary modality
- The model must be registered via `CREATE EMBEDDING MODEL`

**See Also:** [Vector Search](vector-search.md), [CREATE EMBEDDING MODEL](#create-embedding-model)

---

## Multimodal Functions

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
- The FILE MIME type must be enabled by the Worker allowlist and accepted by the selected provider dialect
- The built-in OpenAI chat-completions dialect accepts governed `image/*` content and verifies the declared size and SHA-256 before transmission

**See Also:** [AI_OCR](#ai_ocr), [FILE_FROM_URL](#file_from_url)

---

### AI_PARSE_DOCUMENT

**Synopsis:**

```sql
AI_PARSE_DOCUMENT(model VARCHAR, file FILE) → VARCHAR
```

**Description:** Extracts and structures document content into text. The built-in
implementation requires an `openai-responses` model with DOCUMENT modality and
sends governed bytes as an `input_file` data URL; the chat-completions dialect is
rejected.

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
- The built-in Responses dialect admits PDF, DOCX, PPTX, plain text, and Markdown;
  the Worker FILE MIME allowlist can narrow this set further
- The model must advertise DOCUMENT; image-only chat models are not treated as document providers

**See Also:** [AI_DESCRIBE_IMAGE](#ai_describe_image), [AI_OCR](#ai_ocr)

---

### AI_TRANSCRIBE

**Synopsis:**

```sql
AI_TRANSCRIBE(model VARCHAR, file FILE) → VARCHAR
```

**Description:** Transcribes audio content to text. The built-in implementation
requires an `openai-transcription` model with exactly TEXT/AUDIO modalities and
uploads one governed FILE as multipart `/v1/audio/transcriptions`; audio is never
serialized as a chat image part.

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
- The model must support AUDIO modality and configure `max-batch-size = 1`

**See Also:** [AI_SUMMARIZE](#ai_summarize), [FILE_FROM_URL](#file_from_url)

---

### AI_OCR

**Synopsis:**

```sql
AI_OCR(model VARCHAR, file FILE) → VARCHAR
```

**Description:** Performs Optical Character Recognition on an image, extracting text content. The selected model must support IMAGE input.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | VARCHAR | Vision-capable model name |
| `file` | FILE | Image file containing text |

**Example:**

```sql
-- Extract text from scanned documents
SELECT doc_id, AI_OCR('gpt-4o', scan_image) AS extracted_text
FROM scanned_documents;

-- OCR on a remote image
SELECT AI_OCR(
    'gpt-4o',
    FILE_FROM_URL('https://media.example/receipt.jpg')) AS receipt_text;
```

**Notes:**
- The model argument is mandatory; Pangrova does not silently select a default model
- The FILE MIME type must be enabled by the Worker allowlist and accepted by the provider dialect

**See Also:** [AI_DESCRIBE_IMAGE](#ai_describe_image), [AI_PARSE_DOCUMENT](#ai_parse_document)

---

## File Helper Functions

The FILE type is a struct (ROW type) representing an external file reference using the claim-check pattern.

**FILE struct fields:**

| Field | Type | Description |
|-------|------|-------------|
| `uri` | VARCHAR | Opaque governed `pangrova-blob://v2/...` identity; physical storage URI is never exposed |
| `mime_type` | VARCHAR | MIME content type |
| `size` | BIGINT | File size in bytes |
| `checksum` | VARCHAR | SHA-256 checksum |
| `created_at` | TIMESTAMP | Creation timestamp |

### FILE_FROM_URL

**Synopsis:**

```sql
FILE_FROM_URL(url VARCHAR) → FILE
```

**Description:** Imports an allowlisted HTTPS object into the governed durable FILE store, verifies its bounded content and returns an opaque immutable reference.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | VARCHAR | HTTPS URL whose exact host is present in the Worker allowlist |

**Example:**

```sql
-- Import an allowlisted HTTPS object
SELECT FILE_FROM_URL('https://media.example/images/photo.jpg') AS photo;

-- Insert with FILE reference
INSERT INTO documents (id, attachment)
VALUES (1, FILE_FROM_URL('https://media.example/docs/contract.pdf'));
```

**Notes:**
- Only HTTPS is accepted; private/link-local destinations, redirects, DNS rebinding, oversized bodies and disallowed MIME types fail closed
- MIME, size and SHA-256 are verified before the FILE identity is returned

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

## Embedding Model DDL

Embedding models must be registered before use with `AI_EMBED` or auto-embedding columns.

### CREATE EMBEDDING MODEL

**Synopsis:**

```sql
CREATE EMBEDDING MODEL model_name (
    provider = 'provider_type',
    endpoint = 'endpoint_url',
    dimension = dimension_value
    [, authentication = 'bearer' | 'none']
    [, api_key_secret = 'secret_ref']
    [, max_batch_size = batch_size]
    [, max_requests_per_second = rate_limit]
    [, query_prefix = 'prefix']
    [, document_prefix = 'prefix']
);
```

**Description:** Registers a new embedding model configuration in the catalog.

**Parameters:**

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `provider` | VARCHAR | Yes | Provider type (e.g., `'openai-compatible'`) |
| `endpoint` | VARCHAR | Yes | HTTPS URL; anonymous HTTP is allowed only for a loopback endpoint |
| `dimension` | INTEGER | Yes | Output vector dimensionality |
| `authentication` | VARCHAR | No | `bearer` (default) or explicit `none` |
| `api_key_secret` | VARCHAR | For bearer | Nonblank opaque secret reference; prohibited with `authentication = 'none'` |
| `max_batch_size` | INTEGER | No | Maximum batch size for embedding requests |
| `max_requests_per_second` | INTEGER | No | Rate limit (requests per second) |
| `query_prefix` | VARCHAR | No | Prefix prepended to query text |
| `document_prefix` | VARCHAR | No | Prefix prepended to document text |

**Example:**

```sql
CREATE EMBEDDING MODEL bge_m3 (
    provider = 'openai-compatible',
    endpoint = 'http://localhost:11434/v1/embeddings',
    dimension = 1024,
    authentication = 'none',
    max_batch_size = '64',
    max_requests_per_second = '100'
);
```

**Notes:**
- Requires ADMIN privilege
- Model names must be unique across the catalog
- Credentials are stored as opaque secret references (never plaintext)
- Bearer-authenticated endpoints must use HTTPS; explicit unauthenticated HTTP is limited to literal loopback hosts.

---

### ALTER EMBEDDING MODEL

**Availability:** Available through the Coordinator metadata statement path.
Requires ADMIN privilege.

**Synopsis:**

```sql
ALTER EMBEDDING MODEL model_name SET (
    expected_version = integer,
    property = value [, ...]
);
```

**Description:** Creates the next catalog revision using optimistic concurrency. The
`expected_version` property is mandatory. Before persisting the revision, the Coordinator
requires one active Worker deployment whose exact embedding contract matches the updated
endpoint, secret reference, prefixes and serving limits. The catalog update atomically stores
both the incremented revision and that deployment's version/signature fence. A stale version or
missing/mismatched deployment fails without changing the current revision.

Mutable properties are `endpoint`, `api_key_secret`, `max_batch_size`,
`max_requests_per_second`, `query_prefix`, and `document_prefix`. Provider and vector dimension
are immutable; replace the model under a new name to change either.

**Example:**

```sql
ALTER EMBEDDING MODEL bge_m3 SET (
    expected_version = 0,
    endpoint = 'http://new-host:11434/api/embed',
    max_batch_size = '128'
);
```

---

### DROP EMBEDDING MODEL

**Synopsis:**

```sql
DROP EMBEDDING MODEL model_name [CASCADE];
```

**Description:** Currently rejected without changing the registry. Generated-column
definitions live in connector table metadata, which cannot yet be updated atomically
with the embedding-model registry.

**Example:**

```sql
-- Both forms currently fail closed with AI-0007
DROP EMBEDDING MODEL bge_m3;

DROP EMBEDDING MODEL bge_m3 CASCADE;
```

**Notes:**
- Both forms fail with `AI-0007` and retain the model record until a durable
  dependency authority and safe connector metadata mutation are implemented
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

## Auto-Embedding (Generated Columns)

Generated embedding values use the durable artifact materialization path.

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
| `SYNC` | Rejected until generated NEW/action expressions pass the real Worker write and atomic SDK commit acceptance gates. |
| `ASYNC` | Admitted Iceberg writes create snapshot-pinned durable artifact work; Native builds immutable vector/row-locator objects and publication is fenced and atomic. Paimon generated writes are currently rejected because the SDK lacks a verified atomic declaration-and-snapshot fence. |

Insert using an explicit list of ordinary target columns. Generated targets cannot
be assigned explicitly, even with a NULL placeholder. The Planner's shared SQL/AST
mutation conversion prepares typed NEW/action `AI_EMBED` expressions before
security policies, but this does not enable SYNC: its DDL and write-admission guards
remain in place until the actual Worker write and atomic SDK commit are accepted.
Ordered multi-source concatenation propagates NULL. The Native generated-value
segment-v2 sink retains NULL validity and the exact physical row locator; the
overlay restores SQL NULL, not a zero embedding. Java/Native use one format and
capability version, rejecting old sidecars instead of guessing their semantics.
The actual SQL/queue/provider/SELECT NULL acceptance test is still pending, so
Native component tests alone do not establish end-to-end NULL support.

**Example:**

```sql
-- Supported mode
CREATE TABLE products (
    id BIGINT,
    description VARCHAR,
    image_vec VECTOR(512) GENERATED ALWAYS AS (
        ai_embed('clip_vit', description)
    ) EMBEDDING MODE ASYNC
);
```

**Notes:**
- The expression and model bind to an exact `VECTOR(N)` output contract.
- Work is keyed by source snapshot and fenced generation; the Coordinator does
  not scan rows or calculate embeddings.
- Visibility follows immutable artifact generation publication, not an
  in-process background queue.
- FULL reads use the current SDK declaration; time travel requires the exact
  pinned declaration, including evidence of no generated columns. Paimon stores
  declaration properties with its schema revisions. Iceberg uses immutable
  snapshot-summary evidence or the verified snapshot-creation metadata chain.
  If that history has expired or is unavailable, the historical read fails with
  an explicit unsupported-operation error rather than using current properties
  or silently reading a former generated column as ordinary data.
- End-to-end generated-column query visibility is not yet accepted. The READ-level
  query overlay and shared protected historical-schema verification are wired, but
  the public SQL/provider-result regression has not yet run. Verified Iceberg
  JDBC/Hadoop operations check the frozen declaration at the actual SDK commit
  boundary; unsupported backends are rejected. Lance dataset reads now derive
  physical positions from validated SDK fragment/row addresses, but final
  Native/Java gates and public SQL visibility acceptance are pending. Dataset
  manifest partitions are not interchangeable with physical-file byte ranges.

**See Also:** [ALTER TABLE BACKFILL COLUMN](#alter-table-backfill-column), [Vector Search](vector-search.md)

---

## Backfill Operations

**Availability:** Not wired in the current Coordinator. The parser recognizes
the three statements below, but there are no production statement handlers for
start, cancel, or status. They are reserved syntax and must not be treated as a
durable backfill service. Existing generated-value work is triggered from the
connector commit/materialization path described above.

### ALTER TABLE BACKFILL COLUMN

**Synopsis:**

```sql
ALTER TABLE table_name BACKFILL COLUMN column_name
    [WHERE condition]
    [BATCH_SIZE batch_size]
    [CONCURRENCY concurrency];
```

**Description:** Reserved syntax; not executable in the current runtime.

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

**Description:** Reserved syntax; not executable in the current runtime.

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

**Description:** Reserved syntax; not executable in the current runtime.

**Example:**

```sql
SHOW BACKFILL STATUS ON articles;
-- Returns: column_name, state, total_rows, processed_rows, failed_rows, start_time
```

**Target state vocabulary (not a current result contract):**

| State | Description |
|-------|-------------|
| `SCANNING` | Scanning rows and partitioning work |
| `RUNNING` | Dispatching partitions to Workers |
| `CANCELLING` | Cancellation requested; draining in-flight tasks |
| `COMPLETED` | All partitions processed |
| `FAILED` | Unrecoverable error occurred |

**See Also:** [Auto-Embedding](#auto-embedding-generated-columns), [AI_EMBED](#ai_embed)

---

## Configuration

### Inference Endpoint Configuration

AI execution is disabled unless the Worker YAML declares an explicit authorization
policy and an exact versioned model under `worker.ai.models`. There is no implicit
localhost provider, default model, API-key literal, or JVM-system-property
credential fallback. Each model pins its provider dialect, upstream model ID,
version, endpoint, external secret reference, capabilities, modalities and hard
resource limits. See [Worker Configuration](../../configuration/worker.md#workerai).

### Embedding Configuration

The SQL [CREATE EMBEDDING MODEL](#create-embedding-model) statement records the
catalog contract. Execution additionally requires an exact compatible Worker
model descriptor and advertised capability. Built-in `openai-compatible`
embedding is text-only; binary embedding requires a provider that explicitly
implements that modality. The bound result is `VECTOR(N)` and a count, order,
dimension, non-finite-value, or model-version mismatch fails the whole batch.

### FILE Storage Configuration

`FILE_FROM_BYTES` and `FILE_FROM_URL` publish through the governed Worker FILE
runtime. Production Workers must explicitly configure the shared S3-compatible
backend, local staging directory, external capability-key secret reference,
encryption policy, byte limits and MIME allowlist. `FILE_FROM_URL` accepts HTTPS
only and also requires an exact host allowlist; internal storage URLs are never
returned or accepted as SQL values. SQL sees only checksum-bound opaque
`pangrova-blob://v2/...` references. See
[Worker Configuration](../../configuration/worker.md#workeraifile).

---

## Cross-References

| Topic | Guide |
|-------|-------|
| Vector distance functions & search | [Vector Search](vector-search.md) |
| Data types (VECTOR, FILE) | [VECTOR & FILE types](../../data-types/ai-native/vector.md) |
| SQL DDL for tables with vector columns | [DDL](../ddl.md) |
| Client connectivity | [REST API](../../getting-started/clients/rest-api.md) |
