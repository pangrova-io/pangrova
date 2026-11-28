---
title: "FILE"
description: "Governed multimodal file references and HTTPS import requirements"
---

# FILE

`FILE` stores a governed reference to an object, rather than embedding the full
payload in every table row.

| Property | Value |
|----------|-------|
| Storage | STRUCT (claim-check pattern) |
| Internal | `ROW<uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at TIMESTAMP>` |
| URI | Opaque `pangrova-blob://` reference returned by governed storage |
| Use case | Input to a configured model capability that supports the file's modality |

The Worker publishes the object and receives a governed `BlobReference` before
encoding the SQL row. Model functions decode its URI, MIME type, size, and
checksum when constructing a file prompt. The host validates access to the blob
on read. The Worker enforces its MIME allowlist before publication. Table-scoped
MIME properties currently have no enforcement in SQL writes.

## Declaration and Construction

```sql
CREATE TABLE documents (id BIGINT, attachment FILE);

-- Requires an allowed HTTPS host and configured Worker blob storage.
-- Replace this illustrative host with an approved, reachable source.
INSERT INTO documents VALUES
    (1, FILE_FROM_URL('https://documents.example.com/doc.pdf'));

-- Hex bytes for "hello"; MIME type and size must be allowed by policy.
INSERT INTO documents VALUES
    (2, FILE_FROM_BYTES(X'68656C6C6F', 'text/plain'));
```

`FILE_FROM_BYTES` returns SQL `NULL` when either argument is `NULL` and publishes
no object for that row. A nullable `FILE` retains its null value through
expressions, shuffle, and Arrow Flight results.

`FILE_FROM_URL` accepts **HTTPS only**. The importer checks the configured host
allowlist, URL structure, public DNS addresses, size, MIME type, and checksum.
S3, HDFS, plain HTTP, and other source URI schemes are not accepted by this helper.
The storage backend may use object storage internally; that does not make its
scheme a supported import URL or expose its credentials to SQL.

## Model Usage

A model object and provider with the required modality must already be configured.
These names are illustrative registered-model names, not bundled model services.

```sql
SELECT AI_DESCRIBE_IMAGE('image_model', attachment) AS description FROM documents;
SELECT AI_OCR('ocr_model', attachment) AS text FROM documents;
```

Use images for image description and supported document/image formats for OCR;
not every `FILE` payload is valid for every model. `AI_OCR` requires both the model
and file arguments.

See [AI Functions — File Helpers](../../sql-reference/functions/ai-functions.md#file-helper-functions),
[Worker Configuration](../../configuration/worker.md), and
[Current Capability Status](../../getting-started/capability-status.md).
