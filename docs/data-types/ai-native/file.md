---
title: "FILE"
description: "Multimodal file reference type (claim-check pattern)"
---

# FILE


| Property | Value |
|----------|-------|
| Storage | STRUCT (claim-check pattern) |
| Internal | `ROW<uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at TIMESTAMP>` |
| Use case | Multimodal AI function input (images, documents, audio) |

**Declaration:**

```sql
CREATE TABLE documents (id BIGINT, attachment FILE);
```

**Construction:**

```sql
-- From URL
INSERT INTO documents VALUES (1, FILE_FROM_URL('s3://bucket/doc.pdf'));

-- From raw bytes
INSERT INTO documents VALUES (2, FILE_FROM_BYTES(X'89504E47...', 'image/png'));
```

**Usage with AI functions:**

```sql
SELECT AI_DESCRIBE_IMAGE('gpt-4o', attachment) AS description FROM documents;
SELECT AI_OCR(attachment) AS text FROM documents;
```

**Supported URI schemes:** `s3`, `oss`, `gs`, `gcs`, `adls`, `abfs`, `abfss`, `hdfs`, `http`, `https`, `blob`

**See Also:** [AI Functions — File Helpers](../../sql-reference/functions/ai-functions.md#file-helper-functions)
