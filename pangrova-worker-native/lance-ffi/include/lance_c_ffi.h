/*
 * lance_c_ffi.h — C-compatible API for Lance file format read/write/scan.
 *
 * This header declares the public C ABI exported by liblance_c_ffi.a (Rust).
 * C++ consumers (LanceBatchReader/Writer in pangrova-worker-native) include this
 * header and link against the static library.
 *
 * Data exchange uses the Arrow C Data Interface (ArrowArray + ArrowSchema).
 * See: https://arrow.apache.org/docs/format/CDataInterface.html
 */
#ifndef LANCE_C_FFI_H
#define LANCE_C_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations for Arrow C Data Interface structs.
 * These are defined in arrow/c/abi.h (Arrow C++ SDK). If that header is not
 * available, the caller must provide compatible struct definitions. */
struct ArrowArray;
struct ArrowSchema;

/* ==========================================================================
 * Opaque handle types
 * ========================================================================== */

/** Opaque handle to a Lance dataset reader. */
typedef struct LanceCReader LanceCReader;

/** Opaque handle to a Lance dataset writer. */
typedef struct LanceCWriter LanceCWriter;
typedef struct LanceCStorage LanceCStorage;
typedef struct LanceCPhysicalWriter LanceCPhysicalWriter;

/** ABI implemented by the pinned Rust crate and required by Native. */
#define PANGROVA_LANCE_C_FFI_ABI_VERSION 12u
#define PANGROVA_LANCE_C_FFI_MAX_BATCH_ROWS 1048576u

typedef enum LanceWriteOperation {
  LANCE_WRITE_OPERATION_UNSPECIFIED = 0,
  LANCE_WRITE_OPERATION_CREATE = 1,
  LANCE_WRITE_OPERATION_APPEND = 2,
  LANCE_WRITE_OPERATION_OVERWRITE = 3
} LanceWriteOperation;

/** Owned bytes returned by the FFI. Release exactly once with buffer_free. */
typedef struct LanceCBuffer {
  uint8_t* data;
  size_t len;
  size_t capacity;
} LanceCBuffer;

/** Exact Native pool allocation. Ownership transfers to publish on entry when
 * owner/release are valid, including failed calls. release must never throw and
 * must safely free the actual allocation/pool holder on any HTTP worker thread. */
typedef struct LanceCNativePayload {
  uint8_t* data;
  size_t len;
  void* owner;
  void (*release)(void* owner);
} LanceCNativePayload;

/** Canonical allocation/attempt are metadata only. Disk and object bytes come
 * from actual task grants; encoder TOTAL buffer hint is frozen in the binding. */
typedef struct LanceCPhysicalWriterOptions {
  const uint8_t* binding;
  size_t binding_len;
  const uint8_t* task_attempt;
  size_t task_attempt_len;
  const char* tenant_id;
  const char* spill_directory;
  uint64_t spill_bytes;
  uint64_t object_write_bytes;
} LanceCPhysicalWriterOptions;

LanceCPhysicalWriter* lance_ffi_physical_writer_open(
    const LanceCStorage* storage, const LanceCPhysicalWriterOptions* options);
/** Consumes ArrowArray even on error; resets caller's array slot to empty.
 * Schema is borrowed. Failed/invalid appends poison this handle permanently. */
int32_t lance_ffi_physical_writer_write_batch(LanceCPhysicalWriter* writer,
    struct ArrowArray* array, const struct ArrowSchema* schema);
/** Closes actual FileWriter. Zero bytes is valid only for genuine zero input rows. */
int32_t lance_ffi_physical_writer_finish_stage(LanceCPhysicalWriter* writer, uint64_t* bytes);
int32_t lance_ffi_physical_writer_finish_empty(LanceCPhysicalWriter* writer, LanceCBuffer* receipt);
/** Performs storage PutMode::Create, never Overwrite or HEAD-based emulation. */
int32_t lance_ffi_physical_writer_publish(LanceCPhysicalWriter* writer,
    LanceCNativePayload payload, LanceCBuffer* receipt);
/** Sole canonical ResourceUsage bytes, cumulative actual observations. Valid
 * before close even after failed append/publish; never represents success.
 * The handle's exclusive owner must serialize this call with all writer calls. */
int32_t lance_ffi_physical_writer_resource_usage(const LanceCPhysicalWriter* writer,
    LanceCBuffer* usage);
/** Local SDK / private-stage evidence only, never final-object publication or
 * catalog commit authority. staging_publication: 0 unobserved, 1 not published,
 * 2 published, 3 unknown. UNKNOWN never authorizes final-object deletion. */
typedef struct LanceCWriteDrainEvidence {
  uint32_t encoders_and_uploads_joined;
  uint32_t private_stage_cleaned;
  uint32_t staging_publication;
} LanceCWriteDrainEvidence;
/** Stops new SDK I/O, joins actual encoders/uploads, then cleans private stage.
 * Always retains the handle/runtime. Nonzero preserves a real cleanup error;
 * output distinguishes that error from owners still running / stage still live. */
int32_t lance_ffi_physical_writer_drain(LanceCPhysicalWriter* writer,
    LanceCWriteDrainEvidence* evidence);
/** Releases only when joined AND private-stage-cleaned are actually verified.
 * Failure retains the handle for retry; this does not erase a prior drain error. */
int32_t lance_ffi_physical_writer_destroy(LanceCPhysicalWriter* writer);

/** Borrowed bytes supplied to the connector-owned aggregate commit call. */
typedef struct LanceCCommitFragment {
    const uint8_t* data;
    size_t len;
    const char* expected_staging_prefix;
} LanceCCommitFragment;

/** Return the exact C ABI version implemented by the linked library. */
uint32_t lance_ffi_abi_version(void);

/** Decode the canonical ObjectStoreExecutionBinding; never reads ambient AWS credentials. */
LanceCStorage* lance_ffi_storage_open(const uint8_t* canonical_binding, size_t binding_len);
void lance_ffi_storage_close(LanceCStorage* storage);

/* ==========================================================================
 * Runtime lifecycle
 * ========================================================================== */

/**
 * Initialize the Lance FFI runtime (Tokio async runtime on the Rust side).
 * Calling this explicitly before reader/writer operations amortizes the
 * ~2-5ms one-time runtime spin-up cost. The runtime is process-wide and shared
 * by all handles; repeated calls are idempotent.
 *
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_runtime_init(void);

/**
 * Release the shared runtime reference after native handles are closed. Live
 * readers/writers retain their own reference until close, so they cannot
 * observe a use-after-shutdown. A later init starts a new runtime.
 */
void lance_ffi_runtime_shutdown(void);

/* ==========================================================================
 * Error handling
 * ========================================================================== */

/**
 * Retrieve the last error message from the current thread.
 *
 * @return Null-terminated error string, or NULL if no error occurred.
 *         The pointer is valid until the next lance_ffi_* call on the same thread.
 */
const char* lance_ffi_last_error(void);

/* ==========================================================================
 * Reader API
 * ========================================================================== */

/**
 * Open a Lance dataset for reading.
 *
 * @param uri  Null-terminated path to the Lance dataset (local path or object
 *             store URI, e.g., "s3://bucket/path/dataset.lance").
 * @param version Exact non-zero table snapshot; only Dataset read units interpret it as a Lance manifest version.
 * @param file_detail Canonical FileSplitDetail with exactly one explicit read unit.
 * @param physical_file_size Exact SDK manifest file bytes for a physical file; zero for a Dataset partition.
 * @return Opaque reader handle on success, NULL on failure.
 *         On failure, call lance_ffi_last_error() for details.
 */
LanceCReader* lance_ffi_reader_open(const LanceCStorage* storage, const char* uri, uint64_t version,
                                 const uint8_t* file_detail, size_t file_detail_len,
                                 uint64_t physical_file_size);

/** Original physical file position of the last emitted batch; rejects Dataset scans. */
int32_t lance_ffi_reader_last_batch_row_start(const LanceCReader* reader, uint64_t* out_position);

/** Bounded object range read under an exact Worker-local catalog storage authority. */
int32_t lance_ffi_storage_read_range(const LanceCStorage* storage, const char* uri,
    uint64_t offset, uint64_t length, LanceCBuffer* out_buffer);

/** Returns canonical DatasetScanPartitionsResult metadata for a bounded immutable manifest page. */
int32_t lance_ffi_scan_partitions(const LanceCStorage* storage,
                                const uint8_t* canonical_request, size_t request_len,
                                LanceCBuffer* out_page);

/** Resolve the latest visible dataset version for connector write allocation. */
int32_t lance_ffi_dataset_version(
    const LanceCStorage* storage,
    const char* uri,
    uint64_t* out_version);

/**
 * Resolve version/schema for the exact non-zero pinned snapshot. Both outputs
 * are populated from the same checked-out Dataset; there is no latest fallback.
 */
int32_t lance_ffi_dataset_info(
    const LanceCStorage* storage,
    const char* uri,
    uint64_t version,
    uint64_t* out_version,
    uint8_t* out_schema_sha256,
    size_t schema_sha256_len);

/** Resolve the canonical schema fence of the latest visible dataset. */
int32_t lance_ffi_dataset_schema_fingerprint(
    const LanceCStorage* storage,
    const char* uri,
    uint8_t* out_sha256,
    size_t out_len);

/**
 * Compute the canonical Lance write schema fence. The contract is SHA-256 of
 * the Arrow IPC schema message after recursively removing non-semantic schema
 * and field metadata. The caller supplies exactly 32 writable output bytes.
 */
int32_t lance_ffi_schema_fingerprint(
    const struct ArrowSchema* schema_ptr,
    uint8_t* out_sha256,
    size_t out_len);

/**
 * Set column projection on the reader. Must be called before lance_ffi_reader_next.
 *
 * @param reader   Reader handle from lance_ffi_reader_open.
 * @param columns       Array of null-terminated column names. Ownership stays
 *                      with the caller and names may contain punctuation.
 * @param column_count  Number of entries in columns; must be greater than zero.
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_reader_set_projection(
    LanceCReader* reader,
    const char* const* columns,
    size_t column_count);

/**
 * Read the next batch from the dataset via Arrow C Data Interface.
 *
 * The caller must provide pre-allocated ArrowArray and ArrowSchema structs.
 * On success (return 1), ownership of the array data transfers to the caller
 * who must release it per the Arrow C Data Interface specification.
 *
 * @param reader      Reader handle.
 * @param out_array   Pointer to caller-allocated ArrowArray (populated on success).
 * @param out_schema  Pointer to caller-allocated ArrowSchema (populated on success).
 * @param max_rows    Maximum rows per batch. Pass 0 for default (8192). Values
 *                    above PANGROVA_LANCE_C_FFI_MAX_BATCH_ROWS are rejected.
 * @return  1 if a batch was produced,
 *          0 if end of dataset (no more data),
 *         -1 on error (check lance_ffi_last_error).
 */
int32_t lance_ffi_reader_next(
    LanceCReader* reader,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema,
    uint64_t max_rows);

/**
 * Get the schema of the opened dataset via Arrow C Data Interface.
 *
 * @param reader      Reader handle.
 * @param out_schema  Pointer to caller-allocated ArrowSchema (populated on success).
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_reader_schema(
    const LanceCReader* reader,
    struct ArrowSchema* out_schema);

/**
 * Close and free the reader handle. Safe to call with NULL.
 * After this call, the reader pointer is invalid.
 */
void lance_ffi_reader_close(LanceCReader* reader);

/* ==========================================================================
 * Writer API
 * ========================================================================== */

/**
 * Create a task-attempt-private uncommitted Lance writer.
 *
 * @param uri         Null-terminated output path for the Lance dataset.
 * @param schema_ptr  Arrow C Data Interface schema describing output columns.
 *                    Ownership is NOT transferred; caller retains the schema.
 * @param operation   Exact CREATE/APPEND/OVERWRITE operation.
 * @param expected_read_version  0 only for CREATE, non-zero otherwise.
 * @param schema_fence_sha256    Connector-issued 32-byte schema fence.
 * @param schema_fence_len       Must be exactly 32.
 * @param write_id     Coordinator-issued idempotency identity.
 * @param tenant_id    Canonical TaskRequest security tenant.
 * @param generation   Connector-issued durable write generation.
 * @param transaction_staging_prefix Dataset-relative attempt-private prefix.
 * @param task_attempt Deterministically serialized canonical TaskAttemptId.
 * @param task_attempt_len Serialized TaskAttemptId byte length.
 * @return Opaque writer handle on success, NULL on failure.
 */
LanceCWriter* lance_ffi_writer_open(
    const LanceCStorage* storage,
    const char* uri,
    const struct ArrowSchema* schema_ptr,
    int32_t operation,
    uint64_t expected_read_version,
    const uint8_t* schema_fence_sha256,
    size_t schema_fence_len,
    const char* write_id,
    const char* tenant_id,
    const char* generation,
    const char* transaction_staging_prefix,
    const uint8_t* task_attempt,
    size_t task_attempt_len);

/**
 * Write a record batch to the Lance dataset via Arrow C Data Interface.
 *
 * Ownership of array_ptr data transfers to the writer (caller must not release).
 *
 * @param writer      Writer handle.
 * @param array_ptr   Consumed even on error and reset to an empty ArrowArray.
 * @param schema_ptr  Pointer to ArrowSchema describing the batch.
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_writer_write_batch(
    LanceCWriter* writer,
    struct ArrowArray* array_ptr,
    struct ArrowSchema* schema_ptr);

/**
 * Flush all buffered batches to immutable data files without publishing a
 * dataset manifest. The returned protobuf is typed, checksummed connector
 * commit metadata and must be durably collected before publication.
 *
 * @param writer        Writer handle.
 * @param out_fragment  Receives owned metadata; release with buffer_free.
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_writer_finish(
    LanceCWriter* writer,
    LanceCBuffer* out_fragment);

/** Release a buffer returned by lance_ffi_writer_finish. */
void lance_ffi_buffer_free(LanceCBuffer buffer);

/**
 * Atomically publish one aggregate transaction. This is connector-only: a
 * worker task must never call it. Re-driving an identical write_id and exact
 * fragment set returns the already committed version; a different fragment
 * set for the same write_id fails closed. `recovering` must be non-zero only
 * after the durable journal records that a prior commit call may have run.
 * A non-zero known_committed_version
 * from the connector durable journal checks exactly that version in O(1).
 * Zero scans the full relevant history lazily, bounded by deadline_epoch_ms.
 * Both outputs are written only after the exact persisted manifest is read and
 * hashed. An error, including a deadline after publication, is UNKNOWN and
 * leaves both outputs zero; never infer NOT_COMMITTED from an error string.
 */
int32_t lance_ffi_commit_fragments(
    const LanceCStorage* storage,
    const char* uri,
    const char* write_id,
    const char* tenant_id,
    const char* generation,
    int32_t operation,
    uint64_t expected_read_version,
    const uint8_t* schema_fence_sha256,
    size_t schema_fence_len,
    const LanceCCommitFragment* fragments,
    size_t fragment_count,
    uint32_t max_retries,
    int32_t recovering,
    uint64_t known_committed_version,
    uint64_t deadline_epoch_ms,
    uint64_t* out_committed_version,
    uint8_t* out_manifest_sha256,
    size_t manifest_sha256_len);

/**
 * Idempotently delete transaction sidecars after a durable successful commit
 * or an aborted write. Every reference is revalidated against the exact
 * connector-owned dataset, generation, task attempt, and staging prefix before
 * any object is deleted. Live Lance data files and manifests are never touched.
 */
int32_t lance_ffi_discard_fragments(
    const LanceCStorage* storage,
    const char* uri,
    const char* write_id,
    const char* tenant_id,
    const char* generation,
    int32_t operation,
    uint64_t expected_read_version,
    const uint8_t* schema_fence_sha256,
    size_t schema_fence_len,
    const LanceCCommitFragment* fragments,
    size_t fragment_count);

/**
 * Stops new Dataset input and joins the retained writer thread, whose SDK
 * drains its actual encoders/uploads on errors. Retains handle and runtime.
 * Dataset has no private FileStage; staging publication is unobserved (0).
 * Nonzero preserves the real operation/cleanup error, not a GC authorization.
 */
int32_t lance_ffi_writer_drain(LanceCWriter* writer, LanceCWriteDrainEvidence* evidence);
/** Frees only an actually drained owner. Failure retains it for observation;
 * never deletes manifests, Dataset data objects, or transaction sidecars. */
int32_t lance_ffi_writer_destroy(LanceCWriter* writer);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* LANCE_C_FFI_H */
