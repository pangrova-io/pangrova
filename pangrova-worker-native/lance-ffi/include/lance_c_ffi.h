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

/* ==========================================================================
 * Runtime lifecycle
 * ========================================================================== */

/**
 * Initialize the Lance FFI runtime (Tokio async runtime on the Rust side).
 * Calling this explicitly before reader/writer operations amortizes the
 * ~2-5ms one-time runtime spin-up cost. Safe to call multiple times;
 * subsequent calls are no-ops if already initialized on this thread.
 *
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_runtime_init(void);

/**
 * Shut down the Lance FFI runtime for the current thread.
 * After this call, new reader/writer operations will re-initialize.
 * Safe to call if not initialized (no-op).
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
 * @return Opaque reader handle on success, NULL on failure.
 *         On failure, call lance_ffi_last_error() for details.
 */
LanceCReader* lance_ffi_reader_open(const char* uri);

/**
 * Set column projection on the reader. Must be called before lance_ffi_reader_next.
 *
 * @param reader   Reader handle from lance_ffi_reader_open.
 * @param columns  Null-terminated comma-separated column names (e.g., "id,name,vec").
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_reader_set_projection(LanceCReader* reader, const char* columns);

/**
 * Set a filter expression on the reader. Must be called before lance_ffi_reader_next.
 *
 * @param reader  Reader handle from lance_ffi_reader_open.
 * @param filter  Null-terminated SQL-like filter expression (e.g., "id > 10 AND category = 'tech'").
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_reader_set_filter(LanceCReader* reader, const char* filter);

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
 * @param max_rows    Maximum rows per batch. Pass 0 for default (8192).
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
 * Create a Lance dataset writer.
 *
 * @param uri         Null-terminated output path for the Lance dataset.
 * @param schema_ptr  Arrow C Data Interface schema describing output columns.
 *                    Ownership is NOT transferred; caller retains the schema.
 * @param append      Non-zero to append to existing dataset; 0 to create new.
 * @return Opaque writer handle on success, NULL on failure.
 */
LanceCWriter* lance_ffi_writer_open(
    const char* uri,
    const struct ArrowSchema* schema_ptr,
    int32_t append);

/**
 * Write a record batch to the Lance dataset via Arrow C Data Interface.
 *
 * Ownership of array_ptr data transfers to the writer (caller must not release).
 *
 * @param writer      Writer handle.
 * @param array_ptr   Pointer to ArrowArray with batch data.
 * @param schema_ptr  Pointer to ArrowSchema describing the batch.
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_writer_write_batch(
    LanceCWriter* writer,
    struct ArrowArray* array_ptr,
    struct ArrowSchema* schema_ptr);

/**
 * Flush all buffered batches and finalize the Lance dataset on storage.
 *
 * @param writer  Writer handle.
 * @return 0 on success, -1 on error.
 */
int32_t lance_ffi_writer_finish(LanceCWriter* writer);

/**
 * Close and free the writer handle. Unflushed data is discarded.
 * Safe to call with NULL. After this call, the writer pointer is invalid.
 */
void lance_ffi_writer_close(LanceCWriter* writer);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* LANCE_C_FFI_H */
