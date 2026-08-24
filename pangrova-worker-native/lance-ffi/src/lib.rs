//! Lance C FFI — C-compatible API for Lance file format read/write/scan.
//!
//! Exposes Lance operations via Arrow C Data Interface so that C++ consumers
//! (libpangrova_native / Velox) can read and write Lance files without Rust
//! runtime knowledge. All Arrow data crosses the boundary as ArrowArray +
//! ArrowSchema structs (zero-copy when possible).

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::Schema;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

// =============================================================================
// Opaque handle types exposed to C
// =============================================================================

/// Opaque handle to a Lance dataset reader.
pub struct LanceCReader {
    runtime: Arc<Runtime>,
    dataset: Dataset,
    projected_columns: Option<Vec<String>>,
    filter_expr: Option<String>,
    batch_iter: Option<Box<dyn Iterator<Item = RecordBatch> + Send>>,
}

/// Opaque handle to a Lance dataset writer.
pub struct LanceCWriter {
    runtime: Arc<Runtime>,
    uri: String,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    write_mode: WriteMode,
}

// =============================================================================
// Error handling
// =============================================================================

/// Thread-local last error message for C callers to retrieve.
thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<CString>> = std::cell::RefCell::new(None);
}

fn set_last_error(msg: &str) {
    let c_msg = CString::new(msg).unwrap_or_else(|_| CString::new("unknown error").unwrap());
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(c_msg);
    });
}

/// Retrieve the last error message. Returns NULL if no error.
/// The returned pointer is valid until the next FFI call on the same thread.
#[no_mangle]
pub extern "C" fn lance_ffi_last_error() -> *const c_char {
    LAST_ERROR.with(|e| {
        e.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null())
    })
}

// =============================================================================
// Runtime management
// =============================================================================

/// Global Tokio runtime singleton — shared across all Lance FFI readers and writers.
/// Avoids creating a new multi-thread runtime per open/close cycle, eliminating
/// thread-pool startup overhead (~2ms per call) and reducing OS thread churn.
static GLOBAL_RUNTIME: Lazy<Arc<Runtime>> = Lazy::new(|| {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("lance-ffi-rt")
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for Lance C FFI"),
    )
});

fn get_or_create_runtime() -> Arc<Runtime> {
    GLOBAL_RUNTIME.clone()
}

/// Initialize the Lance FFI runtime eagerly (see lance_c_ffi.h contract).
/// Forces the lazy global Tokio runtime to spin up so later reader/writer
/// calls do not pay the one-time startup cost. Idempotent.
#[no_mangle]
pub extern "C" fn lance_ffi_runtime_init() -> i32 {
    // Lazy::new panics inside the closure on runtime build failure; catch it
    // and surface as an error code instead of unwinding across the C boundary.
    match std::panic::catch_unwind(|| {
        let _ = get_or_create_runtime();
    }) {
        Ok(()) => 0,
        Err(_) => {
            set_last_error("failed to initialize Lance FFI Tokio runtime");
            -1
        }
    }
}

/// Shut down the Lance FFI runtime (see lance_c_ffi.h contract).
/// The runtime is a process-wide shared singleton also used by any live
/// readers/writers, so this is a safe no-op; the runtime is reclaimed at
/// process exit. Idempotent by design.
#[no_mangle]
pub extern "C" fn lance_ffi_runtime_shutdown() {}

// =============================================================================
// Reader API
// =============================================================================

/// Open a Lance dataset for reading.
///
/// # Parameters
/// - `uri`: null-terminated path to the Lance dataset (local or object store URI)
///
/// # Returns
/// - Opaque reader handle on success, NULL on failure (check lance_ffi_last_error)
#[no_mangle]
pub extern "C" fn lance_ffi_reader_open(uri: *const c_char) -> *mut LanceCReader {
    if uri.is_null() {
        set_last_error("uri must not be NULL");
        return ptr::null_mut();
    }
    let uri_str = match unsafe { CStr::from_ptr(uri) }.to_str() {
        Ok(s) => s.to_owned(),
        Err(e) => {
            set_last_error(&format!("invalid UTF-8 in uri: {e}"));
            return ptr::null_mut();
        }
    };

    let runtime = get_or_create_runtime();
    let dataset = match runtime.block_on(Dataset::open(&uri_str)) {
        Ok(ds) => ds,
        Err(e) => {
            set_last_error(&format!("failed to open dataset: {e}"));
            return ptr::null_mut();
        }
    };

    let reader = Box::new(LanceCReader {
        runtime,
        dataset,
        projected_columns: None,
        filter_expr: None,
        batch_iter: None,
    });
    Box::into_raw(reader)
}

/// Set column projection on the reader. Call before lance_ffi_reader_next.
///
/// # Parameters
/// - `columns`: null-terminated comma-separated column names
#[no_mangle]
pub extern "C" fn lance_ffi_reader_set_projection(
    reader: *mut LanceCReader,
    columns: *const c_char,
) -> i32 {
    if reader.is_null() || columns.is_null() {
        set_last_error("reader and columns must not be NULL");
        return -1;
    }
    let reader = unsafe { &mut *reader };
    let cols_str = match unsafe { CStr::from_ptr(columns) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(&format!("invalid UTF-8 in columns: {e}"));
            return -1;
        }
    };
    reader.projected_columns = Some(cols_str.split(',').map(|s| s.trim().to_owned()).collect());
    reader.batch_iter = None;
    0
}

/// Set a filter expression on the reader. Call before lance_ffi_reader_next.
///
/// # Parameters
/// - `filter`: null-terminated SQL-like filter expression (e.g., "id > 10")
#[no_mangle]
pub extern "C" fn lance_ffi_reader_set_filter(
    reader: *mut LanceCReader,
    filter: *const c_char,
) -> i32 {
    if reader.is_null() || filter.is_null() {
        set_last_error("reader and filter must not be NULL");
        return -1;
    }
    let reader = unsafe { &mut *reader };
    let filter_str = match unsafe { CStr::from_ptr(filter) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(&format!("invalid UTF-8 in filter: {e}"));
            return -1;
        }
    };
    reader.filter_expr = Some(filter_str.to_owned());
    reader.batch_iter = None;
    0
}

/// Read the next batch from the dataset, exporting via Arrow C Data Interface.
///
/// # Parameters
/// - `out_array`: pointer to caller-allocated FFI_ArrowArray (populated on success)
/// - `out_schema`: pointer to caller-allocated FFI_ArrowSchema (populated on success)
/// - `max_rows`: maximum rows to return in this batch (0 = default batch size 8192)
///
/// # Returns
/// - 1 if a batch was produced
/// - 0 if no more data (end of dataset)
/// - -1 on error (check lance_ffi_last_error)
#[no_mangle]
pub extern "C" fn lance_ffi_reader_next(
    reader: *mut LanceCReader,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    max_rows: u64,
) -> i32 {
    if reader.is_null() || out_array.is_null() || out_schema.is_null() {
        set_last_error("reader, out_array, and out_schema must not be NULL");
        return -1;
    }
    let reader = unsafe { &mut *reader };

    // Lazily initialize the scan iterator
    if reader.batch_iter.is_none() {
        let mut scanner = reader.dataset.scan();

        if let Some(ref cols) = reader.projected_columns {
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            if let Err(e) = scanner.project(&col_refs) {
                set_last_error(&format!("projection error: {e}"));
                return -1;
            }
        }

        if let Some(ref filter) = reader.filter_expr {
            if let Err(e) = scanner.filter(filter.as_str()) {
                set_last_error(&format!("filter error: {e}"));
                return -1;
            }
        }

        let batch_size = if max_rows > 0 {
            max_rows as usize
        } else {
            8192
        };
        // arrow-rs 53 / lance 0.20: batch_size() is a builder method returning
        // &mut Scanner (no Result).
        scanner.batch_size(batch_size);

        let batches = match reader.runtime.block_on(async {
            use futures::TryStreamExt;
            let stream = scanner.try_into_stream().await?;
            stream.try_collect::<Vec<_>>().await
        }) {
            Ok(bs) => bs,
            Err(e) => {
                set_last_error(&format!("scan error: {e}"));
                return -1;
            }
        };
        reader.batch_iter = Some(Box::new(batches.into_iter()));
    }

    // Get next batch from iterator
    let iter = reader.batch_iter.as_mut().unwrap();
    match iter.next() {
        Some(batch) => {
            let struct_array = arrow::array::StructArray::from(batch);
            match arrow::ffi::to_ffi(&struct_array.into_data()) {
                Ok((ffi_array, ffi_schema)) => {
                    unsafe {
                        ptr::write(out_array, ffi_array);
                        ptr::write(out_schema, ffi_schema);
                    }
                    1
                }
                Err(e) => {
                    set_last_error(&format!("Arrow FFI export error: {e}"));
                    -1
                }
            }
        }
        None => 0,
    }
}

/// Close and free the reader handle.
#[no_mangle]
pub extern "C" fn lance_ffi_reader_close(reader: *mut LanceCReader) {
    if !reader.is_null() {
        unsafe {
            drop(Box::from_raw(reader));
        }
    }
}

// =============================================================================
// Writer API
// =============================================================================

/// Create a Lance dataset writer.
///
/// # Parameters
/// - `uri`: null-terminated path for the output Lance dataset
/// - `schema_ptr`: Arrow C Data Interface schema describing the output columns
/// - `append`: if non-zero, append to existing dataset; otherwise create/overwrite
///
/// # Returns
/// - Opaque writer handle on success, NULL on failure
#[no_mangle]
pub extern "C" fn lance_ffi_writer_open(
    uri: *const c_char,
    schema_ptr: *const FFI_ArrowSchema,
    append: i32,
) -> *mut LanceCWriter {
    if uri.is_null() || schema_ptr.is_null() {
        set_last_error("uri and schema_ptr must not be NULL");
        return ptr::null_mut();
    }
    let uri_str = match unsafe { CStr::from_ptr(uri) }.to_str() {
        Ok(s) => s.to_owned(),
        Err(e) => {
            set_last_error(&format!("invalid UTF-8 in uri: {e}"));
            return ptr::null_mut();
        }
    };

    // Borrow the schema (contract: writer_open does NOT take ownership; the
    // C++ caller releases it). ptr::read here would create an owned
    // FFI_ArrowSchema whose Drop calls release(), causing a double-release
    // (bus error) when the caller releases afterwards.
    let schema = match Schema::try_from(unsafe { &*schema_ptr }) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            set_last_error(&format!("failed to import schema: {e}"));
            return ptr::null_mut();
        }
    };

    let write_mode = if append != 0 {
        WriteMode::Append
    } else {
        WriteMode::Create
    };

    let runtime = get_or_create_runtime();
    let writer = Box::new(LanceCWriter {
        runtime,
        uri: uri_str,
        schema,
        batches: Vec::new(),
        write_mode,
    });
    Box::into_raw(writer)
}

/// Write a batch to the Lance dataset via Arrow C Data Interface.
///
/// # Parameters
/// - `array_ptr`: pointer to FFI_ArrowArray with the batch data
/// - `schema_ptr`: pointer to FFI_ArrowSchema describing the batch
///
/// # Returns
/// - 0 on success, -1 on error
#[no_mangle]
pub extern "C" fn lance_ffi_writer_write_batch(
    writer: *mut LanceCWriter,
    array_ptr: *mut FFI_ArrowArray,
    schema_ptr: *mut FFI_ArrowSchema,
) -> i32 {
    if writer.is_null() || array_ptr.is_null() || schema_ptr.is_null() {
        set_last_error("writer, array_ptr, and schema_ptr must not be NULL");
        return -1;
    }
    let writer = unsafe { &mut *writer };

    // Array: take ownership per the C contract (caller must not release it).
    let ffi_array = unsafe { ptr::read(array_ptr) };
    // Schema: borrow only (contract: caller releases it) — an owned copy
    // would double-release on Drop.
    let ffi_schema = unsafe { &*schema_ptr };

    // arrow-rs 53: array import is from_ffi(array, &schema) -> ArrayData.
    let data = match unsafe { arrow::ffi::from_ffi(ffi_array, ffi_schema) } {
        Ok(d) => d,
        Err(e) => {
            set_last_error(&format!("failed to import array: {e}"));
            return -1;
        }
    };

    let struct_array = arrow::array::StructArray::from(data);
    let batch = RecordBatch::from(struct_array);
    writer.batches.push(batch);
    0
}

/// Flush all buffered batches and finalize the Lance dataset.
///
/// # Returns
/// - 0 on success, -1 on error
#[no_mangle]
pub extern "C" fn lance_ffi_writer_finish(writer: *mut LanceCWriter) -> i32 {
    if writer.is_null() {
        set_last_error("writer must not be NULL");
        return -1;
    }
    let writer = unsafe { &mut *writer };

    if writer.batches.is_empty() {
        return 0;
    }

    let batches = std::mem::take(&mut writer.batches);
    let reader = arrow::record_batch::RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        writer.schema.clone(),
    );

    let params = WriteParams {
        mode: writer.write_mode,
        ..Default::default()
    };

    match writer
        .runtime
        .block_on(Dataset::write(reader, &writer.uri, Some(params)))
    {
        Ok(_) => 0,
        Err(e) => {
            set_last_error(&format!("write error: {e}"));
            -1
        }
    }
}

/// Close and free the writer handle. Unflushed data is discarded.
#[no_mangle]
pub extern "C" fn lance_ffi_writer_close(writer: *mut LanceCWriter) {
    if !writer.is_null() {
        unsafe {
            drop(Box::from_raw(writer));
        }
    }
}

// =============================================================================
// Schema query API
// =============================================================================

/// Get the schema of an opened reader as Arrow C Data Interface.
///
/// # Returns
/// - 0 on success (out_schema populated), -1 on error
#[no_mangle]
pub extern "C" fn lance_ffi_reader_schema(
    reader: *const LanceCReader,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    if reader.is_null() || out_schema.is_null() {
        set_last_error("reader and out_schema must not be NULL");
        return -1;
    }
    let reader = unsafe { &*reader };
    let schema = reader.dataset.schema();
    let arrow_schema: Schema = schema.into();

    // arrow-rs 53: schema export is TryFrom<&Schema> for FFI_ArrowSchema,
    // written to the caller-provided out pointer.
    match FFI_ArrowSchema::try_from(&arrow_schema) {
        Ok(ffi) => {
            unsafe {
                ptr::write(out_schema, ffi);
            }
            0
        }
        Err(e) => {
            set_last_error(&format!("schema export error: {e}"));
            -1
        }
    }
}
