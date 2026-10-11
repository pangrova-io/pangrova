//! Lance C FFI — C-compatible API for Lance file format read/write/scan.
//!
//! Exposes Lance operations via Arrow C Data Interface so that C++ consumers
//! (libpangrova_native / Velox) can read and write Lance files without Rust
//! runtime knowledge. All Arrow data crosses the boundary as ArrowArray +
//! ArrowSchema structs (zero-copy when possible).

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, RecordBatch, RecordBatchReader};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_schema::{DataType, Field, Schema, UnionFields};
use futures::StreamExt;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{Dataset, InsertBuilder, WriteMode, WriteParams};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::stream::BatchStream;
use lance_table::format::pb as lance_pb;
use lance_table::io::commit::CommitError;
use object_store::{path::Path, Error as ObjectStoreError, PutMode, PutOptions, PutPayload};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::runtime::Runtime;
mod managed;
mod preparation;
mod cleanup;
mod physical;
mod physical_schema;
mod scan;
mod scan_identity;
mod storage;
mod write_drain;
use storage::StorageContext;
pub mod canonical {
    include!(concat!(env!("OUT_DIR"), "/canonical.rs"));
}
#[cfg(test)]
mod sdk_read_test_support;
use canonical::pangrova::protocol::v2::{
    checksum::Algorithm as ChecksumAlgorithm, file_split_detail::ReadUnit,
    Checksum as CanonicalChecksum, FileFormat, FileSplitDetail, LanceTransactionReference,
    LanceWriteFragment, LanceWriteOperation, TaskAttemptId as CanonicalTaskAttemptId,
};

const MAX_BATCH_ROWS: u64 = 1_048_576;
const LANCE_WRITE_FRAGMENT_VERSION: u32 = 2;
const SHA256_DIGEST_SIZE: usize = 32;
const MAX_TRANSACTION_SIDECAR_BYTES: usize = 64 * 1024 * 1024;
const MAX_INLINE_FRAGMENT_BYTES: usize = 64 * 1024;
const MAX_AGGREGATE_TRANSACTION_BYTES: usize = 256 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 256 * 1024 * 1024;

macro_rules! ffi_guard {
    ($operation:literal, $failure_value:expr, $body:block) => {{
        match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| $body)) {
            Ok(value) => value,
            Err(payload) => {
                $crate::set_last_error(&$crate::panic_message($operation, payload));
                $failure_value
            }
        }
    }};
}

mod physical_payload;
mod physical_stage;
mod physical_write;
mod physical_write_ffi;
mod dataset_write_ffi;

macro_rules! require_storage {
    ($storage:expr, $uri:expr, $tenant:expr, $failure:expr) => {
        match scoped_storage($storage, $uri, $tenant) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return $failure;
            }
        }
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AtomicCommitStore {
    Local,
    S3ConditionalPut,
}

impl AtomicCommitStore {
    fn for_uri(uri: &str) -> Result<Self, String> {
        if !uri.contains("://") || uri.starts_with("file://") {
            return Ok(Self::Local);
        }
        if uri.starts_with("s3://") {
            return Ok(Self::S3ConditionalPut);
        }
        Err(format!(
            "Lance atomic manifest commit is not configured for URI scheme: {uri}"
        ))
    }

    fn store_params(self) -> ObjectStoreParams {
        let mut params = ObjectStoreParams::default();
        if self == Self::S3ConditionalPut {
            params.storage_options = Some(HashMap::from([(
                "aws_conditional_put".to_owned(),
                "etag".to_owned(),
            )]));
        }
        params
    }


}

async fn verify_conditional_create(
    object_store: &ObjectStore,
    base_path: &Path,
) -> Result<(), CommitError> {
    let probe_path = Path::from(format!(
        "{base_path}/_pangrova/prepared/.conditional-put-probe-{}",
        uuid::Uuid::new_v4()
    ));
    let first_payload = b"pangrova-cas-probe-first";
    let second_payload = b"pangrova-cas-probe-second";
    let verified = async {
        object_store
            .inner
            .put_opts(
                &probe_path,
                PutPayload::from(first_payload.as_slice()),
                PutOptions::from(PutMode::Create),
            )
            .await
            .map_err(|error| CommitError::OtherError(error.into()))?;
        match object_store
            .inner
            .put_opts(
                &probe_path,
                PutPayload::from(second_payload.as_slice()),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Err(ObjectStoreError::AlreadyExists { .. }) => {}
            Ok(_) => {
                return Err(CommitError::OtherError(
                    std::io::Error::other(
                        "S3 backend ignored conditional create; Lance commit is unsafe",
                    )
                    .into(),
                ))
            }
            Err(error) => return Err(CommitError::OtherError(error.into())),
        }
        let payload = object_store
            .inner
            .get(&probe_path)
            .await
            .map_err(|error| CommitError::OtherError(error.into()))?
            .bytes()
            .await
            .map_err(|error| CommitError::OtherError(error.into()))?;
        if payload.as_ref() != first_payload {
            return Err(CommitError::OtherError(
                std::io::Error::other(
                    "S3 backend changed the object after rejecting conditional create",
                )
                .into(),
            ));
        }
        Ok(())
    }
    .await;
    let _ = object_store.inner.delete(&probe_path).await;
    verified
}

impl LanceWriteOperation {
    fn parse(value: i32) -> Result<Self, String> {
        Self::try_from(value)
            .map_err(|_| format!("unsupported Lance write operation value: {value}"))
            .and_then(|operation| {
                if operation == Self::Unspecified {
                    Err("Lance write operation must be specified".to_owned())
                } else {
                    Ok(operation)
                }
            })
    }

    fn write_mode(self) -> WriteMode {
        match self {
            Self::Create => WriteMode::Create,
            Self::Append => WriteMode::Append,
            Self::Overwrite => WriteMode::Overwrite,
            Self::Unspecified => unreachable!("validated before use"),
        }
    }
}

impl CanonicalTaskAttemptId {
    fn validate(&self) -> Result<(), String> {
        let owner = self
            .execution_owner
            .as_ref()
            .ok_or_else(|| "task_attempt.execution_owner must be present".to_owned())?;
        if self.query_or_job_id.trim().is_empty()
            || self.task_id.trim().is_empty()
            || self.stage_id < 0
            || self.partition_id < 0
            || self.plan_epoch == 0
            || self.fencing_token == 0
            || self.control_epoch == 0
            || owner.owner_id.trim().is_empty()
            || owner.owner_incarnation.trim().is_empty()
            || owner.fencing_token == 0
        {
            return Err("task_attempt is incomplete or unfenced".to_owned());
        }
        Ok(())
    }
}

/// Owned byte buffer crossing the C ABI. Release with lance_ffi_buffer_free.
#[repr(C)]
pub struct LanceCBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

/// Immutable scoped storage authority decoded from the canonical Worker-local contract.
pub struct LanceCStorage {
    context: StorageContext,
}

#[no_mangle]
pub extern "C" fn lance_ffi_storage_open(bytes: *const u8, length: usize) -> *mut LanceCStorage {
    ffi_guard!("storage_open", ptr::null_mut(), {
        if bytes.is_null() || length == 0 || length > 64 * 1024 {
            set_last_error("Worker-local storage binding violates its pointer/size contract");
            return ptr::null_mut();
        }
        match StorageContext::decode(unsafe { std::slice::from_raw_parts(bytes, length) }) {
            Ok(context) => Box::into_raw(Box::new(LanceCStorage { context })),
            Err(error) => {
                set_last_error(&error);
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_storage_close(storage: *mut LanceCStorage) {
    if !storage.is_null() {
        drop(unsafe { Box::from_raw(storage) });
    }
}

fn scoped_storage(
    storage: *const LanceCStorage,
    uri: &str,
    tenant: Option<&str>,
) -> Result<StorageContext, String> {
    if storage.is_null() {
        return Err("Dataset access requires Worker-local storage authority".into());
    }
    let context = &unsafe { &*storage }.context;
    context.require_uri(uri, tenant)?;
    Ok(context.clone())
}

// =============================================================================
// Opaque handle types exposed to C
// =============================================================================

/// Opaque handle to a Lance dataset reader.
pub struct LanceCReader {
    runtime: Arc<Runtime>,
    source: LanceReadSource,
    projected_columns: Option<Vec<String>>,
    batch_stream: Option<BatchStream>,
    pending_batch: Option<RecordBatch>,
    scan_started: bool,
    returned_rows: u64,
    last_batch_row_start: u64,
    last_partition_row_offset: Option<u32>,
}

enum LanceReadSource {
    Dataset {
        dataset: Dataset,
        partition: lance_table::format::Fragment,
    },
    Physical(physical::PhysicalFileReader),
}

#[no_mangle]
pub extern "C" fn lance_ffi_reader_last_batch_row_start(
    reader: *const LanceCReader,
    out_position: *mut u64,
) -> i32 {
    ffi_guard!("reader_last_batch_row_start", -1, {
        if reader.is_null() || out_position.is_null() {
            set_last_error("Physical row position requires live reader and output");
            return -1;
        }
        let reader = unsafe { &*reader };
        if !matches!(&reader.source, LanceReadSource::Physical(_)) || reader.returned_rows == 0 {
            set_last_error("Physical row position requires an emitted physical-file batch");
            return -1;
        }
        unsafe {
            *out_position = reader.last_batch_row_start;
        }
        0
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_storage_read_range(
    storage: *const LanceCStorage,
    uri: *const c_char,
    offset: u64,
    length: u64,
    out_buffer: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("storage_read_range", -1, {
        if uri.is_null() || out_buffer.is_null() || length == 0 || length > 64 * 1024 * 1024 {
            set_last_error("Object range requires URI, output and a 1..64 MiB byte length");
            return -1;
        }
        let uri = match unsafe { CStr::from_ptr(uri) }.to_str() {
            Ok(value) => value,
            Err(_) => {
                set_last_error("Object range URI is not UTF-8");
                return -1;
            }
        };
        let scoped = require_storage!(storage, uri, None, -1);
        let end = match offset
            .checked_add(length)
            .and_then(|value| usize::try_from(value).ok())
        {
            Some(value) => value,
            None => {
                set_last_error("Object range overflows its offset");
                return -1;
            }
        };
        let result = get_or_create_runtime().block_on(async {
            let (store, path) = ObjectStore::from_uri_and_params(
                Arc::new(ObjectStoreRegistry::default()),
                uri,
                &scoped.params(ObjectStoreParams::default()),
            )
            .await
            .map_err(|error| error.to_string())?;
            if end > store.size(&path).await.map_err(|error| error.to_string())? {
                return Err("Object range exceeds its immutable object".to_string());
            }
            store
                .read_one_range(&path, offset as usize..end)
                .await
                .map_err(|error| error.to_string())
        });
        match result {
            Ok(bytes) => {
                unsafe {
                    *out_buffer = into_c_buffer(bytes.to_vec());
                }
                0
            }
            Err(error) => {
                set_last_error(&format!("Object range read failed: {error}"));
                -1
            }
        }
    })
}

/// Opaque handle to a Lance dataset writer.
pub struct LanceCWriter {
    runtime: Arc<Runtime>,
    uri: String,
    schema: Arc<Schema>,
    operation: LanceWriteOperation,
    expected_read_version: u64,
    accepted_base_snapshot: Option<canonical::pangrova::protocol::v2::AcceptedSourceSnapshotRef>,
    schema_fence_sha256: [u8; SHA256_DIGEST_SIZE],
    write_id: String,
    tenant_id: String,
    generation: String,
    transaction_staging_prefix: String,
    task_attempt: CanonicalTaskAttemptId,
    batch_sender: Option<SyncSender<RecordBatch>>,
    writer_thread: Option<JoinHandle<Result<Transaction, String>>>,
    cancelled: Arc<AtomicBool>,
    finished: bool,
    rows_written: u64,
    input_bytes: u64,
    storage: StorageContext,
    drained: bool,
    drain_failure: Option<String>,
}

struct ChannelBatchReader {
    receiver: Receiver<RecordBatch>,
    schema: Arc<Schema>,
    cancelled: Arc<AtomicBool>,
    cancellation_reported: bool,
}

impl Iterator for ChannelBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let cancellation = |reported: &mut bool| {
            if *reported {
                None
            } else {
                *reported = true;
                Some(Err(ArrowError::InvalidArgumentError(
                    "Lance FFI writer cancelled before finish".to_owned(),
                )))
            }
        };
        if self.cancelled.load(Ordering::Acquire) {
            return cancellation(&mut self.cancellation_reported);
        }
        match self.receiver.recv() {
            Ok(_) if self.cancelled.load(Ordering::Acquire) => {
                cancellation(&mut self.cancellation_reported)
            }
            Ok(batch) => Some(Ok(batch)),
            Err(_) if self.cancelled.load(Ordering::Acquire) => {
                cancellation(&mut self.cancellation_reported)
            }
            Err(_) => None,
        }
    }
}

impl RecordBatchReader for ChannelBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// The schema fence covers SQL-visible structure, not transport-specific Arrow
// metadata whose HashMap iteration order is not canonical. Nested field
// metadata is removed recursively before producing an Arrow IPC schema message.
fn normalize_schema_field(field: &Field) -> Field {
    field
        .clone()
        .with_data_type(normalize_schema_type(field.data_type()))
        .with_metadata(HashMap::new())
}

fn normalize_schema_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(Arc::new(normalize_schema_field(field))),
        DataType::ListView(field) => DataType::ListView(Arc::new(normalize_schema_field(field))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(normalize_schema_field(field)), *size)
        }
        DataType::LargeList(field) => DataType::LargeList(Arc::new(normalize_schema_field(field))),
        DataType::LargeListView(field) => {
            DataType::LargeListView(Arc::new(normalize_schema_field(field)))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(normalize_schema_field(field)))
                .collect(),
        ),
        DataType::Union(fields, mode) => {
            let normalized: Vec<(i8, Arc<Field>)> = fields
                .iter()
                .map(|(id, field)| (id, Arc::new(normalize_schema_field(field))))
                .collect();
            let type_ids: Vec<i8> = normalized.iter().map(|(id, _)| *id).collect();
            DataType::Union(
                UnionFields::new(type_ids, normalized.into_iter().map(|(_, field)| field)),
                *mode,
            )
        }
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(normalize_schema_type(key)),
            Box::new(normalize_schema_type(value)),
        ),
        DataType::Map(field, sorted) => {
            DataType::Map(Arc::new(normalize_schema_field(field)), *sorted)
        }
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            Arc::new(normalize_schema_field(run_ends)),
            Arc::new(normalize_schema_field(values)),
        ),
        primitive => primitive.clone(),
    }
}

fn canonical_schema_fingerprint(schema: &Schema) -> [u8; SHA256_DIGEST_SIZE] {
    let normalized = Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| Arc::new(normalize_schema_field(field)))
            .collect::<Vec<_>>(),
    );
    let mut dictionaries = arrow::ipc::writer::DictionaryTracker::new(false);
    let encoded = arrow::ipc::writer::IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            &normalized,
            &mut dictionaries,
            &arrow::ipc::writer::IpcWriteOptions::default(),
        );
    sha256(&encoded.ipc_message)
        .try_into()
        .expect("SHA-256 always produces 32 bytes")
}

fn validate_relative_object_key(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 2048
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains("//")
        || value.contains('\\')
        || value.contains("://")
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'=')
        })
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(format!(
            "{field} must be a normalized dataset-relative object key"
        ));
    }
    Ok(())
}

fn transaction_object_key(prefix: &str, transaction_sha256: &[u8]) -> Result<String, String> {
    validate_relative_object_key(prefix, "transaction_staging_prefix")?;
    Ok(format!(
        "{prefix}/transaction-{}.pb",
        hex_digest(transaction_sha256)
    ))
}

fn object_path(base: &Path, relative_key: &str) -> Result<Path, String> {
    validate_relative_object_key(relative_key, "relative_object_key")?;
    Ok(relative_key
        .split('/')
        .fold(base.clone(), |path, segment| path.child(segment)))
}

async fn write_transaction_sidecar(
    storage: &StorageContext,
    dataset_uri: &str,
    relative_key: &str,
    transaction: &[u8],
) -> Result<(), String> {
    let (store, base) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        dataset_uri,
        &storage.params(AtomicCommitStore::for_uri(dataset_uri)?.store_params()),
    )
    .await
    .map_err(|error| format!("failed to open Lance transaction object store: {error}"))?;
    let path = object_path(&base, relative_key)?;
    store
        .put(&path, transaction)
        .await
        .map_err(|error| format!("failed to write Lance transaction sidecar: {error}"))
}

async fn read_transaction_sidecar(
    storage: &StorageContext,
    dataset_uri: &str,
    relative_key: &str,
    expected_size: usize,
) -> Result<Vec<u8>, String> {
    if expected_size == 0 || expected_size > MAX_TRANSACTION_SIDECAR_BYTES {
        return Err("Lance transaction sidecar size violates the hard limit".to_owned());
    }
    let (store, base) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        dataset_uri,
        &storage.params(AtomicCommitStore::for_uri(dataset_uri)?.store_params()),
    )
    .await
    .map_err(|error| format!("failed to open Lance transaction object store: {error}"))?;
    let path = object_path(&base, relative_key)?;
    let reader = store
        .open(&path)
        .await
        .map_err(|error| format!("failed to open Lance transaction sidecar: {error}"))?;
    let actual_size = reader
        .size()
        .await
        .map_err(|error| format!("failed to inspect Lance transaction sidecar: {error}"))?;
    if actual_size != expected_size {
        return Err(format!(
            "Lance transaction sidecar size mismatch before download: \
             expected {expected_size}, got {actual_size}"
        ));
    }
    let bytes = reader
        .get_range(0..expected_size)
        .await
        .map_err(|error| format!("failed to read Lance transaction sidecar: {error}"))?;
    if bytes.len() != expected_size {
        return Err(format!(
            "Lance transaction sidecar range size mismatch: expected {expected_size}, got {}",
            bytes.len()
        ));
    }
    Ok(bytes.to_vec())
}



fn start_writer(writer: &mut LanceCWriter) -> Result<(), String> {
    if writer.finished || writer.drained || writer.cancelled.load(Ordering::Acquire) {
        return Err("Dataset writer cannot start IO after finish or drain".into());
    }
    if writer.batch_sender.is_some() {
        return Ok(());
    }

    // Two in-flight batches provide producer/consumer overlap while bounding
    // memory independently of dataset size.
    let (sender, receiver) = sync_channel(2);
    let runtime = writer.runtime.clone();
    let uri = writer.uri.clone();
    let schema = writer.schema.clone();
    let reader_schema = schema.clone();
    let cancelled = writer.cancelled.clone();
    let operation = writer.operation;
    let expected_read_version = writer.expected_read_version;
    let expected_schema_fence = writer.schema_fence_sha256;
    let accepted_base = writer.accepted_base_snapshot.clone();
    let storage = writer.storage.clone();
    let thread = std::thread::Builder::new()
        .name("lance-ffi-writer".to_owned())
        .spawn(move || {
            let reader: Box<dyn RecordBatchReader + Send> = Box::new(ChannelBatchReader {
                receiver,
                schema: reader_schema,
                cancelled,
                cancellation_reported: false,
            });
            let store = AtomicCommitStore::for_uri(&uri)?;
            let params = WriteParams {
                mode: operation.write_mode(),
                store_params: Some(storage.params(store.store_params())),
                commit_handler: Some(Arc::new(managed::CatalogCommitHandler { accepted: accepted_base.clone(), candidate: None })),
                ..Default::default()
            };
            runtime.block_on(async move {
                let transaction = match operation {
                    LanceWriteOperation::Create => {
                        if expected_read_version != 0 {
                            return Err(format!(
                                "CREATE requires expected_read_version=0, got {expected_read_version}"
                            ));
                        }
                        InsertBuilder::new(uri.as_str())
                            .with_params(&params)
                            .execute_uncommitted_stream(reader)
                            .await
                            .map_err(|error| format!("uncommitted CREATE error: {error}"))
                    }
                    LanceWriteOperation::Append | LanceWriteOperation::Overwrite => {
                        if expected_read_version == 0 {
                            return Err(format!(
                                "{:?} requires a non-zero expected_read_version",
                                operation
                            ));
                        }
                        let fenced_dataset = managed::open(&storage, accepted_base.as_ref()
                            .ok_or("Positive Dataset write lacks its accepted base")?).await?;
                        if operation == LanceWriteOperation::Append {
                            let existing_schema: Schema = fenced_dataset.schema().into();
                            if canonical_schema_fingerprint(&existing_schema)
                                != expected_schema_fence
                            {
                                return Err(
                                    "APPEND output schema fence does not match the expected Lance snapshot"
                                        .to_owned(),
                                );
                            }
                        }
                        InsertBuilder::new(Arc::new(fenced_dataset))
                            .with_params(&params)
                            .execute_uncommitted_stream(reader)
                            .await
                            .map_err(|error| {
                                format!("uncommitted {:?} error: {error}", operation)
                            })
                    }
                    LanceWriteOperation::Unspecified => unreachable!("validated before use"),
                }
                ?;

                if transaction.read_version != expected_read_version {
                    return Err(format!(
                        "Lance returned read_version={} for expected_read_version={expected_read_version}",
                        transaction.read_version
                    ));
                }
                Ok(transaction)
            })
        })
        .map_err(|error| format!("failed to start Lance writer thread: {error}"))?;

    writer.batch_sender = Some(sender);
    writer.writer_thread = Some(thread);
    Ok(())
}

fn join_writer(writer: &mut LanceCWriter) -> Result<Option<Transaction>, String> {
    writer.batch_sender.take();
    match writer.writer_thread.take() {
        Some(thread) => match thread.join() {
            Ok(result) => {
                // The owned SDK retains and explicitly drains every writer on
                // operation errors before this thread returns its Result.
                writer.drained = true;
                if let Err(error) = &result { writer.drain_failure = Some(error.clone()); }
                result.map(Some)
            }
            Err(_) => {
                let error = "Lance writer thread panicked before SDK drain could be verified".to_owned();
                writer.drain_failure = Some(error.clone());
                Err(error)
            }
        },
        None => {
            if let Some(error) = &writer.drain_failure { return Err(error.clone()); }
            writer.drained = true;
            Ok(None)
        }
    }
}

fn parse_required_string(
    value: *const c_char,
    name: &str,
    max_len: usize,
) -> Result<String, String> {
    if value.is_null() {
        return Err(format!("{name} must not be NULL"));
    }
    let value = unsafe { CStr::from_ptr(value) }
        .to_str()
        .map_err(|error| format!("invalid UTF-8 in {name}: {error}"))?;
    if value.trim().is_empty() {
        return Err(format!("{name} must not be blank"));
    }
    if value.len() > max_len {
        return Err(format!("{name} exceeds {max_len} bytes"));
    }
    Ok(value.to_owned())
}

fn parse_sha256(
    value: *const u8,
    len: usize,
    name: &str,
) -> Result<[u8; SHA256_DIGEST_SIZE], String> {
    if value.is_null() || len != SHA256_DIGEST_SIZE {
        return Err(format!(
            "{name} must contain exactly {SHA256_DIGEST_SIZE} bytes"
        ));
    }
    let mut digest = [0_u8; SHA256_DIGEST_SIZE];
    digest.copy_from_slice(unsafe { slice::from_raw_parts(value, len) });
    Ok(digest)
}

fn parse_task_attempt(value: *const u8, len: usize) -> Result<CanonicalTaskAttemptId, String> {
    if value.is_null() || len == 0 || len > 64 * 1024 {
        return Err("task_attempt must contain 1..65536 protobuf bytes".to_owned());
    }
    let attempt = CanonicalTaskAttemptId::decode(unsafe { slice::from_raw_parts(value, len) })
        .map_err(|error| format!("invalid canonical TaskAttemptId protobuf: {error}"))?;
    attempt.validate()?;
    Ok(attempt)
}

fn sha256(value: &[u8]) -> Vec<u8> {
    Sha256::digest(value).to_vec()
}

fn into_c_buffer(mut bytes: Vec<u8>) -> LanceCBuffer {
    let buffer = LanceCBuffer {
        data: bytes.as_mut_ptr(),
        len: bytes.len(),
        capacity: bytes.capacity(),
    };
    std::mem::forget(bytes);
    buffer
}

fn transaction_bytes(transaction: &Transaction) -> Vec<u8> {
    lance_pb::Transaction::from(transaction).encode_to_vec()
}

fn decode_transaction(bytes: &[u8]) -> Result<Transaction, String> {
    let message = lance_pb::Transaction::decode(bytes)
        .map_err(|error| format!("invalid Lance transaction protobuf: {error}"))?;
    Transaction::try_from(message).map_err(|error| format!("invalid Lance transaction: {error}"))
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn operation_fragments(operation: &Operation) -> Result<&[lance_table::format::Fragment], String> {
    match operation {
        Operation::Append { fragments } | Operation::Overwrite { fragments, .. } => Ok(fragments),
        _ => Err("Lance SQL table writes only accept Append or Overwrite transactions".to_owned()),
    }
}

fn transaction_row_count(transaction: &Transaction) -> Result<u64, String> {
    operation_fragments(&transaction.operation)?
        .iter()
        .try_fold(0_u64, |total, fragment| {
            let rows = fragment
                .physical_rows
                .ok_or_else(|| "Lance transaction fragment is missing physical_rows".to_owned())?;
            let rows = u64::try_from(rows)
                .map_err(|_| "Lance fragment row count exceeds u64".to_owned())?;
            total
                .checked_add(rows)
                .ok_or_else(|| "Lance transaction row count overflow".to_owned())
        })
}

fn remaining_commit_time(deadline_epoch_ms: u64) -> Result<Duration, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("invalid system clock: {error}"))?
        .as_millis();
    let remaining = u128::from(deadline_epoch_ms)
        .checked_sub(now)
        .filter(|value| *value > 0)
        .ok_or_else(|| "Lance publication deadline expired".to_owned())?;
    Ok(Duration::from_millis(u64::try_from(remaining).map_err(
        |_| "Lance publication deadline exceeds supported range",
    )?))
}

struct ValidatedFragmentReference {
    fragment: LanceWriteFragment,
    transaction_digest: Vec<u8>,
    relative_object_key: String,
    transaction_size: usize,
}

fn validate_fragment_reference(
    bytes: &[u8],
    expected_staging_prefix: &str,
    dataset_uri: &str,
    write_id: &str,
    tenant_id: &str,
    generation: &str,
    operation: LanceWriteOperation,
    expected_read_version: u64,
    schema_fence_sha256: &[u8; SHA256_DIGEST_SIZE],
) -> Result<ValidatedFragmentReference, String> {
    validate_relative_object_key(expected_staging_prefix, "expected_staging_prefix")?;
    if bytes.is_empty() || bytes.len() > MAX_INLINE_FRAGMENT_BYTES {
        return Err("Lance write fragment is empty or exceeds the 64 KiB hard limit".to_owned());
    }
    let fragment = LanceWriteFragment::decode(bytes)
        .map_err(|error| format!("invalid Lance write fragment protobuf: {error}"))?;
    if fragment.contract_version != LANCE_WRITE_FRAGMENT_VERSION {
        return Err(format!(
            "unsupported Lance write fragment version: {}",
            fragment.contract_version
        ));
    }
    if fragment.dataset_uri != dataset_uri
        || fragment.write_id != write_id
        || LanceWriteOperation::parse(fragment.operation)? != operation
        || fragment.read_version != expected_read_version
        || fragment.schema_fence_sha256 != schema_fence_sha256.as_slice()
    {
        return Err(
            "Lance write fragment does not match the connector-owned commit fence".to_owned(),
        );
    }
    let task_attempt = fragment
        .task_attempt
        .as_ref()
        .ok_or_else(|| "Lance write fragment is missing task_attempt".to_owned())?;
    task_attempt.validate()?;
    let reference = fragment
        .transaction
        .as_ref()
        .ok_or_else(|| "Lance write fragment is missing transaction reference".to_owned())?;
    let checksum = reference
        .sha256
        .as_ref()
        .ok_or_else(|| "Lance transaction reference is missing SHA-256".to_owned())?;
    if checksum.algorithm != ChecksumAlgorithm::Sha256 as i32
        || checksum.digest.len() != SHA256_DIGEST_SIZE
    {
        return Err("Lance transaction reference has an invalid SHA-256".to_owned());
    }
    let expected_key = transaction_object_key(expected_staging_prefix, &checksum.digest)?;
    if reference.tenant_id != tenant_id
        || reference.write_id != write_id
        || reference.generation != generation
        || reference.task_attempt.as_ref() != Some(task_attempt)
        || reference.relative_object_key != expected_key
    {
        return Err(
            "Lance transaction reference violates the connector-owned namespace fence".to_owned(),
        );
    }
    let transaction_size = usize::try_from(reference.size_bytes)
        .map_err(|_| "Lance transaction sidecar size exceeds the platform limit")?;
    if transaction_size == 0 || transaction_size > MAX_TRANSACTION_SIDECAR_BYTES {
        return Err("Lance transaction sidecar size violates the 64 MiB hard limit".to_owned());
    }
    let transaction_digest = checksum.digest.clone();
    Ok(ValidatedFragmentReference {
        fragment,
        transaction_digest,
        relative_object_key: expected_key,
        transaction_size,
    })
}





// =============================================================================
// Error handling
// =============================================================================

// Thread-local last error message for C callers to retrieve.
thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<CString>> = std::cell::RefCell::new(None);
}

fn set_last_error(msg: &str) {
    let c_msg = CString::new(msg).unwrap_or_else(|_| CString::new("unknown error").unwrap());
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(c_msg);
    });
}

fn panic_message(operation: &str, payload: Box<dyn std::any::Any + Send>) -> String {
    let detail = if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_owned()
    };
    format!("Lance C FFI {operation} panicked: {detail}")
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

/// Exact ABI implemented by this pinned Rust crate.
#[no_mangle]
pub extern "C" fn lance_ffi_abi_version() -> u32 {
    13
}

// =============================================================================
// Runtime management
// =============================================================================

/// One runtime per active native lifecycle. Open handles retain an Arc so
/// shutdown cannot invalidate a reader or writer still using the runtime.
static GLOBAL_RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);

fn new_runtime() -> Arc<Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("lance-ffi-rt")
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for Lance C FFI"),
    )
}

fn get_or_create_runtime() -> Arc<Runtime> {
    let mut runtime = GLOBAL_RUNTIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    runtime.get_or_insert_with(new_runtime).clone()
}

/// Initialize the Lance FFI runtime eagerly (see lance_c_ffi.h contract).
/// Forces the lazy global Tokio runtime to spin up so later reader/writer
/// calls do not pay the one-time startup cost. Idempotent.
#[no_mangle]
pub extern "C" fn lance_ffi_runtime_init() -> i32 {
    // Runtime construction may panic on thread startup failure; surface an
    // error code instead of unwinding across the C boundary.
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

/// Release the global runtime reference. Any open reader/writer retains its
/// own reference until closed. A subsequent init creates a fresh runtime.
#[no_mangle]
pub extern "C" fn lance_ffi_runtime_shutdown() {
    let runtime = GLOBAL_RUNTIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    drop(runtime);
}

// =============================================================================
// Reader API
// =============================================================================

/// Open a Lance dataset for reading.
///
/// # Parameters
/// - `uri`: null-terminated path to the Lance dataset (local or object store URI)
/// - `version`: exact, non-zero catalog-pinned Lance version
///
/// # Returns
/// - Opaque reader handle on success, NULL on failure (check lance_ffi_last_error)
#[no_mangle]
pub extern "C" fn lance_ffi_reader_open(
    storage: *const LanceCStorage,
    uri: *const c_char,
    version: u64,
    file_detail: *const u8,
    file_detail_len: usize,
    physical_file_size: u64,
) -> *mut LanceCReader {
    ffi_guard!("reader_open", ptr::null_mut(), {
        if uri.is_null() {
            set_last_error("uri must not be NULL");
            return ptr::null_mut();
        }
        if version == 0 {
            set_last_error("Lance read version must be non-zero");
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
        let storage = require_storage!(storage, &uri_str, None, ptr::null_mut());
        if file_detail.is_null() || file_detail_len == 0 || file_detail_len > 64 * 1024 {
            set_last_error("Reader requires a bounded canonical FileSplitDetail read unit");
            return ptr::null_mut();
        }
        let detail = match FileSplitDetail::decode(unsafe {
            std::slice::from_raw_parts(file_detail, file_detail_len)
        }) {
            Ok(value) => value,
            Err(_) => {
                set_last_error("Malformed canonical FileSplitDetail");
                return ptr::null_mut();
            }
        };
        if detail.file_format != FileFormat::Lance as i32 {
            set_last_error("Lance reader requires exact FILE_FORMAT_LANCE");
            return ptr::null_mut();
        }
        let source = match detail.read_unit {
            Some(ReadUnit::PhysicalFileScan(scan)) => {
                match runtime.block_on(physical::PhysicalFileReader::open(
                    &storage,
                    &uri_str,
                    physical_file_size,
                    scan,
                )) {
                    Ok(reader) => LanceReadSource::Physical(reader),
                    Err(error) => {
                        set_last_error(&error);
                        return ptr::null_mut();
                    }
                }
            }
            Some(ReadUnit::ScanPartition(partition)) => {
                if physical_file_size != 0 {
                    set_last_error("Dataset manifest partition is not a physical file byte range");
                    return ptr::null_mut();
                }
                let reference = match partition.accepted_snapshot.as_ref() {
                    Some(value) => value,
                    None => { set_last_error("Dataset reader requires accepted source membership"); return ptr::null_mut(); }
                };
                if let Err(error) = managed::validate(reference, &uri_str, version) {
                    set_last_error(&error); return ptr::null_mut();
                }
                let dataset = match runtime.block_on(managed::open(&storage, reference)) {
                    Ok(dataset) => dataset,
                    Err(error) => { set_last_error(&error); return ptr::null_mut(); }
                };
                let partition = match runtime.block_on(scan::select(
                    &storage, &uri_str, version, &dataset, &partition,
                )) {
                    Ok(value) => value,
                    Err(error) => {
                        set_last_error(&error);
                        return ptr::null_mut();
                    }
                };
                LanceReadSource::Dataset { dataset, partition }
            }
            None => {
                set_last_error("Lance reader requires one explicit read unit");
                return ptr::null_mut();
            }
        };

        let reader = Box::new(LanceCReader {
            runtime,
            source,
            projected_columns: None,
            batch_stream: None,
            pending_batch: None,
            scan_started: false,
            returned_rows: 0,
            last_batch_row_start: 0,
            last_partition_row_offset: None,
        });
        Box::into_raw(reader)
    })
}

#[no_mangle]
pub extern "C" fn lance_ffi_scan_partitions(
    storage: *const LanceCStorage,
    request: *const u8,
    request_len: usize,
    out_page: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("scan_partitions", -1, {
        if storage.is_null()
            || request.is_null()
            || request_len == 0
            || request_len > 64 * 1024
            || out_page.is_null()
        {
            set_last_error(
                "Dataset partition enumeration requires bounded request and storage authority",
            );
            return -1;
        }
        let context = &unsafe { &*storage }.context;
        let bytes = unsafe { std::slice::from_raw_parts(request, request_len) };
        match get_or_create_runtime().block_on(scan::enumerate(context, bytes)) {
            Ok(bytes) => {
                unsafe {
                    ptr::write(out_page, into_c_buffer(bytes));
                }
                0
            }
            Err(error) => {
                set_last_error(&error);
                -1
            }
        }
    })
}

/// Compute the canonical schema fence used by Lance write bindings.
#[no_mangle]
pub extern "C" fn lance_ffi_schema_fingerprint(
    schema_ptr: *const FFI_ArrowSchema,
    out_sha256: *mut u8,
    out_len: usize,
) -> i32 {
    ffi_guard!("schema_fingerprint", -1, {
        if schema_ptr.is_null() || out_sha256.is_null() || out_len != SHA256_DIGEST_SIZE {
            set_last_error("schema_ptr and 32-byte out_sha256 are required");
            return -1;
        }
        let schema = match Schema::try_from(unsafe { &*schema_ptr }) {
            Ok(schema) => schema,
            Err(error) => {
                set_last_error(&format!("failed to import schema for fingerprint: {error}"));
                return -1;
            }
        };
        let fingerprint = canonical_schema_fingerprint(&schema);
        unsafe {
            ptr::copy_nonoverlapping(fingerprint.as_ptr(), out_sha256, fingerprint.len());
        }
        0
    })
}

/// Set column projection on the reader. Call before lance_ffi_reader_next.
///
/// # Parameters
/// - `columns`: array of null-terminated column names
/// - `column_count`: number of entries in `columns`
#[no_mangle]
pub extern "C" fn lance_ffi_reader_set_projection(
    reader: *mut LanceCReader,
    columns: *const *const c_char,
    column_count: usize,
) -> i32 {
    ffi_guard!("reader_set_projection", -1, {
        if reader.is_null() || columns.is_null() || column_count == 0 {
            set_last_error("reader and columns must not be NULL and column_count must be positive");
            return -1;
        }
        let reader = unsafe { &mut *reader };
        if reader.scan_started {
            set_last_error("projection cannot change after scanning starts");
            return -1;
        }
        let mut projected_columns = Vec::with_capacity(column_count);
        for index in 0..column_count {
            let column = unsafe { *columns.add(index) };
            if column.is_null() {
                set_last_error(&format!("column at index {index} must not be NULL"));
                return -1;
            }
            let column = match unsafe { CStr::from_ptr(column) }.to_str() {
                Ok(value) if !value.is_empty() => value.to_owned(),
                Ok(_) => {
                    set_last_error(&format!("column at index {index} must not be empty"));
                    return -1;
                }
                Err(error) => {
                    set_last_error(&format!(
                        "invalid UTF-8 in column at index {index}: {error}"
                    ));
                    return -1;
                }
            };
            if projected_columns.contains(&column) {
                set_last_error(&format!("duplicate projection column: {column}"));
                return -1;
            }
            projected_columns.push(column);
        }
        reader.projected_columns = Some(projected_columns);
        reader.batch_stream = None;
        0
    })
}

/// Read the next batch from the dataset, exporting via Arrow C Data Interface.
///
/// # Parameters
/// - `out_array`: pointer to caller-allocated FFI_ArrowArray (populated on success)
/// - `out_schema`: pointer to caller-allocated FFI_ArrowSchema (populated on success)
/// - `max_rows`: maximum rows to return (0 = default 8192, hard limit 1,048,576)
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
    ffi_guard!("reader_next", -1, {
        if reader.is_null() || out_array.is_null() || out_schema.is_null() {
            set_last_error("reader, out_array, and out_schema must not be NULL");
            return -1;
        }
        let reader = unsafe { &mut *reader };

        if max_rows > MAX_BATCH_ROWS {
            set_last_error(&format!(
                "max_rows exceeds the Lance FFI limit of {MAX_BATCH_ROWS}"
            ));
            return -1;
        }
        let requested_rows = if max_rows == 0 {
            8192
        } else {
            max_rows as usize
        };

        // Lazily initialize the scan stream. The stream batch size is stable;
        // pending_batch enforces each caller's current limit without rebuilding
        // the scan or discarding rows when downstream backpressure changes.
        if reader.batch_stream.is_none() {
            let stream = match &reader.source {
                LanceReadSource::Physical(file) => {
                    let _runtime_context = reader.runtime.enter();
                    match file.start(reader.projected_columns.as_deref()) {
                        Ok(stream) => stream,
                        Err(error) => {
                            set_last_error(&error);
                            return -1;
                        }
                    }
                }
                LanceReadSource::Dataset { dataset, partition } => {
                    match reader.runtime.block_on(scan::start_dataset_scan(
                        dataset, partition, reader.projected_columns.as_deref(),
                    )) {
                        Ok(stream) => stream,
                        Err(error) => {
                            set_last_error(&error);
                            return -1;
                        }
                    }
                }
            };
            reader.batch_stream = Some(stream);
            reader.scan_started = true;
        }

        // Pull one batch at a time. Retaining the asynchronous stream bounds memory
        // to the execution pipeline instead of collecting the complete split.
        let next_batch = if let Some(pending) = reader.pending_batch.take() {
            Some(Ok(pending))
        } else {
            let runtime = reader.runtime.clone();
            let Some(stream) = reader.batch_stream.as_mut() else {
                set_last_error("scan stream was not initialized");
                return -1;
            };
            runtime.block_on(stream.next())
        };
        match next_batch {
            Some(Ok(batch)) => {
                let (batch, remainder) = scan::bounded_batch(batch, requested_rows);
                let returned_rows = batch.num_rows();
                let batch = match &reader.source {
                    LanceReadSource::Dataset { partition, .. }
                        if scan_identity::requested(reader.projected_columns.as_deref()) =>
                    {
                        match scan_identity::materialize(
                            &batch,
                            partition,
                            reader.projected_columns.as_ref().unwrap(),
                            &mut reader.last_partition_row_offset,
                        ) {
                            Ok(batch) => batch,
                            Err(error) => {
                                set_last_error(&error);
                                return -1;
                            }
                        }
                    }
                    _ => batch,
                };
                let struct_array = arrow::array::StructArray::from(batch);
                match arrow::ffi::to_ffi(&struct_array.into_data()) {
                    Ok((ffi_array, ffi_schema)) => {
                        reader.pending_batch = remainder;
                        if let LanceReadSource::Physical(file) = &reader.source {
                            reader.last_batch_row_start = file.row_start() + reader.returned_rows;
                            reader.returned_rows += returned_rows as u64;
                        }
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
            Some(Err(e)) => {
                set_last_error(&format!("scan error: {e}"));
                -1
            }
            None => 0,
        }
    })
}

/// Close and free the reader handle.
#[no_mangle]
pub extern "C" fn lance_ffi_reader_close(reader: *mut LanceCReader) {
    ffi_guard!("reader_close", (), {
        if !reader.is_null() {
            unsafe {
                drop(Box::from_raw(reader));
            }
        }
    })
}

// =============================================================================
// Writer API
// =============================================================================

/// Create a task-attempt-private uncommitted Lance writer.
///
/// # Parameters
/// - `uri`: null-terminated path for the output Lance dataset
/// - `schema_ptr`: Arrow C Data Interface schema describing the output columns
/// - `operation`: exact LanceWriteOperation value; unspecified is rejected
/// - `expected_read_version`: 0 only for CREATE, non-zero otherwise
/// - `schema_fence_sha256`: connector-issued 32-byte schema fence
/// - `write_id`: coordinator-issued idempotency identity
/// - `task_attempt`: deterministic canonical TaskAttemptId protobuf bytes
///
/// # Returns
/// - Opaque writer handle on success, NULL on failure
#[no_mangle]
pub extern "C" fn lance_ffi_writer_open(
    storage: *const LanceCStorage,
    uri: *const c_char,
    schema_ptr: *const FFI_ArrowSchema,
    operation: i32,
    expected_read_version: u64,
    accepted_base_snapshot: *const u8,
    accepted_base_snapshot_len: usize,
    schema_fence_sha256: *const u8,
    schema_fence_len: usize,
    write_id: *const c_char,
    tenant_id: *const c_char,
    generation: *const c_char,
    transaction_staging_prefix: *const c_char,
    task_attempt: *const u8,
    task_attempt_len: usize,
) -> *mut LanceCWriter {
    ffi_guard!("writer_open", ptr::null_mut(), {
        if uri.is_null() || schema_ptr.is_null() {
            set_last_error("uri and schema_ptr must not be NULL");
            return ptr::null_mut();
        }
        let uri_str = match parse_required_string(uri, "uri", 8192) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return ptr::null_mut();
            }
        };
        if let Err(error) = AtomicCommitStore::for_uri(&uri_str) {
            set_last_error(&error);
            return ptr::null_mut();
        }
        let operation = match LanceWriteOperation::parse(operation) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return ptr::null_mut();
            }
        };
        if (operation == LanceWriteOperation::Create && expected_read_version != 0)
            || (operation != LanceWriteOperation::Create && expected_read_version == 0)
        {
            set_last_error(
                "CREATE requires version 0; APPEND/OVERWRITE require a non-zero version",
            );
            return ptr::null_mut();
        }
        let schema_fence_sha256 =
            match parse_sha256(schema_fence_sha256, schema_fence_len, "schema_fence_sha256") {
                Ok(value) => value,
                Err(error) => {
                    set_last_error(&error);
                    return ptr::null_mut();
                }
            };
        let write_id = match parse_required_string(write_id, "write_id", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return ptr::null_mut();
            }
        };
        let tenant_id = match parse_required_string(tenant_id, "tenant_id", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return ptr::null_mut();
            }
        };
        let generation = match parse_required_string(generation, "generation", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return ptr::null_mut();
            }
        };
        let transaction_staging_prefix = match parse_required_string(
            transaction_staging_prefix,
            "transaction_staging_prefix",
            2048,
        ) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return ptr::null_mut();
            }
        };
        if let Err(error) =
            validate_relative_object_key(&transaction_staging_prefix, "transaction_staging_prefix")
        {
            set_last_error(&error);
            return ptr::null_mut();
        }
        let task_attempt = match parse_task_attempt(task_attempt, task_attempt_len) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
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
        let actual_schema_fence = canonical_schema_fingerprint(schema.as_ref());
        if actual_schema_fence != schema_fence_sha256 {
            set_last_error("output Arrow schema does not match schema_fence_sha256");
            return ptr::null_mut();
        }

        let base = if expected_read_version == 0 {
            if accepted_base_snapshot_len != 0 {
                set_last_error("CREATE Dataset write must not carry an accepted base");
                return ptr::null_mut();
            }
            None
        } else {
            if accepted_base_snapshot.is_null() || accepted_base_snapshot_len == 0 || accepted_base_snapshot_len > 64 * 1024 {
                set_last_error("Positive Dataset write requires bounded accepted base metadata");
                return ptr::null_mut();
            }
            let bytes = unsafe { slice::from_raw_parts(accepted_base_snapshot, accepted_base_snapshot_len) };
            let reference = match canonical::pangrova::protocol::v2::AcceptedSourceSnapshotRef::decode(bytes) {
                Ok(reference) => reference,
                Err(error) => { set_last_error(&error.to_string()); return ptr::null_mut(); }
            };
            if reference.encode_to_vec() != bytes || reference.tenant_id != tenant_id
                || reference.physical_schema_sha256.as_ref().is_none_or(|digest| digest.digest != schema_fence_sha256)
            {
                set_last_error("Dataset accepted base tenant/schema or canonical encoding differs");
                return ptr::null_mut();
            }
            if let Err(error) = managed::validate(&reference, &uri_str, expected_read_version) {
                set_last_error(&error); return ptr::null_mut();
            }
            Some(reference)
        };
        let context = require_storage!(storage, &uri_str, Some(&tenant_id), ptr::null_mut());
        if let Some(reference) = &base {
            if let Err(error) = context.require_binding(reference.catalog.as_ref().unwrap(), &tenant_id) {
                set_last_error(&error); return ptr::null_mut();
            }
        }
        let runtime = get_or_create_runtime();
        let writer = Box::new(LanceCWriter {
            runtime,
            storage: context,
            uri: uri_str,
            schema,
            operation,
            expected_read_version,
            accepted_base_snapshot: base,
            schema_fence_sha256,
            write_id,
            tenant_id,
            generation,
            transaction_staging_prefix,
            task_attempt,
            batch_sender: None,
            writer_thread: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            finished: false,
            drained: false,
            drain_failure: None,
            rows_written: 0,
            input_bytes: 0,
        });
        Box::into_raw(writer)
    })
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
    ffi_guard!("writer_write_batch", -1, {
        if array_ptr.is_null() {
            set_last_error("array_ptr must not be NULL");
            return -1;
        }
        // Consume and clear the caller slot even when a later guard fails.
        // The local owner releases exactly once on every import/error path.
        let ffi_array = unsafe { std::mem::replace(&mut *array_ptr, FFI_ArrowArray::empty()) };
        if writer.is_null() || schema_ptr.is_null() {
            set_last_error("writer, array_ptr, and schema_ptr must not be NULL");
            return -1;
        }
        let writer = unsafe { &mut *writer };
        if writer.finished || writer.drained || writer.cancelled.load(Ordering::Acquire) {
            set_last_error("Dataset writer refuses a batch after finish or drain");
            return -1;
        }
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

        if !matches!(data.data_type(), arrow_schema::DataType::Struct(_)) {
            set_last_error("Dataset write input must be a complete Arrow struct");
            return -1;
        }
        let struct_array = arrow::array::StructArray::from(data);
        let batch = RecordBatch::from(struct_array);
        if batch.schema().as_ref() != writer.schema.as_ref() {
            set_last_error("batch schema differs from writer schema");
            return -1;
        }
        if let Err(error) = start_writer(writer) {
            set_last_error(&error);
            return -1;
        }
        let Some(sender) = writer.batch_sender.as_ref() else {
            set_last_error("Lance writer channel was not initialized");
            return -1;
        };
        let rows = match u64::try_from(batch.num_rows()) {
            Ok(value) => value,
            Err(_) => {
                set_last_error("batch row count exceeds u64");
                return -1;
            }
        };
        let input_bytes = match u64::try_from(batch.get_array_memory_size()) {
            Ok(value) => value,
            Err(_) => {
                set_last_error("batch memory size exceeds u64");
                return -1;
            }
        };
        match sender.send(batch) {
            Ok(()) => {
                writer.rows_written = match writer.rows_written.checked_add(rows) {
                    Some(value) => value,
                    None => {
                        set_last_error("writer row count overflow");
                        return -1;
                    }
                };
                writer.input_bytes = match writer.input_bytes.checked_add(input_bytes) {
                    Some(value) => value,
                    None => {
                        set_last_error("writer byte count overflow");
                        return -1;
                    }
                };
                0
            }
            Err(_) => {
                set_last_error("Lance writer stopped before accepting the batch");
                -1
            }
        }
    })
}

/// Flush all buffered batches without publishing a dataset manifest.
///
/// # Returns
/// - `out_fragment`: canonical typed commit metadata owned by the caller
///
/// # Returns
/// - 0 on success, -1 on error
#[no_mangle]
pub extern "C" fn lance_ffi_writer_finish(
    writer: *mut LanceCWriter,
    out_fragment: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("writer_finish", -1, {
        if writer.is_null() || out_fragment.is_null() {
            set_last_error("writer and out_fragment must not be NULL");
            return -1;
        }
        unsafe {
            ptr::write(
                out_fragment,
                LanceCBuffer {
                    data: ptr::null_mut(),
                    len: 0,
                    capacity: 0,
                },
            );
        }
        let writer = unsafe { &mut *writer };
        if writer.finished || writer.drained || writer.cancelled.load(Ordering::Acquire) {
            set_last_error("Dataset writer refuses finish after finish or drain");
            return -1;
        }
        // Start the consumer even when no batches were submitted so a successful
        // finish publishes a valid zero-row dataset with the declared schema.
        if let Err(error) = start_writer(writer) {
            set_last_error(&error);
            return -1;
        }
        match join_writer(writer) {
            Ok(Some(transaction)) => {
                let lance_transaction = transaction_bytes(&transaction);
                if lance_transaction.is_empty()
                    || lance_transaction.len() > MAX_TRANSACTION_SIDECAR_BYTES
                {
                    set_last_error("Lance transaction sidecar violates the 64 MiB hard limit");
                    return -1;
                }
                let transaction_digest = sha256(&lance_transaction);
                let relative_object_key = match transaction_object_key(
                    &writer.transaction_staging_prefix,
                    &transaction_digest,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        set_last_error(&error);
                        return -1;
                    }
                };
                let transaction_reference = LanceTransactionReference {
                    relative_object_key: relative_object_key.clone(),
                    generation: writer.generation.clone(),
                    size_bytes: lance_transaction.len() as u64,
                    sha256: Some(CanonicalChecksum {
                        algorithm: ChecksumAlgorithm::Sha256 as i32,
                        digest: transaction_digest.clone(),
                    }),
                    tenant_id: writer.tenant_id.clone(),
                    write_id: writer.write_id.clone(),
                    task_attempt: Some(writer.task_attempt.clone()),
                };
                let fragment = LanceWriteFragment {
                    contract_version: LANCE_WRITE_FRAGMENT_VERSION,
                    dataset_uri: writer.uri.clone(),
                    write_id: writer.write_id.clone(),
                    task_attempt: Some(writer.task_attempt.clone()),
                    operation: writer.operation as i32,
                    read_version: transaction.read_version,
                    schema_fence_sha256: writer.schema_fence_sha256.to_vec(),
                    transaction: Some(transaction_reference),
                    rows_written: writer.rows_written,
                    input_bytes: writer.input_bytes,
                };
                let encoded = fragment.encode_to_vec();
                if encoded.len() > MAX_INLINE_FRAGMENT_BYTES {
                    set_last_error("Lance write fragment exceeds the 64 KiB control-plane limit");
                    return -1;
                }
                if let Err(error) = writer.runtime.block_on(write_transaction_sidecar(
                    &writer.storage,
                    &writer.uri,
                    &relative_object_key,
                    &lance_transaction,
                )) {
                    set_last_error(&error);
                    return -1;
                }
                unsafe {
                    ptr::write(out_fragment, into_c_buffer(encoded));
                }
                writer.finished = true;
                0
            }
            Ok(None) => {
                set_last_error("writer thread did not produce a transaction");
                -1
            }
            Err(error) => {
                set_last_error(&error);
                -1
            }
        }
    })
}

/// Free an owned buffer returned by this library.
#[no_mangle]
pub extern "C" fn lance_ffi_buffer_free(buffer: LanceCBuffer) {
    ffi_guard!("buffer_free", (), {
        if buffer.data.is_null() {
            return;
        }
        unsafe {
            drop(Vec::from_raw_parts(
                buffer.data,
                buffer.len,
                buffer.capacity,
            ));
        }
    })
}

/// Prepare an immutable SDK manifest at its reserved private path. This cannot publish source visibility.
#[no_mangle]
pub extern "C" fn lance_ffi_prepare_write(
    storage: *const LanceCStorage,
    request_bytes: *const u8,
    request_len: usize,
    out_result: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("prepare_write", -1, {
        if request_bytes.is_null() || request_len == 0 || request_len > 16 * 1024 * 1024 || out_result.is_null() {
            set_last_error("Canonical bounded preparation request and output are required");
            return -1;
        }
        unsafe { *out_result = LanceCBuffer { data: ptr::null_mut(), len: 0, capacity: 0 }; }
        let bytes = unsafe { slice::from_raw_parts(request_bytes, request_len) };
        let request = match canonical::pangrova::protocol::v2::PrepareDistributedWriteRequest::decode(bytes) {
            Ok(value) if value.encode_to_vec() == bytes => value,
            _ => { set_last_error("Preparation request is not canonical protobuf"); return -1; }
        };
        let uri = match request.output_binding.as_ref().and_then(|output| output.binding.as_ref()) {
            Some(canonical::pangrova::protocol::v2::write_output_binding::Binding::Lance(binding)) => binding.dataset_uri.clone(),
            _ => { set_last_error("Preparation requires a Lance allocation"); return -1; }
        };
        let tenant = match request.security.as_ref() {
            Some(security) => security.tenant_id.clone(),
            None => { set_last_error("Preparation requires security"); return -1; }
        };
        if let Err(error) = remaining_commit_time(request.deadline_epoch_ms) {
            set_last_error(&error); return -1;
        }
        if request.invocation_id.len() != 32 || !request.invocation_id.bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)) {
            set_last_error("Preparation requires a durable invocation nonce"); return -1;
        }
        let fingerprint = request.commit_fingerprint.clone();
        let invocation_id = request.invocation_id.clone();
        // An RPC deadline only bounds admission. Dropping the SDK future on timeout
        // cannot prove object-store calls have drained. Wait for its actual return;
        // a lost HTTP reply leaves the durable issuance held for later reconciliation.
        let result = get_or_create_runtime().block_on(async {
            let scoped = scoped_storage(storage, &uri, Some(&tenant))?;
            preparation::prepare(&scoped, request).await
        });
        let mut result = match result {
            Ok(result) => result,
            Err(error) => {
                set_last_error(&error);
                canonical::pangrova::protocol::v2::PrepareDistributedWriteResult {
                    outcome: canonical::pangrova::protocol::v2::WritePreparationOutcome::Unknown as i32,
                    commit_fingerprint: fingerprint, publication: None, object_page: None,
                    invocation_id: invocation_id.clone(), sdk_joined: false,
                }
            }
        };
        result.invocation_id = invocation_id;
        unsafe { *out_result = into_c_buffer(result.encode_to_vec()); }
        0
    })
}

/// Actual joined deletion of the exact catalog-reserved object page.
#[no_mangle]
pub extern "C" fn lance_ffi_cleanup_objects(
    storage: *const LanceCStorage, request_bytes: *const u8, request_len: usize, out_result: *mut LanceCBuffer,
) -> i32 {
    ffi_guard!("cleanup_objects", -1, {
        if request_bytes.is_null() || request_len == 0 || request_len > 8 * 1024 * 1024 || out_result.is_null() {
            set_last_error("Canonical bounded cleanup request and output are required"); return -1;
        }
        unsafe { *out_result = LanceCBuffer { data: ptr::null_mut(), len: 0, capacity: 0 }; }
        let bytes = unsafe { slice::from_raw_parts(request_bytes, request_len) };
        let request = match canonical::pangrova::protocol::v2::CleanupSourceObjectsRequest::decode(bytes) {
            Ok(value) if value.encode_to_vec() == bytes => value,
            _ => { set_last_error("Cleanup request is not canonical protobuf"); return -1; }
        };
        if let Err(error) = remaining_commit_time(request.deadline_epoch_ms) { set_last_error(&error); return -1; }
        let tenant = request.security.as_ref().map(|value| value.tenant_id.clone()).unwrap_or_default();
        let uri = request.dataset_uri.clone();
        match get_or_create_runtime().block_on(async {
            let scoped = scoped_storage(storage, &uri, Some(&tenant))?;
            cleanup::cleanup(&scoped, request).await
        }) {
            Ok(result) => { unsafe { *out_result = into_c_buffer(result.encode_to_vec()); } 0 }
            Err(error) => { set_last_error(&error); -1 }
        }
    })
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
    ffi_guard!("reader_schema", -1, {
        if reader.is_null() || out_schema.is_null() {
            set_last_error("reader and out_schema must not be NULL");
            return -1;
        }
        let reader = unsafe { &*reader };
        let arrow_schema: Schema = match &reader.source {
            LanceReadSource::Dataset { dataset, .. } => dataset.schema().into(),
            LanceReadSource::Physical(file) => file.schema(),
        };

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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance::dataset::CommitBuilder;
    use lance_table::io::commit::CommitHandler;
    use std::process::Command;
    use std::time::Duration;

    struct MinioContainer {
        id: String,
        endpoint: String,
    }

    impl MinioContainer {
        fn start(image: &str) -> Self {
            let output = Command::new("docker")
                .args([
                    "run",
                    "-d",
                    "--rm",
                    "-p",
                    "127.0.0.1::9000",
                    "-e",
                    "MINIO_ROOT_USER=minioadmin",
                    "-e",
                    "MINIO_ROOT_PASSWORD=minioadmin",
                    image,
                    "server",
                    "/data",
                ])
                .output()
                .expect("Docker is required for the Lance S3 commit test");
            assert!(
                output.status.success(),
                "MinIO container failed to start: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let id = String::from_utf8(output.stdout).unwrap().trim().to_owned();
            let container = Self {
                id,
                endpoint: String::new(),
            };
            let port_output = Command::new("docker")
                .args(["port", &container.id, "9000/tcp"])
                .output()
                .expect("cannot inspect MinIO port");
            assert!(port_output.status.success(), "cannot inspect MinIO port");
            let port = String::from_utf8(port_output.stdout)
                .unwrap()
                .trim()
                .rsplit(':')
                .next()
                .unwrap()
                .parse::<u16>()
                .expect("invalid MinIO host port");
            let mut container = container;
            container.endpoint = format!("http://127.0.0.1:{port}");
            let mut last_error = String::new();
            for _ in 0..80 {
                let alias = Command::new("docker")
                    .args([
                        "exec",
                        &container.id,
                        "mc",
                        "alias",
                        "set",
                        "pangrova",
                        "http://127.0.0.1:9000",
                        "minioadmin",
                        "minioadmin",
                    ])
                    .output()
                    .expect("cannot execute MinIO client");
                if alias.status.success() {
                    return container;
                }
                last_error = String::from_utf8_lossy(&alias.stderr).into_owned();
                std::thread::sleep(Duration::from_millis(250));
            }
            panic!("MinIO did not become ready: {last_error}");
        }

        fn create_bucket(&self, bucket: &str) {
            let target = format!("pangrova/{bucket}");
            let output = Command::new("docker")
                .args(["exec", &self.id, "mc", "mb", &target])
                .output()
                .expect("cannot create MinIO bucket");
            assert!(
                output.status.success(),
                "MinIO bucket creation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    impl Drop for MinioContainer {
        fn drop(&mut self) {
            let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
        }
    }

    #[test]
    fn remote_commit_selects_conditional_put_without_default_unsafe_handler() {
        assert_eq!(
            AtomicCommitStore::for_uri("/tmp/vectors.lance").unwrap(),
            AtomicCommitStore::Local
        );
        assert_eq!(
            AtomicCommitStore::for_uri("file:///tmp/vectors.lance").unwrap(),
            AtomicCommitStore::Local
        );
        let s3 = AtomicCommitStore::for_uri("s3://warehouse/vectors.lance").unwrap();
        assert_eq!(s3, AtomicCommitStore::S3ConditionalPut);
        assert_eq!(
            s3.store_params()
                .storage_options
                .unwrap()
                .get("aws_conditional_put"),
            Some(&"etag".to_owned())
        );
        assert!(AtomicCommitStore::for_uri("s3+ddb://warehouse/vectors.lance").is_err());
        assert!(AtomicCommitStore::for_uri("gs://warehouse/vectors.lance").is_err());
    }

    #[test]
    fn unsupported_remote_writer_is_rejected_before_staging_any_fragments() {
        let uri = CString::new("s3+ddb://warehouse/vectors.lance").unwrap();
        let schema = FFI_ArrowSchema::try_from(&Schema::empty()).unwrap();
        let writer = lance_ffi_writer_open(
            ptr::null(),
            uri.as_ptr(),
            &schema,
            LanceWriteOperation::Overwrite as i32,
            1,
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            0,
        );
        assert!(writer.is_null());
        let error = unsafe { CStr::from_ptr(lance_ffi_last_error()) }
            .to_str()
            .unwrap();
        assert!(error.contains("atomic manifest commit is not configured"));
    }

    fn sdk_prepared_transaction(name: &str, fingerprint: &str) -> Transaction {
        let schema = lance_core::datatypes::Schema::try_from(&Schema::new(vec![
            Field::new(name, DataType::Int64, false),
        ])).unwrap();
        let mut transaction = Transaction::new(0, Operation::Overwrite {
            fragments: Vec::new(), schema, config_upsert_values: None,
        }, None, None);
        transaction.uuid = name.to_owned();
        transaction.tag = Some(fingerprint.to_owned());
        transaction
    }

    async fn sdk_prepare(store: ObjectStore, uri: &str, params: &ObjectStoreParams, name: &str, fingerprint: &str) -> lance::Result<Dataset> {
        CommitBuilder::new(uri).with_store_params(params.clone()).with_object_store(Arc::new(store))
            .with_commit_handler(Arc::new(managed::CatalogCommitHandler {
                accepted: None, candidate: Some((format!("_pangrova/prepared/{fingerprint}.manifest"), 1, fingerprint.into())),
            })).with_max_retries(1).enable_v2_manifest_paths(true)
            .execute(sdk_prepared_transaction(name, fingerprint)).await
    }

    async fn assert_sdk_candidate_race(first_store: ObjectStore, second_store: ObjectStore,
            base: Path, uri: &str, params: ObjectStoreParams, supports_conditional_create: bool) {
        let fingerprint = hex_digest(&sha256(b"actual-sdk-candidate-race"));
        let (first, second) = futures::join!(
            sdk_prepare(first_store.clone(), uri, &params, "writer-one", &fingerprint),
            sdk_prepare(second_store, uri, &params, "writer-two", &fingerprint),
        );
        let path = object_path(&base, &format!("_pangrova/prepared/{fingerprint}.manifest")).unwrap();
        if supports_conditional_create {
            assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
            assert_eq!(usize::from(matches!(first, Err(lance::Error::DatasetAlreadyExists { .. })))
                + usize::from(matches!(second, Err(lance::Error::DatasetAlreadyExists { .. }))), 1);
            let expected = if first.is_ok() { "writer-one" } else { "writer-two" };
            let winner = first.or(second).unwrap();
            assert_eq!(winner.version().version, 1);
            assert_eq!(winner.manifest().tag.as_deref(), Some(fingerprint.as_str()));
            assert_eq!(winner.read_transaction().await.unwrap().unwrap().uuid, expected);
            let arrow_schema: Schema = winner.schema().into();
            assert_eq!(arrow_schema.field(0).name(), expected);
            let payload = first_store.inner.get(&path).await.unwrap().bytes().await.unwrap();
            assert!(!payload.is_empty());
            let late = sdk_prepare(first_store.clone(), uri, &params, "late-writer", &fingerprint).await;
            assert!(matches!(late, Err(lance::Error::DatasetAlreadyExists { .. })));
            let unchanged = first_store.inner.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(unchanged, payload);
            assert!(matches!(first_store.inner.head(&object_path(&base, "_versions/1.manifest").unwrap()).await,
                Err(ObjectStoreError::NotFound { .. })));
        } else {
            for result in [first, second] {
                let error = result.expect_err("unsafe MinIO conditional PUT must fail closed");
                assert!(format!("{error:?}").contains("ignored conditional create"));
            }
            assert!(matches!(first_store.inner.head(&path).await, Err(ObjectStoreError::NotFound { .. })));
        }
    }

    #[test]
    fn conditional_prepared_manifest_has_one_winner_for_same_candidate() {
        get_or_create_runtime().block_on(async {
            let store = ObjectStore::memory();
            assert_sdk_candidate_race(store.clone(), store, Path::from("test-dataset"),
                "memory:///test-dataset", ObjectStoreParams::default(), true).await;
        });
    }

    #[test]
    fn sdk_prepared_manifest_and_sidecar_preserve_physical_directory_hierarchy() {
        let root = std::env::temp_dir().join(format!("pangrova-lance-path-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let uri = root.to_str().unwrap();
        let fingerprint = hex_digest(&sha256(b"actual-sdk-nested-path"));
        get_or_create_runtime().block_on(async {
            let params = ObjectStoreParams::default();
            let (store, base) = ObjectStore::from_uri_and_params(Arc::new(ObjectStoreRegistry::default()),
                uri, &params).await.unwrap();
            let dataset = sdk_prepare(store.clone(), uri, &params, "nested-writer", &fingerprint).await.unwrap();
            let relative = format!("_pangrova/prepared/{fingerprint}.manifest");
            let manifest = object_path(&base, &relative).unwrap();
            assert_eq!(manifest.as_ref(), format!("{base}/{relative}"));
            assert!(root.join(&relative).is_file());
            assert!(store.inner.head(&manifest).await.unwrap().size > 0);
            assert!(matches!(store.inner.head(&base.child(relative.as_str())).await,
                Err(ObjectStoreError::NotFound { .. })));
            let transaction_file = dataset.manifest().transaction_file.as_ref().unwrap();
            assert!(root.join("_transactions").join(transaction_file).is_file());
            assert_eq!(dataset.read_transaction().await.unwrap().unwrap().uuid, "nested-writer");
            let storage = StorageContext::test_local(uri);
            let sidecar = "_pangrova/write-staging/nested-writer/attempt-1/transaction-test.pb";
            write_transaction_sidecar(&storage, uri, sidecar, b"exact-sidecar").await.unwrap();
            assert!(root.join(sidecar).is_file());
            let path = object_path(&base, sidecar).unwrap();
            assert_eq!(path.as_ref(), format!("{base}/{sidecar}"));
            assert_eq!(store.inner.head(&path).await.unwrap().size, 13);
            assert_eq!(read_transaction_sidecar(&storage, uri, sidecar, 13).await.unwrap(), b"exact-sidecar");
            assert!(matches!(store.inner.head(&base.child(sidecar)).await,
                Err(ObjectStoreError::NotFound { .. })));
            assert!(object_path(&base, "../transaction-test.pb").is_err());
            assert!(object_path(&base, "_pangrova%2Fwrite-staging/transaction-test.pb").is_err());
            assert!(object_path(&base, "_pangrova//write-staging/transaction-test.pb").is_err());
        });
        std::fs::remove_dir_all(root).unwrap();
    }

    fn assert_minio_conditional_commit(image: &str, supports_conditional_create: bool) {
        let minio = MinioContainer::start(image);
        let bucket = format!("pangrova-lance-{}", uuid::Uuid::new_v4());
        minio.create_bucket(&bucket);
        let uri = format!("s3://{bucket}/test-dataset");
        let mut params = AtomicCommitStore::for_uri(&uri).unwrap().store_params();
        params.storage_options.as_mut().unwrap().extend([
            ("aws_endpoint".to_owned(), minio.endpoint.clone()),
            ("aws_region".to_owned(), "us-east-1".to_owned()),
            ("aws_access_key_id".to_owned(), "minioadmin".to_owned()),
            ("aws_secret_access_key".to_owned(), "minioadmin".to_owned()),
            ("aws_allow_http".to_owned(), "true".to_owned()),
        ]);
        get_or_create_runtime().block_on(async {
            let (store, base) = ObjectStore::from_uri_and_params(Arc::new(ObjectStoreRegistry::default()),
                &uri, &params).await.unwrap();
            let (second_store, _) = ObjectStore::from_uri_and_params(Arc::new(ObjectStoreRegistry::default()),
                &uri, &params).await.unwrap();
            assert_sdk_candidate_race(store, second_store, base, &uri, params, supports_conditional_create).await;
        });
    }

    #[test]
    fn minio_without_conditional_create_fails_closed() {
        assert_minio_conditional_commit("minio/minio:RELEASE.2024-01-16T16-07-38Z", false);
    }

    #[test]
    fn minio_conditional_manifest_commit_has_one_winner() {
        assert_minio_conditional_commit("tobi312/minio:RELEASE.2025-02-28T09-55-16Z", true);
    }

    #[test]
    fn runtime_shutdown_releases_global_reference_and_allows_reinit() {
        let first = get_or_create_runtime();
        lance_ffi_runtime_shutdown();
        let second = get_or_create_runtime();
        assert!(!Arc::ptr_eq(&first, &second));
        lance_ffi_runtime_shutdown();
    }

    #[test]
    fn resolver_without_accepted_membership_never_discovers_filesystem_history() {
        get_or_create_runtime().block_on(async {
            let store = ObjectStore::memory();
            let base = Path::from("test-history");
            for version in [2, 42, 10_000_000] {
                store.put(&object_path(&base, &format!("_versions/{version}.manifest")).unwrap(), b"unaccepted-object").await.unwrap();
            }
            let resolver = managed::CatalogCommitHandler { accepted: None, candidate: None };
            assert!(resolver.resolve_latest_version(&base, &store).await.is_err());
            assert!(resolver.resolve_latest_location(&base, &store).await.is_err());
            assert!(resolver.resolve_latest_version_id(&base, &store).await.is_err());
            assert!(resolver.list_manifests(&base, store.inner.as_ref()).await.unwrap().collect::<Vec<_>>().await.is_empty());
        });
    }

    #[test]
    fn resolver_cannot_treat_recorded_version_as_an_accepted_membership() {
        get_or_create_runtime().block_on(async {
            let store = ObjectStore::memory(); let base = Path::from("test-history-hint");
            store.put(&object_path(&base, "_versions/42.manifest").unwrap(), b"unaccepted-object").await.unwrap();
            let resolver = managed::CatalogCommitHandler { accepted: None, candidate: None };
            for version in [1, 41, 42, 100, 10_000_000] {
                assert!(resolver.resolve_version(&base, version, store.inner.as_ref()).await.is_err());
                assert!(resolver.resolve_version_location(&base, version, store.inner.as_ref()).await.is_err());
            }
        });
    }

    #[test]
    fn resolver_has_no_implicit_latest_or_prior_base_fallback() {
        get_or_create_runtime().block_on(async {
            let store = ObjectStore::memory(); let base = Path::from("test-history-base");
            for version in [7, 999_995, 999_996, 999_997, 999_998, 999_999, 1_000_000] {
                store.put(&object_path(&base, &format!("_versions/{version}.manifest")).unwrap(), b"unaccepted-object").await.unwrap();
                let resolver = managed::CatalogCommitHandler { accepted: None, candidate: None };
                assert!(resolver.resolve_version(&base, version, store.inner.as_ref()).await.is_err());
            }
        });
    }

    #[test]
    fn transaction_sidecar_rejects_declared_size_mismatch_before_download() {
        let root = std::env::temp_dir().join(format!(
            "pangrova-lance-sidecar-bounds-{}",
            std::process::id()
        ));
        let relative_key = "attempt-1/transaction-test.pb";
        let object_path = root.join(relative_key);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"oversized-sidecar").unwrap();

        let runtime = get_or_create_runtime();
        let error = runtime
            .block_on(read_transaction_sidecar(
                &StorageContext::test_local(root.to_str().unwrap()),
                root.to_str().unwrap(),
                relative_key,
                4,
            ))
            .unwrap_err();

        assert!(error.contains("size mismatch before download"), "{error}");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn transaction_sidecar_reads_exact_declared_range() {
        let root = std::env::temp_dir().join(format!(
            "pangrova-lance-sidecar-exact-{}",
            std::process::id()
        ));
        let relative_key = "attempt-1/transaction-test.pb";
        let object_path = root.join(relative_key);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"exact-sidecar").unwrap();

        let runtime = get_or_create_runtime();
        let bytes = runtime
            .block_on(read_transaction_sidecar(
                &StorageContext::test_local(root.to_str().unwrap()),
                root.to_str().unwrap(),
                relative_key,
                13,
            ))
            .unwrap();

        assert_eq!(bytes, b"exact-sidecar");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn schema_fingerprint_ignores_non_semantic_metadata() {
        let plain = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let mut field_metadata = HashMap::new();
        field_metadata.insert("transport_hint".to_owned(), "dictionary".to_owned());
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("producer".to_owned(), "test".to_owned());
        let decorated = Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false).with_metadata(field_metadata)],
            schema_metadata,
        );
        assert_eq!(
            canonical_schema_fingerprint(&plain),
            canonical_schema_fingerprint(&decorated)
        );
    }

    #[test]
    fn schema_fingerprint_fences_structural_drift() {
        let expected = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let renamed = Schema::new(vec![Field::new("other", DataType::Int64, false)]);
        let retyped = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        assert_ne!(
            canonical_schema_fingerprint(&expected),
            canonical_schema_fingerprint(&renamed)
        );
        assert_ne!(
            canonical_schema_fingerprint(&expected),
            canonical_schema_fingerprint(&retyped)
        );
    }
}
