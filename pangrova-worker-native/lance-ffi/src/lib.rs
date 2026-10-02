//! Lance C FFI — C-compatible API for Lance file format read/write/scan.
//!
//! Exposes Lance operations via Arrow C Data Interface so that C++ consumers
//! (libpangrova_native / Velox) can read and write Lance files without Rust
//! runtime knowledge. All Arrow data crosses the boundary as ArrowArray +
//! ArrowSchema structs (zero-copy when possible).

use std::collections::{HashMap, HashSet};
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
use async_trait::async_trait;
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, Dataset, InsertBuilder, WriteMode, WriteParams};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::stream::BatchStream;
use lance_table::format::pb as lance_pb;
use lance_table::format::{Index, Manifest};
use lance_table::io::commit::{
    CommitError, CommitHandler, ManifestNamingScheme, ManifestWriter, RenameCommitHandler,
};
use object_store::{path::Path, Error as ObjectStoreError, PutMode, PutOptions, PutPayload};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::runtime::Runtime;
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
use canonical::pangrova::protocol::v2::ExecutionOwnerIdentity as CanonicalExecutionOwnerIdentity;
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

    fn commit_handler(self) -> Arc<dyn CommitHandler> {
        match self {
            Self::Local => Arc::new(RenameCommitHandler),
            Self::S3ConditionalPut => Arc::new(ConditionalPutCommitHandler),
        }
    }
}

async fn open_atomic_dataset(uri: &str, storage: &StorageContext) -> Result<Dataset, lance::Error> {
    let store = AtomicCommitStore::for_uri(uri).map_err(|error| std::io::Error::other(error))?;
    let mut builder = DatasetBuilder::from_uri(uri).with_commit_handler(store.commit_handler());
    if let Some(options) = storage.params(store.store_params()).storage_options {
        builder = builder.with_storage_options(options);
    }
    builder.load().await
}

#[derive(Debug)]
struct ConditionalPutCommitHandler;

#[async_trait]
impl CommitHandler for ConditionalPutCommitHandler {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<Index>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
    ) -> Result<Path, CommitError> {
        verify_conditional_create(object_store, base_path).await?;
        let path = naming_scheme.manifest_path(base_path, manifest.version);
        let staged_path = Path::from(format!("{}-pangrova-staged-{}", path, uuid::Uuid::new_v4()));
        let commit = async {
            manifest_writer(object_store, manifest, indices, &staged_path).await?;
            let metadata = object_store
                .inner
                .head(&staged_path)
                .await
                .map_err(|error| CommitError::OtherError(error.into()))?;
            if metadata.size == 0 || metadata.size > MAX_MANIFEST_BYTES {
                return Err(CommitError::OtherError(
                    std::io::Error::other(format!(
                        "Lance manifest size {} violates the {}-byte conditional-commit limit",
                        metadata.size, MAX_MANIFEST_BYTES
                    ))
                    .into(),
                ));
            }
            let payload = object_store
                .inner
                .get(&staged_path)
                .await
                .map_err(|error| CommitError::OtherError(error.into()))?
                .bytes()
                .await
                .map_err(|error| CommitError::OtherError(error.into()))?;
            match object_store
                .inner
                .put_opts(
                    &path,
                    PutPayload::from(payload),
                    PutOptions::from(PutMode::Create),
                )
                .await
            {
                Ok(_) => Ok(path),
                Err(ObjectStoreError::AlreadyExists { .. }) => Err(CommitError::CommitConflict),
                Err(error) => Err(CommitError::OtherError(error.into())),
            }
        }
        .await;
        let _ = object_store.inner.delete(&staged_path).await;
        commit
    }
}

async fn verify_conditional_create(
    object_store: &ObjectStore,
    base_path: &Path,
) -> Result<(), CommitError> {
    let probe_path = Path::from(format!(
        "{base_path}/_versions/.pangrova-conditional-put-probe-{}",
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

#[derive(Debug)]
struct ExactVersionCommitHandler {
    expected_next_version: u64,
    inner: Arc<dyn CommitHandler>,
}

#[async_trait]
impl CommitHandler for ExactVersionCommitHandler {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<Index>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
    ) -> Result<Path, CommitError> {
        if manifest.version != self.expected_next_version {
            return Err(CommitError::CommitConflict);
        }
        self.inner
            .commit(
                manifest,
                indices,
                base_path,
                object_store,
                manifest_writer,
                naming_scheme,
            )
            .await
    }
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

/// Borrowed buffer used by the connector-owned aggregate commit call.
#[repr(C)]
pub struct LanceCCommitFragment {
    pub data: *const u8,
    pub len: usize,
    pub expected_staging_prefix: *const c_char,
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

async fn delete_transaction_sidecar(
    storage: &StorageContext,
    dataset_uri: &str,
    relative_key: &str,
) -> Result<(), String> {
    let (store, base) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        dataset_uri,
        &storage.params(AtomicCommitStore::for_uri(dataset_uri)?.store_params()),
    )
    .await
    .map_err(|error| format!("failed to open Lance transaction object store: {error}"))?;
    let path = object_path(&base, relative_key)?;
    match store.delete(&path).await {
        Ok(()) | Err(lance::Error::NotFound { .. }) => Ok(()),
        Err(lance::Error::IO { source, .. })
            if matches!(
                source.downcast_ref::<object_store::Error>(),
                Some(object_store::Error::NotFound { .. })
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(format!(
            "failed to delete Lance transaction sidecar: {error}"
        )),
    }
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
                commit_handler: Some(store.commit_handler()),
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
                        match open_atomic_dataset(&uri, &storage).await {
                            Ok(dataset) => {
                                return Err(format!(
                                    "CREATE target already exists at version {}: {uri}",
                                    dataset.version().version
                                ));
                            }
                            Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. }) => {}
                            Err(error) => {
                                return Err(format!(
                                    "failed to fence CREATE target {uri}: {error}"
                                ));
                            }
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
                        let dataset = open_atomic_dataset(&uri, &storage)
                            .await
                            .map_err(|error| format!("failed to open fenced target {uri}: {error}"))?;
                        let latest_version = dataset.version().version;
                        if latest_version < expected_read_version {
                            return Err(format!(
                                "target version {latest_version} is older than expected version {expected_read_version}"
                            ));
                        }
                        let fenced_dataset = dataset
                            .checkout_version(expected_read_version)
                            .await
                            .map_err(|error| {
                                format!(
                                    "failed to checkout expected version {expected_read_version}: {error}"
                                )
                            })?;
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

fn recovery_scan_versions(
    expected_read_version: u64,
    latest_version: u64,
    known_committed_version: u64,
) -> Result<std::iter::Rev<std::ops::RangeInclusive<u64>>, String> {
    if known_committed_version > 0 {
        if known_committed_version > latest_version
            || known_committed_version <= expected_read_version
        {
            return Err(format!(
                "commit journal version {known_committed_version} is newer than dataset version {latest_version}"
            ));
        }
        return Ok((known_committed_version..=known_committed_version).rev());
    }
    if latest_version <= expected_read_version {
        return Ok((1..=0).rev());
    }
    Ok((expected_read_version + 1..=latest_version).rev())
}

async fn find_committed_write(
    dataset: &Dataset,
    expected_read_version: u64,
    known_committed_version: u64,
    tag_prefix: &str,
    exact_tag: &str,
) -> Result<Option<u64>, String> {
    let latest = dataset.version().version;
    let versions = recovery_scan_versions(expected_read_version, latest, known_committed_version)?;
    for version in versions {
        let versioned = dataset
            .checkout_version(version)
            .await
            .map_err(|error| format!("failed to inspect Lance version {version}: {error}"))?;
        let Some(transaction) = versioned.read_transaction().await.map_err(|error| {
            format!("failed to read Lance transaction at version {version}: {error}")
        })?
        else {
            continue;
        };
        match transaction.tag.as_deref() {
            Some(tag) if tag == exact_tag => return Ok(Some(version)),
            Some(tag) if tag.starts_with(tag_prefix) => {
                return Err(format!(
                    "write_id was already committed with different fragment metadata at version {version}"
                ));
            }
            _ => {}
        }
    }
    if known_committed_version > 0 {
        return Err(format!(
            "commit journal version {known_committed_version} does not contain the expected write identity"
        ));
    }
    Ok(None)
}

async fn committed_manifest_digest(
    storage: &StorageContext,
    uri: &str,
    version: u64,
) -> Result<[u8; SHA256_DIGEST_SIZE], String> {
    let atomic_store = AtomicCommitStore::for_uri(uri)?;
    let (store, base) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        uri,
        &storage.params(atomic_store.store_params()),
    )
    .await
    .map_err(|error| format!("failed to open committed manifest store: {error}"))?;
    let location = atomic_store
        .commit_handler()
        .resolve_version_location(&base, version, store.inner.as_ref())
        .await
        .map_err(|error| {
            format!("failed to resolve committed manifest version {version}: {error}")
        })?;
    let metadata = store
        .inner
        .head(&location.path)
        .await
        .map_err(|error| format!("failed to inspect committed manifest: {error}"))?;
    if metadata.size == 0 || metadata.size > MAX_MANIFEST_BYTES {
        return Err("committed Lance manifest violates metadata size limit".to_owned());
    }
    let bytes = store
        .inner
        .get(&location.path)
        .await
        .map_err(|error| format!("failed to read committed manifest: {error}"))?
        .bytes()
        .await
        .map_err(|error| format!("failed to read committed manifest bytes: {error}"))?;
    if bytes.len() != metadata.size || bytes.len() > MAX_MANIFEST_BYTES {
        return Err("committed Lance manifest changed while reading".to_owned());
    }
    sha256(&bytes)
        .try_into()
        .map_err(|_| "invalid manifest SHA-256 length".to_owned())
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

struct RecoveryFence<'a> {
    dataset_uri: &'a str,
    write_id: &'a str,
    tenant_id: &'a str,
    generation: &'a str,
    operation: LanceWriteOperation,
    expected_read_version: u64,
    schema_fence_sha256: &'a [u8; SHA256_DIGEST_SIZE],
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

fn append_digest_field(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_be_bytes());
    output.extend_from_slice(value);
}

fn fragment_recovery_identity(
    fragments: &mut [ValidatedFragmentReference],
    fence: &RecoveryFence<'_>,
) -> (String, String) {
    fragments.sort_by_key(|entry| {
        entry
            .fragment
            .task_attempt
            .as_ref()
            .expect("validated fragment has task_attempt")
            .encode_to_vec()
    });
    let mut digest_input = b"pangrova.lance.write-commit.v2\0".to_vec();
    append_digest_field(&mut digest_input, fence.dataset_uri.as_bytes());
    append_digest_field(&mut digest_input, fence.write_id.as_bytes());
    append_digest_field(&mut digest_input, fence.tenant_id.as_bytes());
    append_digest_field(&mut digest_input, fence.generation.as_bytes());
    digest_input.extend_from_slice(&(fence.operation as i32).to_be_bytes());
    digest_input.extend_from_slice(&fence.expected_read_version.to_be_bytes());
    digest_input.extend_from_slice(fence.schema_fence_sha256);
    digest_input.extend_from_slice(&(fragments.len() as u64).to_be_bytes());
    for entry in fragments {
        append_digest_field(
            &mut digest_input,
            &entry
                .fragment
                .task_attempt
                .as_ref()
                .expect("validated fragment has task_attempt")
                .encode_to_vec(),
        );
        digest_input.extend_from_slice(&entry.transaction_digest);
    }
    let aggregate_digest = sha256(&digest_input);
    let tag_prefix = format!(
        "pangrova-write:{}:",
        hex_digest(&sha256(fence.write_id.as_bytes()))
    );
    let exact_tag = format!("{}{}", tag_prefix, hex_digest(&aggregate_digest));
    (tag_prefix, exact_tag)
}

async fn commit_fragments_impl(
    storage: &StorageContext,
    dataset_uri: String,
    write_id: String,
    tenant_id: String,
    generation: String,
    operation: LanceWriteOperation,
    expected_read_version: u64,
    schema_fence_sha256: [u8; SHA256_DIGEST_SIZE],
    fragment_inputs: Vec<(Vec<u8>, String)>,
    max_retries: u32,
    recovering: bool,
    known_committed_version: u64,
) -> Result<u64, String> {
    if fragment_inputs.is_empty() {
        return Err("at least one Lance write fragment is required".to_owned());
    }
    let store = AtomicCommitStore::for_uri(&dataset_uri)?;

    let mut references = Vec::with_capacity(fragment_inputs.len());
    let mut attempts = HashSet::with_capacity(fragment_inputs.len());
    let mut aggregate_transaction_bytes = 0_usize;
    for (bytes, expected_staging_prefix) in fragment_inputs {
        let reference = validate_fragment_reference(
            &bytes,
            &expected_staging_prefix,
            &dataset_uri,
            &write_id,
            &tenant_id,
            &generation,
            operation,
            expected_read_version,
            &schema_fence_sha256,
        )?;
        let attempt = reference
            .fragment
            .task_attempt
            .as_ref()
            .expect("validated fragment has task_attempt")
            .encode_to_vec();
        if !attempts.insert(attempt) {
            return Err("Lance write fragments contain a duplicate task attempt".to_owned());
        }
        aggregate_transaction_bytes = aggregate_transaction_bytes
            .checked_add(reference.transaction_size)
            .filter(|size| *size <= MAX_AGGREGATE_TRANSACTION_BYTES)
            .ok_or_else(|| {
                "aggregate Lance transaction metadata exceeds the 256 MiB hard limit".to_owned()
            })?;
        references.push(reference);
    }

    let recovery_fence = RecoveryFence {
        dataset_uri: &dataset_uri,
        write_id: &write_id,
        tenant_id: &tenant_id,
        generation: &generation,
        operation,
        expected_read_version,
        schema_fence_sha256: &schema_fence_sha256,
    };
    let (tag_prefix, exact_tag) = fragment_recovery_identity(&mut references, &recovery_fence);
    let existing = match open_atomic_dataset(&dataset_uri, storage).await {
        Ok(dataset) => Some(dataset),
        Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. }) => None,
        Err(error) => return Err(format!("failed to open Lance commit target: {error}")),
    };
    if recovering {
        if let Some(dataset) = existing.as_ref() {
            if let Some(version) = find_committed_write(
                dataset,
                expected_read_version,
                known_committed_version,
                &tag_prefix,
                &exact_tag,
            )
            .await?
            {
                return Ok(version);
            }
        }
    }

    let mut combined_fragments = Vec::new();
    let mut overwrite_schema = None;
    for reference in references {
        let lance_transaction = read_transaction_sidecar(
            storage,
            &dataset_uri,
            &reference.relative_object_key,
            reference.transaction_size,
        )
        .await?;
        if sha256(&lance_transaction) != reference.transaction_digest {
            return Err("Lance transaction sidecar checksum mismatch".to_owned());
        }
        let transaction = decode_transaction(&lance_transaction)?;
        if transaction.read_version != expected_read_version || transaction.blobs_op.is_some() {
            return Err(
                "Lance transaction version/blob operation violates the commit fence".to_owned(),
            );
        }
        if transaction_row_count(&transaction)? != reference.fragment.rows_written {
            return Err(
                "Lance write fragment row count does not match transaction metadata".to_owned(),
            );
        }
        match (&transaction.operation, operation) {
            (Operation::Append { .. }, LanceWriteOperation::Append)
            | (Operation::Overwrite { .. }, LanceWriteOperation::Create)
            | (Operation::Overwrite { .. }, LanceWriteOperation::Overwrite) => {}
            _ => return Err("Lance transaction operation does not match commit mode".to_owned()),
        }
        match transaction.operation {
            Operation::Append { fragments } => combined_fragments.extend(fragments),
            Operation::Overwrite {
                fragments,
                schema,
                config_upsert_values,
            } => {
                if config_upsert_values
                    .as_ref()
                    .is_some_and(|values| !values.is_empty())
                {
                    return Err(
                        "Lance SQL write fragments may not mutate dataset config".to_owned()
                    );
                }
                if let Some(existing) = overwrite_schema.as_ref() {
                    if existing != &schema {
                        return Err("Lance overwrite fragments disagree on schema".to_owned());
                    }
                } else {
                    overwrite_schema = Some(schema);
                }
                combined_fragments.extend(fragments);
            }
            _ => unreachable!("validated above"),
        }
    }
    let aggregate_digest = exact_tag
        .strip_prefix(&tag_prefix)
        .expect("exact recovery tag has prefix");
    let aggregate_operation = match operation {
        LanceWriteOperation::Append => Operation::Append {
            fragments: combined_fragments,
        },
        LanceWriteOperation::Create | LanceWriteOperation::Overwrite => Operation::Overwrite {
            fragments: combined_fragments,
            schema: overwrite_schema.ok_or_else(|| {
                "Lance create/overwrite fragments did not contain a schema".to_owned()
            })?,
            config_upsert_values: None,
        },
        LanceWriteOperation::Unspecified => unreachable!("validated before use"),
    };
    let mut aggregate = Transaction::new(expected_read_version, aggregate_operation, None, None);
    aggregate.uuid = aggregate_digest.to_owned();
    aggregate.tag = Some(exact_tag.clone());

    let committed = match (operation, existing) {
        (LanceWriteOperation::Create, None) => CommitBuilder::new(dataset_uri.as_str())
            .with_commit_handler(store.commit_handler())
            .with_store_params(storage.params(store.store_params()))
            .with_max_retries(max_retries)
            .execute(aggregate)
            .await,
        (LanceWriteOperation::Create, Some(dataset)) => {
            return Err(format!(
                "CREATE target already exists at version {}",
                dataset.version().version
            ));
        }
        (LanceWriteOperation::Append | LanceWriteOperation::Overwrite, Some(dataset)) => {
            if dataset.version().version < expected_read_version {
                return Err(format!(
                    "commit target version {} is older than expected version {expected_read_version}",
                    dataset.version().version
                ));
            }
            dataset
                .checkout_version(expected_read_version)
                .await
                .map_err(|error| {
                    format!("expected Lance version {expected_read_version} is unavailable: {error}")
                })?;
            let next_version = expected_read_version
                .checked_add(1)
                .ok_or_else(|| "Lance version overflow".to_owned())?;
            CommitBuilder::new(dataset_uri.as_str())
                .with_commit_handler(Arc::new(ExactVersionCommitHandler {
                    expected_next_version: next_version,
                    inner: store.commit_handler(),
                }))
                .with_store_params(storage.params(store.store_params()))
                .with_max_retries(1)
                .execute(aggregate)
                .await
        }
        (LanceWriteOperation::Append | LanceWriteOperation::Overwrite, None) => {
            return Err("APPEND/OVERWRITE target does not exist".to_owned());
        }
        (LanceWriteOperation::Unspecified, _) => unreachable!("validated before use"),
    }
    .map_err(|error| format!("atomic Lance commit failed: {error}"))?;
    Ok(committed.version().version)
}

async fn discard_fragments_impl(
    storage: &StorageContext,
    dataset_uri: String,
    write_id: String,
    tenant_id: String,
    generation: String,
    operation: LanceWriteOperation,
    expected_read_version: u64,
    schema_fence_sha256: [u8; SHA256_DIGEST_SIZE],
    fragment_inputs: Vec<(Vec<u8>, String)>,
) -> Result<(), String> {
    if fragment_inputs.is_empty() {
        return Err("at least one Lance write fragment is required".to_owned());
    }
    let mut references = Vec::with_capacity(fragment_inputs.len());
    let mut attempts = HashSet::with_capacity(fragment_inputs.len());
    for (bytes, expected_staging_prefix) in fragment_inputs {
        let reference = validate_fragment_reference(
            &bytes,
            &expected_staging_prefix,
            &dataset_uri,
            &write_id,
            &tenant_id,
            &generation,
            operation,
            expected_read_version,
            &schema_fence_sha256,
        )?;
        let attempt = reference
            .fragment
            .task_attempt
            .as_ref()
            .expect("validated fragment has task_attempt")
            .encode_to_vec();
        if !attempts.insert(attempt) {
            return Err("Lance write fragments contain a duplicate task attempt".to_owned());
        }
        references.push(reference);
    }
    for reference in references {
        delete_transaction_sidecar(storage, &dataset_uri, &reference.relative_object_key).await?;
    }
    Ok(())
}

unsafe fn copy_commit_fragment_views(
    fragments: *const LanceCCommitFragment,
    fragment_count: usize,
) -> Result<Vec<(Vec<u8>, String)>, String> {
    if fragments.is_null() || fragment_count == 0 {
        return Err("fragments and a positive fragment_count are required".to_owned());
    }
    if fragment_count > 1_000_000 {
        return Err("fragment_count exceeds the hard safety limit".to_owned());
    }
    let views = unsafe { slice::from_raw_parts(fragments, fragment_count) };
    let mut owned = Vec::with_capacity(fragment_count);
    let mut total_bytes = 0_usize;
    for (index, view) in views.iter().enumerate() {
        if view.data.is_null() || view.len == 0 || view.len > MAX_INLINE_FRAGMENT_BYTES {
            return Err(format!(
                "fragment {index} is empty or exceeds the 64 KiB hard limit"
            ));
        }
        let expected_staging_prefix = parse_required_string(
            view.expected_staging_prefix,
            "expected_staging_prefix",
            2048,
        )
        .map_err(|error| format!("fragment {index}: {error}"))?;
        total_bytes = total_bytes
            .checked_add(view.len)
            .filter(|value| *value <= MAX_AGGREGATE_TRANSACTION_BYTES)
            .ok_or_else(|| {
                "aggregate fragment metadata exceeds the 256 MiB hard limit".to_owned()
            })?;
        owned.push((
            unsafe { slice::from_raw_parts(view.data, view.len) }.to_vec(),
            expected_staging_prefix,
        ));
    }
    Ok(owned)
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
    12
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
                let dataset = match runtime.block_on(async {
                    open_atomic_dataset(&uri_str, &storage)
                        .await?
                        .checkout_version(version)
                        .await
                }) {
                    Ok(ds) => ds,
                    Err(e) => {
                        set_last_error(&format!("failed to open dataset: {e}"));
                        return ptr::null_mut();
                    }
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

/// Resolve the latest visible dataset version for connector write allocation.
#[no_mangle]
pub extern "C" fn lance_ffi_dataset_version(
    storage: *const LanceCStorage,
    uri: *const c_char,
    out_version: *mut u64,
) -> i32 {
    ffi_guard!("dataset_version", -1, {
        if out_version.is_null() {
            set_last_error("out_version must not be NULL");
            return -1;
        }
        let uri = match parse_required_string(uri, "uri", 8192) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let runtime = get_or_create_runtime();
        let storage = require_storage!(storage, &uri, None, -1);
        match runtime.block_on(open_atomic_dataset(&uri, &storage)) {
            Ok(dataset) => {
                unsafe {
                    *out_version = dataset.version().version;
                }
                0
            }
            Err(error) => {
                set_last_error(&format!("failed to resolve Lance dataset version: {error}"));
                -1
            }
        }
    })
}

/// Resolve version and schema fence from one opened snapshot so connector
/// allocation cannot observe a version/schema pair split by a concurrent commit.
#[no_mangle]
pub extern "C" fn lance_ffi_dataset_info(
    storage: *const LanceCStorage,
    uri: *const c_char,
    version: u64,
    out_version: *mut u64,
    out_schema_sha256: *mut u8,
    schema_sha256_len: usize,
) -> i32 {
    ffi_guard!("dataset_info", -1, {
        if out_version.is_null()
            || out_schema_sha256.is_null()
            || schema_sha256_len != SHA256_DIGEST_SIZE
        {
            set_last_error("out_version and 32-byte out_schema_sha256 are required");
            return -1;
        }
        let uri = match parse_required_string(uri, "uri", 8192) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let runtime = get_or_create_runtime();
        let storage = require_storage!(storage, &uri, None, -1);
        match runtime.block_on(async {
            let dataset = open_atomic_dataset(&uri, &storage).await?;
            dataset.checkout_version(version).await
        }) {
            Ok(dataset) => {
                let schema: Schema = dataset.schema().into();
                let fingerprint = canonical_schema_fingerprint(&schema);
                unsafe {
                    *out_version = dataset.version().version;
                    ptr::copy_nonoverlapping(
                        fingerprint.as_ptr(),
                        out_schema_sha256,
                        fingerprint.len(),
                    );
                }
                0
            }
            Err(error) => {
                set_last_error(&format!("failed to resolve Lance dataset info: {error}"));
                -1
            }
        }
    })
}

/// Resolve the canonical schema fence of the latest visible dataset version.
#[no_mangle]
pub extern "C" fn lance_ffi_dataset_schema_fingerprint(
    storage: *const LanceCStorage,
    uri: *const c_char,
    out_sha256: *mut u8,
    out_len: usize,
) -> i32 {
    ffi_guard!("dataset_schema_fingerprint", -1, {
        if out_sha256.is_null() || out_len != SHA256_DIGEST_SIZE {
            set_last_error("a 32-byte out_sha256 is required");
            return -1;
        }
        let uri = match parse_required_string(uri, "uri", 8192) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let runtime = get_or_create_runtime();
        let storage = require_storage!(storage, &uri, None, -1);
        match runtime.block_on(open_atomic_dataset(&uri, &storage)) {
            Ok(dataset) => {
                let schema: Schema = dataset.schema().into();
                let fingerprint = canonical_schema_fingerprint(&schema);
                unsafe {
                    ptr::copy_nonoverlapping(fingerprint.as_ptr(), out_sha256, fingerprint.len());
                }
                0
            }
            Err(error) => {
                set_last_error(&format!(
                    "failed to resolve Lance dataset schema fingerprint: {error}"
                ));
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
                    let mut scanner = dataset.scan();
                    scanner.with_fragments(vec![partition.clone()]);
                    let positions = scan_identity::requested(reader.projected_columns.as_deref());
                    if positions {
                        let schema: Schema = dataset.schema().into();
                        if schema.index_of(scan_identity::POSITION).is_ok()
                            || schema.index_of(lance_core::ROW_ADDR).is_ok()
                        {
                            set_last_error(
                                "Dataset schema collides with reserved physical identity metadata",
                            );
                            return -1;
                        }
                        scanner
                            .with_row_address()
                            .scan_in_order(true)
                            .use_scalar_index(false);
                    }
                    if let Some(ref cols) = reader.projected_columns {
                        let col_refs: Vec<&str> = cols
                            .iter()
                            .filter(|column| column.as_str() != scan_identity::POSITION)
                            .map(|s| s.as_str())
                            .collect();
                        if let Err(e) = scanner.project(&col_refs) {
                            set_last_error(&format!("projection error: {e}"));
                            return -1;
                        }
                    }

                    // arrow-rs 53 / lance 0.20: batch_size() is a builder method returning
                    // &mut Scanner (no Result).
                    scanner.batch_size(8192);

                    match reader.runtime.block_on(scanner.try_into_stream()) {
                        Ok(stream) => stream.boxed(),
                        Err(e) => {
                            set_last_error(&format!("scan error: {e}"));
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
                let returned_rows = batch.num_rows().min(requested_rows);
                let remainder = if returned_rows < batch.num_rows() {
                    Some(batch.slice(returned_rows, batch.num_rows() - returned_rows))
                } else {
                    None
                };
                let batch = batch.slice(0, returned_rows);
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

        let runtime = get_or_create_runtime();
        let writer = Box::new(LanceCWriter {
            runtime,
            storage: require_storage!(storage, &uri_str, Some(&tenant_id), ptr::null_mut()),
            uri: uri_str,
            schema,
            operation,
            expected_read_version,
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

/// Atomically publish a deterministic aggregate of uncommitted worker fragments.
///
/// This is the sole manifest-publishing primitive. It is intended to be called
/// by ConnectorMetadata.finishDistributedWrite, never by a worker task.
#[no_mangle]
pub extern "C" fn lance_ffi_commit_fragments(
    storage: *const LanceCStorage,
    uri: *const c_char,
    write_id: *const c_char,
    tenant_id: *const c_char,
    generation: *const c_char,
    operation: i32,
    expected_read_version: u64,
    schema_fence_sha256: *const u8,
    schema_fence_len: usize,
    fragments: *const LanceCCommitFragment,
    fragment_count: usize,
    max_retries: u32,
    recovering: i32,
    known_committed_version: u64,
    deadline_epoch_ms: u64,
    out_committed_version: *mut u64,
    out_manifest_sha256: *mut u8,
    manifest_sha256_len: usize,
) -> i32 {
    ffi_guard!("commit_fragments", -1, {
        if fragments.is_null()
            || fragment_count == 0
            || out_committed_version.is_null()
            || out_manifest_sha256.is_null()
            || manifest_sha256_len != SHA256_DIGEST_SIZE
        {
            set_last_error(
                "fragments, a positive count, version output and 32-byte manifest digest output are required",
            );
            return -1;
        }
        unsafe {
            *out_committed_version = 0;
            ptr::write_bytes(out_manifest_sha256, 0, SHA256_DIGEST_SIZE);
        }
        match remaining_commit_time(deadline_epoch_ms) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let uri = match parse_required_string(uri, "uri", 8192) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let write_id = match parse_required_string(write_id, "write_id", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let tenant_id = match parse_required_string(tenant_id, "tenant_id", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let generation = match parse_required_string(generation, "generation", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let operation = match LanceWriteOperation::parse(operation) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        if (operation == LanceWriteOperation::Create && expected_read_version != 0)
            || (operation != LanceWriteOperation::Create && expected_read_version == 0)
        {
            set_last_error(
                "CREATE requires version 0; APPEND/OVERWRITE require a non-zero version",
            );
            return -1;
        }
        let schema_fence_sha256 =
            match parse_sha256(schema_fence_sha256, schema_fence_len, "schema_fence_sha256") {
                Ok(value) => value,
                Err(error) => {
                    set_last_error(&error);
                    return -1;
                }
            };
        let owned = match unsafe { copy_commit_fragment_views(fragments, fragment_count) } {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let runtime = get_or_create_runtime();
        let remaining = match remaining_commit_time(deadline_epoch_ms) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        match runtime.block_on(async {
            tokio::time::timeout(remaining, async {
                let storage = scoped_storage(storage, &uri, Some(&tenant_id))?;
                let version = commit_fragments_impl(
                    &storage,
                    uri.clone(),
                    write_id,
                    tenant_id,
                    generation,
                    operation,
                    expected_read_version,
                    schema_fence_sha256,
                    owned,
                    max_retries.clamp(1, 64),
                    recovering != 0,
                    known_committed_version,
                )
                .await?;
                let digest = committed_manifest_digest(&storage, &uri, version).await?;
                Ok::<_, String>((version, digest))
            })
            .await
            .map_err(|_| {
                "Lance publication deadline exceeded; commit outcome is unknown".to_owned()
            })?
        }) {
            Ok((version, digest)) => {
                unsafe {
                    *out_committed_version = version;
                    ptr::copy_nonoverlapping(
                        digest.as_ptr(),
                        out_manifest_sha256,
                        SHA256_DIGEST_SIZE,
                    );
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

/// Idempotently delete fenced transaction sidecars after commit or abort.
#[no_mangle]
pub extern "C" fn lance_ffi_discard_fragments(
    storage: *const LanceCStorage,
    uri: *const c_char,
    write_id: *const c_char,
    tenant_id: *const c_char,
    generation: *const c_char,
    operation: i32,
    expected_read_version: u64,
    schema_fence_sha256: *const u8,
    schema_fence_len: usize,
    fragments: *const LanceCCommitFragment,
    fragment_count: usize,
) -> i32 {
    ffi_guard!("discard_fragments", -1, {
        let uri = match parse_required_string(uri, "uri", 8192) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let write_id = match parse_required_string(write_id, "write_id", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let tenant_id = match parse_required_string(tenant_id, "tenant_id", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let generation = match parse_required_string(generation, "generation", 512) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let operation = match LanceWriteOperation::parse(operation) {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        if (operation == LanceWriteOperation::Create && expected_read_version != 0)
            || (operation != LanceWriteOperation::Create && expected_read_version == 0)
        {
            set_last_error(
                "CREATE requires version 0; APPEND/OVERWRITE require a non-zero version",
            );
            return -1;
        }
        let schema_fence_sha256 =
            match parse_sha256(schema_fence_sha256, schema_fence_len, "schema_fence_sha256") {
                Ok(value) => value,
                Err(error) => {
                    set_last_error(&error);
                    return -1;
                }
            };
        let owned = match unsafe { copy_commit_fragment_views(fragments, fragment_count) } {
            Ok(value) => value,
            Err(error) => {
                set_last_error(&error);
                return -1;
            }
        };
        let runtime = get_or_create_runtime();
        let storage = require_storage!(storage, &uri, Some(&tenant_id), -1);
        match runtime.block_on(discard_fragments_impl(
            &storage,
            uri,
            write_id,
            tenant_id,
            generation,
            operation,
            expected_read_version,
            schema_fence_sha256,
            owned,
        )) {
            Ok(()) => 0,
            Err(error) => {
                set_last_error(&error);
                -1
            }
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

    fn test_manifest_writer<'a>(
        store: &'a ObjectStore,
        manifest: &'a mut Manifest,
        _indices: Option<Vec<Index>>,
        path: &'a Path,
    ) -> futures::future::BoxFuture<'a, lance::Result<()>> {
        let marker = manifest.tag.as_deref().unwrap_or("test-manifest");
        Box::pin(async move { store.put(path, marker.as_bytes()).await })
    }

    #[test]
    fn conditional_manifest_commit_has_one_winner_for_same_version() {
        let runtime = get_or_create_runtime();
        runtime.block_on(async {
            let store = ObjectStore::memory();
            let handler = ConditionalPutCommitHandler;
            let mut first_manifest = Manifest::new(
                lance_core::datatypes::Schema::default(),
                Arc::new(Vec::new()),
                lance_table::format::DataStorageFormat::default(),
                None,
            );
            first_manifest.version = 1;
            first_manifest.tag = Some("writer-one".to_owned());
            let mut second_manifest = first_manifest.clone();
            second_manifest.tag = Some("writer-two".to_owned());
            let base = Path::from("test-dataset");
            let (first, second) = futures::join!(
                handler.commit(
                    &mut first_manifest,
                    None,
                    &base,
                    &store,
                    test_manifest_writer,
                    ManifestNamingScheme::V2,
                ),
                handler.commit(
                    &mut second_manifest,
                    None,
                    &base,
                    &store,
                    test_manifest_writer,
                    ManifestNamingScheme::V2,
                )
            );
            assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
            assert_eq!(
                usize::from(matches!(first, Err(CommitError::CommitConflict)))
                    + usize::from(matches!(second, Err(CommitError::CommitConflict))),
                1
            );
            let path = ManifestNamingScheme::V2.manifest_path(&base, 1);
            let payload = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
            let expected = if first.is_ok() {
                b"writer-one".as_slice()
            } else {
                b"writer-two".as_slice()
            };
            assert_eq!(payload.as_ref(), expected);
        });
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
        let runtime = get_or_create_runtime();
        runtime.block_on(async {
            let (store, base) = ObjectStore::from_uri_and_params(
                Arc::new(ObjectStoreRegistry::default()),
                &uri,
                &params,
            )
            .await
            .unwrap();
            let (second_store, _) = ObjectStore::from_uri_and_params(
                Arc::new(ObjectStoreRegistry::default()),
                &uri,
                &params,
            )
            .await
            .unwrap();
            let handler = ConditionalPutCommitHandler;
            let mut first_manifest = Manifest::new(
                lance_core::datatypes::Schema::default(),
                Arc::new(Vec::new()),
                lance_table::format::DataStorageFormat::default(),
                None,
            );
            first_manifest.version = 1;
            first_manifest.tag = Some("writer-one".to_owned());
            let mut second_manifest = first_manifest.clone();
            second_manifest.tag = Some("writer-two".to_owned());
            let (first, second) = futures::join!(
                handler.commit(
                    &mut first_manifest,
                    None,
                    &base,
                    &store,
                    test_manifest_writer,
                    ManifestNamingScheme::V2,
                ),
                handler.commit(
                    &mut second_manifest,
                    None,
                    &base,
                    &second_store,
                    test_manifest_writer,
                    ManifestNamingScheme::V2,
                )
            );
            let path = ManifestNamingScheme::V2.manifest_path(&base, 1);
            if supports_conditional_create {
                assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
                assert_eq!(
                    usize::from(matches!(first, Err(CommitError::CommitConflict)))
                        + usize::from(matches!(second, Err(CommitError::CommitConflict))),
                    1
                );
                let expected = if first.is_ok() {
                    b"writer-one".as_slice()
                } else {
                    b"writer-two".as_slice()
                };
                let payload = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(payload.as_ref(), expected);
                let mut late_manifest = first_manifest.clone();
                late_manifest.tag = Some("late-writer".to_owned());
                assert!(matches!(
                    handler
                        .commit(
                            &mut late_manifest,
                            None,
                            &base,
                            &store,
                            test_manifest_writer,
                            ManifestNamingScheme::V2,
                        )
                        .await,
                    Err(CommitError::CommitConflict)
                ));
                let unchanged = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(unchanged.as_ref(), expected);
            } else {
                for result in [first, second] {
                    let error = result.expect_err("unsafe MinIO conditional PUT must fail closed");
                    assert!(format!("{error:?}").contains("ignored conditional create"));
                }
                assert!(matches!(
                    store.inner.head(&path).await,
                    Err(ObjectStoreError::NotFound { .. })
                ));
            }
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

    fn recovery_test_reference(task_id: &str, digest: u8) -> ValidatedFragmentReference {
        let attempt = CanonicalTaskAttemptId {
            query_or_job_id: "query".to_owned(),
            stage_id: 1,
            partition_id: digest as i32,
            plan_epoch: 1,
            attempt_id: 0,
            fencing_token: 1,
            task_id: task_id.to_owned(),
            control_epoch: 1,
            execution_owner: Some(CanonicalExecutionOwnerIdentity {
                owner_id: "coordinator".to_owned(),
                owner_incarnation: "incarnation".to_owned(),
                fencing_token: 1,
            }),
        };
        ValidatedFragmentReference {
            fragment: LanceWriteFragment {
                contract_version: LANCE_WRITE_FRAGMENT_VERSION,
                dataset_uri: "dataset".to_owned(),
                write_id: "write".to_owned(),
                task_attempt: Some(attempt),
                operation: LanceWriteOperation::Append as i32,
                read_version: 1,
                schema_fence_sha256: vec![7; SHA256_DIGEST_SIZE],
                transaction: None,
                rows_written: 1,
                input_bytes: 1,
            },
            transaction_digest: vec![digest; SHA256_DIGEST_SIZE],
            relative_object_key: "unused".to_owned(),
            transaction_size: 1,
        }
    }

    fn recovery_test_identity(
        generation: &str,
        mut references: Vec<ValidatedFragmentReference>,
    ) -> (String, String) {
        let schema = [7; SHA256_DIGEST_SIZE];
        fragment_recovery_identity(
            &mut references,
            &RecoveryFence {
                dataset_uri: "dataset",
                write_id: "write",
                tenant_id: "tenant",
                generation,
                operation: LanceWriteOperation::Append,
                expected_read_version: 1,
                schema_fence_sha256: &schema,
            },
        )
    }

    #[test]
    fn recovery_scan_is_lazy_and_covers_the_full_version_history() {
        let mut versions = recovery_scan_versions(1, 10_000_000, 0).unwrap();
        assert_eq!(versions.next(), Some(10_000_000));
        assert_eq!(versions.next_back(), Some(2));
        assert_eq!(
            recovery_scan_versions(1, 100, 0)
                .unwrap()
                .find(|version| *version == 2),
            Some(2)
        );
    }

    #[test]
    fn recovery_journal_hint_checks_only_the_recorded_version() {
        assert_eq!(
            recovery_scan_versions(1, 10_000_000, 42)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![42]
        );
        assert!(recovery_scan_versions(1, 41, 42).is_err());
        assert!(recovery_scan_versions(42, 100, 42).is_err());
    }

    #[test]
    fn recovery_scan_never_precedes_the_read_version() {
        assert_eq!(
            recovery_scan_versions(999_995, 1_000_000, 0)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![1_000_000, 999_999, 999_998, 999_997, 999_996]
        );
        assert_eq!(recovery_scan_versions(7, 7, 0).unwrap().next(), None);
    }

    #[test]
    fn recovery_identity_is_order_independent_and_fences_generation() {
        let first = recovery_test_identity(
            "generation-1",
            vec![
                recovery_test_reference("task-b", 2),
                recovery_test_reference("task-a", 1),
            ],
        );
        let reordered = recovery_test_identity(
            "generation-1",
            vec![
                recovery_test_reference("task-a", 1),
                recovery_test_reference("task-b", 2),
            ],
        );
        let next_generation = recovery_test_identity(
            "generation-2",
            vec![
                recovery_test_reference("task-a", 1),
                recovery_test_reference("task-b", 2),
            ],
        );

        assert_eq!(first, reordered);
        assert_eq!(first.0, next_generation.0);
        assert_ne!(first.1, next_generation.1);
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
