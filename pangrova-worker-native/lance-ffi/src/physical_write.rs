//! One physical FileWriter and conditional object publication, never a Dataset transaction.
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use lance_encoding::version::LanceFileVersion;
use lance_file::v2::writer::{FileWriter, FileWriterOptions};
use lance_io::object_store::{ObjectStore, ObjectStoreRegistry};
use lance_io::object_writer::{ObjectWritePublication, ObjectWriter};
use object_store::{PutMode, PutOptions};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::canonical::pangrova::protocol::v2::{
    Checksum, FileFormat, PhysicalColumnStatistics, PhysicalFileReceipt, PhysicalFileWriteBinding,
    PhysicalFileWriteFragment, ResourceUsage, TaskAttemptId,
};
use crate::physical_payload::OwnedPayload;
use crate::physical_schema::adapt_batch;
use crate::physical_stage::FileStage;
use crate::storage::StorageContext;
use crate::{AtomicCommitStore, ChecksumAlgorithm};

pub(crate) struct StageConfig<'a> {
    pub directory: &'a Path,
    pub spill_bytes: u64,
    pub object_write_bytes: u64,
}

pub(crate) struct PhysicalWriter {
    storage: StorageContext,
    binding: PhysicalFileWriteBinding,
    attempt: TaskAttemptId,
    schema: SchemaRef,
    stage: FileStage,
    writer: Option<FileWriter>,
    rows: u64,
    input_bytes: u64,
    resource_usage: ResourceUsage,
    null_counts: Vec<u64>,
    closed: bool,
    published: bool,
    failed: bool,
    pub(crate) drained: bool,
    pub(crate) stage_cleaned: bool,
    pub(crate) staging_publication: Option<ObjectWritePublication>,
}

impl PhysicalWriter {
    pub(crate) async fn open(
        storage: StorageContext,
        bytes: &[u8],
        attempt: TaskAttemptId,
        tenant: &str,
        config: StageConfig<'_>,
    ) -> Result<Self, String> {
        let binding = PhysicalFileWriteBinding::decode(bytes).map_err(|error| error.to_string())?;
        validate_binding(&binding, &attempt)?;
        storage.require_binding(
            binding
                .catalog
                .as_ref()
                .ok_or("Physical catalog is missing")?,
            tenant,
        )?;
        storage.require_uri(&binding.files[0].opaque_location, Some(tenant))?;
        AtomicCommitStore::for_uri(&binding.files[0].opaque_location)?;
        let cache_bytes = binding
            .policy
            .as_ref()
            .ok_or("Physical policy is missing")?
            .writer_buffer_hint_bytes;
        if cache_bytes == 0 || cache_bytes > i64::MAX as u64 || config.object_write_bytes == 0 {
            return Err(
                "Physical writer requires an explicit cache configuration and object-write grant"
                    .into(),
            );
        }
        let reader = StreamReader::try_new(Cursor::new(&binding.physical_schema), None)
            .map_err(|error| format!("Physical schema metadata is invalid: {error}"))?;
        let schema = reader.schema();
        validate_schema(&binding, &schema)?;
        let stage = FileStage::new(
            config.directory,
            config.spill_bytes.min(config.object_write_bytes),
        )?;
        let object = ObjectWriter::new(stage.store(), stage.object())
            .await
            .map_err(|error| error.to_string())?;
        let writer = FileWriter::new_lazy(
            object,
            FileWriterOptions {
                data_cache_bytes: Some(cache_bytes),
                keep_original_array: Some(false),
                format_version: Some(LanceFileVersion::V2_0),
                ..Default::default()
            },
        );
        Ok(Self {
            storage,
            null_counts: vec![0; schema.fields().len()],
            schema,
            binding,
            attempt,
            stage,
            writer: Some(writer),
            rows: 0,
            input_bytes: 0,
            resource_usage: ResourceUsage::default(),
            closed: false,
            published: false,
            failed: false,
            drained: false,
            stage_cleaned: false,
            staging_publication: None,
        })
    }

    pub(crate) async fn write(&mut self, batch: RecordBatch) -> Result<(), String> {
        if self.closed || self.published || self.failed {
            return Err("Physical writer is closed or failed".into());
        }
        self.failed = true;
        let input_bytes = batch.get_array_memory_size() as u64;
        let adapted = adapt_batch(
            &batch,
            self.schema.clone(),
            &mut self.resource_usage.columnar_conversion_copied_bytes,
        )
        .map_err(|error| error.to_string())?;
        self.writer
            .as_mut()
            .ok_or("Physical writer is missing")?
            .write_batch(&adapted)
            .await
            .map_err(|error| error.to_string())?;
        self.rows = self
            .rows
            .checked_add(adapted.num_rows() as u64)
            .ok_or("Physical row count overflow")?;
        self.input_bytes = self
            .input_bytes
            .checked_add(input_bytes)
            .ok_or("Physical input buffer bytes overflow")?;
        for (count, column) in self.null_counts.iter_mut().zip(adapted.columns()) {
            *count = count
                .checked_add(column.null_count() as u64)
                .ok_or("Physical null count overflow")?;
        }
        self.failed = false;
        Ok(())
    }

    /// Closes the actual encoder before the caller obtains an exact-sized Native payload allocation.
    pub(crate) async fn finish_stage(&mut self) -> Result<u64, String> {
        if self.published || self.failed {
            return Err("Physical writer has published or failed".into());
        }
        if !self.closed {
            self.closed = true;
            self.failed = true;
            let writer = self.writer.as_mut().ok_or("Physical writer is missing")?;
            if self.rows == 0 {
                let outcome = writer.abort().await;
                self.drained = true;
                self.staging_publication = Some(outcome.publication);
                if !outcome.failures.is_empty() {
                    return Err(outcome.failures.join("; "));
                }
                self.failed = false;
                return Ok(0);
            }
            let rows = writer.finish().await.map_err(|error| error.to_string())?;
            if rows != self.rows {
                return Err("Closed physical file rows differ from actual writer input".into());
            }
            self.stage.size()?;
            if self.stage.version()? != (0, 3) {
                return Err("Physical writer produced an unexpected actual encoding".into());
            }
            self.failed = false;
        }
        if self.rows == 0 {
            return Ok(0);
        }
        if self.stage.version()? != (0, 3) {
            return Err("Physical writer produced an unexpected actual encoding".into());
        }
        self.stage.size()
    }

    pub(crate) fn empty_fragment(&mut self) -> Result<Vec<u8>, String> {
        if !self.closed || self.rows != 0 || self.published || self.failed {
            return Err(
                "Only a closed, genuinely empty physical writer has an empty receipt".into(),
            );
        }
        self.published = true;
        Ok(self.fragment(Vec::new()).encode_to_vec())
    }

    pub(crate) async fn publish(&mut self, mut payload: OwnedPayload) -> Result<Vec<u8>, String> {
        if !self.closed || self.rows == 0 || self.published || self.failed {
            return Err(
                "Physical publication requires one successfully closed nonempty file".into(),
            );
        }
        self.failed = true;
        let size = self.stage.size()?;
        let (major, minor) = self.stage.version()?;
        let digest = self.stage.load(payload.as_mut_slice())?;
        let minimum = self.binding.files[0]
            .restored_max_sequence
            .checked_add(1)
            .ok_or("Sequence overflow")?;
        let maximum = minimum
            .checked_add(i64::try_from(self.rows - 1).map_err(|_| "Sequence overflow")?)
            .ok_or("Sequence overflow")?;
        let uri = &self.binding.files[0].opaque_location;
        let backend = AtomicCommitStore::for_uri(uri)?;
        let (store, path) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            uri,
            &self.storage.params(backend.store_params()),
        )
        .await
        .map_err(|error| error.to_string())?;
        // No HEAD claim, copy-if-absent emulation or retry with Overwrite. PutPayload clones retain
        // the actual Native pool owner until the last request body clone has been dropped.
        store
            .inner
            .put_opts(
                &path,
                payload.into_bytes().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| format!("Immutable physical file creation failed: {error}"))?;
        self.published = true;
        let columns = self
            .binding
            .columns
            .iter()
            .zip(&self.null_counts)
            .map(|(column, count)| PhysicalColumnStatistics {
                field_id: column.field_id,
                column_name: column.name.clone(),
                null_count: Some(*count),
                collection_policy: "SDK_DISABLED".into(),
                ..Default::default()
            })
            .collect();
        let receipt = PhysicalFileReceipt {
            allocation: Some(self.binding.files[0].clone()),
            size_bytes: size,
            physical_rows: self.rows,
            schema_id: self.binding.schema_id,
            file_format: self.binding.file_format,
            encoding_major: major as u32,
            encoding_minor: minor as u32,
            object_checksum: Some(checksum(&digest)),
            min_sequence: minimum,
            max_sequence: maximum,
            columns,
            ..Default::default()
        };
        Ok(self.fragment(vec![receipt]).encode_to_vec())
    }

    fn fragment(&self, files: Vec<PhysicalFileReceipt>) -> PhysicalFileWriteFragment {
        PhysicalFileWriteFragment {
            contract_version: 1,
            write_id: self.binding.write_id.clone(),
            generation: self.binding.generation.clone(),
            task_attempt: Some(self.attempt.clone()),
            physical_schema_checksum: self.binding.physical_schema_checksum.clone(),
            files,
            input_rows: self.rows,
            input_bytes: self.input_bytes,
        }
    }

    pub(crate) fn resource_usage(&self) -> &ResourceUsage {
        &self.resource_usage
    }

    pub(crate) async fn drain(&mut self) -> Result<(), String> {
        self.closed = true;
        self.failed = true;
        let writer = self.writer.as_mut().ok_or("Physical writer is missing")?;
        let outcome = writer.abort().await;
        // The actual SDK awaits every owned encoder and dispatched upload.
        // Publication errors are not evidence of unfinished CPU / I/O work.
        self.drained = true;
        self.staging_publication = Some(outcome.publication);
        let mut failures = outcome.failures;
        match self.stage.close() {
            Ok(()) => self.stage_cleaned = true,
            Err(error) => {
                failures.push(format!("Private physical staging cleanup failed: {error}"))
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    pub(crate) fn can_destroy(&self) -> bool {
        self.drained && self.stage_cleaned
    }
}

impl Drop for PhysicalWriter {
    fn drop(&mut self) {
        drop(self.writer.take());
    }
}

fn checksum(bytes: &[u8]) -> Checksum {
    Checksum {
        algorithm: ChecksumAlgorithm::Sha256 as i32,
        digest: bytes.to_vec(),
    }
}

fn require_checksum(value: Option<&Checksum>, bytes: &[u8]) -> Result<(), String> {
    if value != Some(&checksum(&Sha256::digest(bytes))) {
        return Err("Physical metadata checksum differs".into());
    }
    Ok(())
}

fn validate_binding(
    binding: &PhysicalFileWriteBinding,
    attempt: &TaskAttemptId,
) -> Result<(), String> {
    attempt.validate()?;
    if binding.file_format != FileFormat::Lance as i32
        || binding.write_id.trim().is_empty()
        || binding.generation.trim().is_empty()
        || binding.schema_id > i64::MAX as u64
        || binding.base_snapshot_id < -1
        || binding.files.len() != 1
        || binding.physical_schema.is_empty()
        || binding.physical_schema.len() > 16 * 1024 * 1024
    {
        return Err("Physical writer requires a bounded exact append allocation".into());
    }
    require_checksum(
        binding.physical_schema_checksum.as_ref(),
        &binding.physical_schema,
    )?;
    let policy = binding
        .policy
        .as_ref()
        .ok_or("Physical policy is missing")?;
    if policy.write_mode != "INSERT"
        || policy.bucket_count != 1
        || !policy.partition_field_ids.is_empty()
        || !policy.primary_key_field_ids.is_empty()
        || policy.options.get("file.format").map(String::as_str) != Some("lance")
    {
        return Err(
            "Physical writer requires the allocated unpartitioned one-bucket append policy".into(),
        );
    }
    let mut declared = policy.clone();
    declared.declaration_checksum = None;
    require_checksum(
        policy.declaration_checksum.as_ref(),
        &declared.encode_to_vec(),
    )?;
    let file = &binding.files[0];
    let mut allocation = file.clone();
    allocation.allocation_checksum = None;
    require_checksum(
        file.allocation_checksum.as_ref(),
        &allocation.encode_to_vec(),
    )?;
    let suffix = format!(
        "attempt-{}",
        crate::hex_digest(&Sha256::digest(attempt.encode_to_vec()))
    );
    if file.task_attempt.as_ref() != Some(attempt)
        || file.bucket_id != 0
        || file.file_ordinal != 0
        || !file.partition.is_empty()
        || file.restored_max_sequence < -1
        || file.file_source != "APPEND"
        || binding.attempt_prefix != format!("{}/{}", binding.allocation_prefix, suffix)
        || !file.file_name.ends_with(&format!(".{suffix}.lance"))
        || file.file_name.contains(['/', '\\'])
        || file.opaque_location != format!("{}/{}", binding.allocation_prefix, file.file_name)
    {
        return Err(
            "Physical file path/provenance differs from its complete fenced task attempt".into(),
        );
    }
    Ok(())
}

fn validate_schema(binding: &PhysicalFileWriteBinding, schema: &SchemaRef) -> Result<(), String> {
    if schema.fields().is_empty()
        || schema.fields().len() != binding.columns.len()
        || schema.fields().len() > 4096
    {
        return Err("Physical schema has no matching bounded ordered columns".into());
    }
    for (field, column) in schema.fields().iter().zip(&binding.columns) {
        if field.name() != &column.name
            || field.is_nullable() != column.nullable
            || column.logical_type.is_none()
            || field.metadata().get("PARQUET:field_id") != Some(&column.field_id.to_string())
        {
            return Err(
                "Physical schema field identity/nullability differs from its canonical column"
                    .into(),
            );
        }
    }
    Ok(())
}
