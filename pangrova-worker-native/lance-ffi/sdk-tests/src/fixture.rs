use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use lance_encoding::version::LanceFileVersion;
use lance_file::v2::writer::{FileWriter, FileWriterOptions};
use lance_io::object_store::ObjectStore;
use object_store::path::Path;

pub struct LocalFile {
    pub directory: PathBuf,
    pub object: Path,
    pub store: ObjectStore,
}

impl LocalFile {
    pub fn new() -> Self {
        let directory =
            std::env::temp_dir().join(format!("pangrova-sdk-drain-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("actual.lance");
        Self {
            directory,
            object: Path::from_absolute_path(path).unwrap(),
            store: ObjectStore::local(),
        }
    }

    pub async fn writer(&self, version: LanceFileVersion) -> FileWriter {
        FileWriter::new_lazy(
            self.store.create(&self.object).await.unwrap(),
            FileWriterOptions {
                data_cache_bytes: Some(1),
                keep_original_array: Some(true),
                format_version: Some(version),
                ..Default::default()
            },
        )
    }

    pub async fn assert_not_published(&self) {
        assert!(matches!(
            self.store.inner.head(&self.object).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
}

impl Drop for LocalFile {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

struct ArrayOwner {
    values: Vec<f64>,
    releases: Arc<AtomicUsize>,
}

impl Drop for ArrayOwner {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::AcqRel);
    }
}

/// Real externally owned, aligned allocation. The SDK must not release it while
/// an actual queued encoder still reads it. No encoder or output is substituted.
pub fn owned_batch(columns: usize) -> (RecordBatch, Arc<AtomicUsize>) {
    owned_batch_rows(columns, 65536)
}

pub fn owned_batch_rows(columns: usize, rows: usize) -> (RecordBatch, Arc<AtomicUsize>) {
    let releases = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(ArrayOwner {
        values: (0..rows).map(|index| index as f64 * 1.0000000001).collect(),
        releases: releases.clone(),
    });
    let len = owner.values.len();
    let pointer = NonNull::new(owner.values.as_ptr().cast_mut().cast::<u8>()).unwrap();
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, len * size_of::<f64>(), owner) };
    let array: ArrayRef = Arc::new(Float64Array::new(ScalarBuffer::new(buffer, 0, len), None));
    let schema = Arc::new(Schema::new(
        (0..columns)
            .map(|column| Field::new(format!("value_{column}"), DataType::Float64, false))
            .collect::<Vec<_>>(),
    ));
    (
        RecordBatch::try_new(schema, vec![array; columns]).unwrap(),
        releases,
    )
}
