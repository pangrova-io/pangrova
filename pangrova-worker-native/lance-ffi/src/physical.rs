//! The normal, single physical-file path through the public Lance SDK metadata and decoder APIs.
//! No Dataset, footer rewrite, dependency patch or retry-to-another-reader is involved.
use std::sync::Arc;

use arrow_schema::Schema;
use futures::{FutureExt, StreamExt};
use lance_core::cache::FileMetadataCache;
use lance_encoding::decoder::{
    schedule_and_decode, DecoderPlugins, FilterExpression, RequestedRows, SchedulerDecoderConfig,
};
use lance_encoding::version::LanceFileVersion;
use lance_encoding::EncodingsIo;
use lance_file::v2::reader::{CachedFileMetadata, FileReader, ReaderProjection};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::scheduler::{FileScheduler, ScanScheduler, SchedulerConfig};
use lance_io::stream::BatchStream;

use crate::canonical::pangrova::protocol::v2::PhysicalFileScanDetail;
use crate::storage::StorageContext;

const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;

/// Scoped read-at adapter using only the public SDK scheduler/encoding APIs.
#[derive(Debug)]
struct PhysicalEncodingIo(FileScheduler);

impl EncodingsIo for PhysicalEncodingIo {
    fn submit_request(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
        priority: u64,
    ) -> futures::future::BoxFuture<'static, lance_core::Result<Vec<bytes::Bytes>>> {
        self.0.submit_request(ranges, priority).boxed()
    }
}

pub(crate) struct PhysicalFileReader {
    metadata: Arc<CachedFileMetadata>,
    io: Arc<PhysicalEncodingIo>,
    version: LanceFileVersion,
    scan: PhysicalFileScanDetail,
    cache: Arc<FileMetadataCache>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Float32Type;
    use arrow_array::{
        Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field};
    use futures::TryStreamExt;
    use lance_file::v2::writer::{FileWriter, FileWriterOptions};

    struct Fixture(std::path::PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).unwrap();
        }
    }

    async fn fixture() -> Fixture {
        let path =
            std::env::temp_dir().join(format!("pangrova-physical-{}.lance", uuid::Uuid::new_v4()));
        let vector = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            [
                Some(vec![Some(1.0), Some(-2.0), Some(3.5)]),
                None,
                Some(vec![Some(0.0), Some(4.0), Some(-5.0)]),
            ],
            3,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("embedding", vector.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![i64::MIN, 7, i64::MAX])),
                Arc::new(StringArray::from(vec![Some("café"), None, Some("last")])),
                Arc::new(vector),
            ],
        )
        .unwrap();
        let store = ObjectStore::local();
        let object = object_store::path::Path::from_absolute_path(&path).unwrap();
        let mut writer = FileWriter::new_lazy(
            store.create(&object).await.unwrap(),
            FileWriterOptions {
                format_version: Some(LanceFileVersion::V2_0),
                ..Default::default()
            },
        );
        writer.write_batch(&batch).await.unwrap();
        assert_eq!(writer.finish().await.unwrap(), 3);
        let fixture = Fixture(path);
        let bytes = std::fs::read(&fixture.0).unwrap();
        // The pinned SDK writer's version_to_numbers spells V2_0 as (0, 3).
        // This corpus proves that real spelling, not a rewritten (2, 0) footer.
        assert_eq!(
            &bytes[bytes.len() - 8..],
            &[0, 0, 3, 0, b'L', b'A', b'N', b'C']
        );
        fixture
    }

    #[test]
    fn real_v2_file_preserves_nullable_vectors_projection_and_exact_ranges() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let file = fixture().await;
            let uri = file.0.to_str().unwrap();
            let storage = StorageContext::test_local(uri);
            let size = std::fs::metadata(&file.0).unwrap().len();
            let scan = PhysicalFileScanDetail {
                physical_rows: 3,
                row_start: 0,
                row_end: 3,
                file_schema_id: 0,
                ..Default::default()
            };
            let reader = PhysicalFileReader::open(&storage, uri, size, scan.clone())
                .await
                .unwrap();
            assert_eq!(reader.version, LanceFileVersion::V2_0);
            let batches = reader
                .start(None)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(ids.values().as_ref(), &[i64::MIN, 7, i64::MAX]);
            let labels = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(labels.value(0), "café");
            assert!(batches[0].column(1).is_null(1));
            assert!(batches[0].column(2).is_null(1));
            let vectors = batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let first_vector = vectors.value(0);
            assert_eq!(
                first_vector
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[1.0, -2.0, 3.5]
            );
            let columns = ["id".to_owned()];
            let projected = PhysicalFileReader::open(
                &storage,
                uri,
                size,
                PhysicalFileScanDetail {
                    row_start: 1,
                    row_end: 3,
                    ..scan.clone()
                },
            )
            .await
            .unwrap();
            let (first, second) = futures::join!(
                projected
                    .start(Some(&columns))
                    .unwrap()
                    .try_collect::<Vec<_>>(),
                reader
                    .start(Some(&columns))
                    .unwrap()
                    .try_collect::<Vec<_>>()
            );
            let first = first.unwrap();
            assert_eq!(first[0].num_columns(), 1);
            let ids = first[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(ids.values().as_ref(), &[7, i64::MAX]);
            assert_eq!(second.unwrap()[0].num_rows(), 3);
            assert!(
                PhysicalFileReader::open(&storage, uri, size + 1, scan.clone())
                    .await
                    .is_err()
            );
            assert!(PhysicalFileReader::open(
                &storage,
                uri,
                size,
                PhysicalFileScanDetail {
                    physical_rows: 4,
                    row_end: 4,
                    ..scan
                }
            )
            .await
            .is_err());
        });
    }
}

impl PhysicalFileReader {
    pub(crate) async fn open(
        storage: &StorageContext,
        uri: &str,
        expected_size: u64,
        scan: PhysicalFileScanDetail,
    ) -> Result<Self, String> {
        if expected_size < 40 || scan.row_start > scan.row_end || scan.row_end > scan.physical_rows
        {
            return Err(
                "Physical file requires exact byte size and bounded physical row interval".into(),
            );
        }
        storage.require_uri(uri, None)?;
        let (store, path) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            uri,
            &storage.params(ObjectStoreParams::default()),
        )
        .await
        .map_err(|error| format!("Physical file store binding failed: {error}"))?;
        let config = SchedulerConfig::max_bandwidth(&store);
        let scheduler = ScanScheduler::new(Arc::new(store), config);
        let file = scheduler
            .open_file(&path)
            .await
            .map_err(|error| error.to_string())?;
        let actual_size = file
            .reader()
            .size()
            .await
            .map_err(|error| error.to_string())? as u64;
        if actual_size != expected_size {
            return Err("Physical file size differs from its table snapshot".into());
        }
        let footer = file
            .submit_single(actual_size - 40..actual_size, 0)
            .await
            .map_err(|error| error.to_string())?;
        if &footer[36..40] != b"LANC" {
            return Err("Physical file has invalid Lance footer magic".into());
        }
        let major = u16::from_le_bytes(footer[32..34].try_into().unwrap()) as u32;
        let minor = u16::from_le_bytes(footer[34..36].try_into().unwrap()) as u32;
        let version = LanceFileVersion::try_from_major_minor(major, minor)
            .map_err(|error| error.to_string())?;
        if !matches!(version, LanceFileVersion::V2_0 | LanceFileVersion::V2_1) {
            return Err("Physical file encoding is not supported by the pinned Native SDK".into());
        }
        let globals = u32::from_le_bytes(footer[24..28].try_into().unwrap()) as u64;
        let columns = u32::from_le_bytes(footer[28..32].try_into().unwrap());
        let global_start = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let global_bytes = globals.checked_mul(16).ok_or("Physical footer overflow")?;
        if globals == 0
            || columns == 0
            || columns > 4096
            || global_start > actual_size - 40
            || global_bytes > actual_size - 40 - global_start
            || global_bytes > MAX_METADATA_BYTES
        {
            return Err("Physical footer metadata offsets violate their bounds".into());
        }
        let first_global = file
            .submit_single(global_start..global_start + 16, 0)
            .await
            .map_err(|error| error.to_string())?;
        let schema_start = u64::from_le_bytes(first_global[..8].try_into().unwrap());
        if schema_start > global_start || actual_size - schema_start > MAX_METADATA_BYTES {
            return Err("Physical metadata exceeds the 64 MiB bound".into());
        }
        let metadata = FileReader::read_all_metadata(&file)
            .await
            .map_err(|error| error.to_string())?;
        if metadata.num_rows != scan.physical_rows {
            return Err("Physical file row count differs from its table snapshot".into());
        }
        // Read-only metadata is left untouched. The SDK's high-level 0.20 constructor
        // calls a stale CachedFileMetadata.version() match; this public decoder path
        // uses the actual validated footer version directly instead.
        let cache = Arc::new(FileMetadataCache::new(128).with_base_path(path));
        Ok(Self {
            metadata: Arc::new(metadata),
            io: Arc::new(PhysicalEncodingIo(file)),
            version,
            scan,
            cache,
        })
    }

    pub(crate) fn schema(&self) -> Schema {
        self.metadata.file_schema.as_ref().into()
    }

    pub(crate) fn start(&self, columns: Option<&[String]>) -> Result<BatchStream, String> {
        let schema = self.metadata.file_schema.as_ref();
        let projection = if let Some(columns) = columns {
            let names = columns.iter().map(String::as_str).collect::<Vec<_>>();
            let projected = schema.project(&names).map_err(|error| error.to_string())?;
            let structural = self.version >= LanceFileVersion::V2_1;
            let indices: std::collections::HashMap<_, _> = schema
                .fields_pre_order()
                .filter(|field| !structural || field.children.is_empty())
                .enumerate()
                .map(|(index, field)| (field.id, index as u32))
                .collect();
            let column_indices = projected
                .fields_pre_order()
                .filter(|field| !structural || field.children.is_empty())
                .map(|field| {
                    indices
                        .get(&field.id)
                        .copied()
                        .ok_or("Physical projection has an unknown field")
                })
                .collect::<Result<Vec<_>, _>>()?;
            ReaderProjection {
                schema: Arc::new(projected),
                column_indices,
            }
        } else {
            ReaderProjection::from_whole_schema(schema, self.version)
        };
        let tasks = schedule_and_decode(
            self.metadata.column_infos.clone(),
            RequestedRows::Ranges(vec![self.scan.row_start..self.scan.row_end]),
            FilterExpression::no_filter(),
            projection.column_indices,
            projection.schema,
            SchedulerDecoderConfig {
                decoder_plugins: Arc::new(DecoderPlugins::default()),
                batch_size: 8192,
                io: self.io.clone(),
                cache: self.cache.clone(),
                should_validate: true,
            },
        );
        Ok(tasks.map(|task| task.task).buffered(2).boxed())
    }

    pub(crate) fn row_start(&self) -> u64 {
        self.scan.row_start
    }
}
