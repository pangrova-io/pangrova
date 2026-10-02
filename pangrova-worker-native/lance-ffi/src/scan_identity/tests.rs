use super::*;
use crate::{canonical_schema_fingerprint, committed_manifest_digest, scan, StorageContext};
use arrow_array::{RecordBatchIterator, UInt64Array};
use lance::dataset::{Dataset, WriteParams};
use prost::Message;

fn partition(id: u64, rows: usize) -> Fragment {
    Fragment {
        physical_rows: Some(rows),
        ..Fragment::new(id)
    }
}

fn addresses(values: Vec<Option<u64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            ROW_ADDR,
            DataType::UInt64,
            true,
        )])),
        vec![Arc::new(UInt64Array::from(values))],
    )
    .unwrap()
}

#[test]
fn validates_fragment_bounds_nulls_and_progression_without_collapsing_holes() {
    let columns = vec![POSITION.to_owned()];
    let mut last = None;
    let batch = addresses(vec![Some(7u64 << 32), Some((7u64 << 32) | 3)]);
    let result = materialize(&batch, &partition(7, 8), &columns, &mut last).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 3]
    );
    assert_eq!(last, Some(3));
    for invalid in [
        None,
        Some(8u64 << 32),
        Some((7u64 << 32) | 8),
        Some((7u64 << 32) | 3),
    ] {
        assert!(materialize(
            &addresses(vec![invalid]),
            &partition(7, 8),
            &columns,
            &mut last
        )
        .is_err());
        assert_eq!(last, Some(3));
    }
    assert!(materialize(&batch, &partition(u64::MAX, 8), &columns, &mut None).is_err());
    assert!(materialize(&batch, &partition(7, 1usize << 33), &columns, &mut None).is_err());
}

#[test]
fn maximum_valid_offset_stays_signed_and_duplicate_offset_is_rejected_across_batches() {
    let columns = vec![POSITION.to_owned()];
    let mut last = None;
    let maximum = (5u64 << 32) | u64::from(u32::MAX);
    let batch = addresses(vec![Some(maximum)]);
    let result = materialize(&batch, &partition(5, 1usize << 32), &columns, &mut last).unwrap();
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::from(u32::MAX)
    );
    assert!(materialize(&batch, &partition(5, 1usize << 32), &columns, &mut last).is_err());
}

#[test]
fn row_address_round_trips_sparse_offsets_and_preserves_physical_buffers() {
    for fragment in 0..128u64 {
        let physical = Arc::new(Int64Array::from_iter_values(0..17));
        let offsets: Vec<u32> = (0..17)
            .map(|row| row * ((fragment as u32 % 7) + 1))
            .collect();
        let encoded = UInt64Array::from_iter_values(
            offsets
                .iter()
                .map(|offset| u64::from(RowAddress::new_from_parts(fragment as u32, *offset))),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, false),
                Field::new(ROW_ADDR, DataType::UInt64, false),
            ])),
            vec![physical.clone(), Arc::new(encoded)],
        )
        .unwrap();
        let result = materialize(
            &batch,
            &partition(fragment, 128),
            &[POSITION.into(), "value".into()],
            &mut None,
        )
        .unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            offsets
                .iter()
                .map(|offset| i64::from(*offset))
                .collect::<Vec<_>>()
        );
        assert!(Arc::ptr_eq(result.column(1), batch.column(0)));
    }
}

#[test]
fn rejects_missing_wrong_type_or_colliding_metadata_without_advancing_state() {
    let columns = vec![POSITION.to_owned()];
    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            ROW_ADDR,
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![0]))],
    )
    .unwrap();
    let missing = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(vec![0]))],
    )
    .unwrap();
    let collision = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(ROW_ADDR, DataType::UInt64, false),
            Field::new(POSITION, DataType::Int64, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![0])),
        ],
    )
    .unwrap();
    let mut last = None;
    for invalid in [wrong_type, missing, collision] {
        assert!(materialize(&invalid, &partition(0, 1), &columns, &mut last).is_err());
        assert_eq!(last, None);
    }
    assert!(materialize(
        &addresses(vec![Some(0)]),
        &Fragment::new(0),
        &columns,
        &mut last
    )
    .is_err());
    assert!(materialize(
        &addresses(vec![Some(0)]),
        &partition(0, 1),
        &[POSITION.into(), "absent".into()],
        &mut last
    )
    .is_err());
    assert_eq!(last, None);
}

struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn input() -> RecordBatchIterator<std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>>
{
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..16))],
    )
    .unwrap();
    RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema)
}

async fn canonical_partition(
    dataset: &Dataset,
    storage: &StorageContext,
    id: u64,
) -> scan::Partition {
    let schema: Schema = dataset.schema().into();
    let checksum = |digest: Vec<u8>| crate::CanonicalChecksum {
        algorithm: crate::ChecksumAlgorithm::Sha256 as i32,
        digest,
    };
    scan::Partition {
        partition_id: id,
        physical_rows: dataset
            .manifest()
            .fragments
            .iter()
            .find(|fragment| fragment.id == id)
            .unwrap()
            .physical_rows
            .unwrap() as u64,
        physical_schema_sha256: Some(checksum(canonical_schema_fingerprint(&schema).to_vec())),
        manifest_sha256: Some(checksum(
            committed_manifest_digest(storage, dataset.uri(), dataset.version().version)
                .await
                .unwrap()
                .to_vec(),
        )),
    }
}

struct Reader(*mut crate::LanceCReader);
impl Drop for Reader {
    fn drop(&mut self) {
        crate::lance_ffi_reader_close(self.0);
    }
}

fn open_reader(uri: &str, version: u64, partition: scan::Partition) -> Reader {
    let storage = crate::LanceCStorage {
        context: StorageContext::test_local(uri),
    };
    let detail = crate::FileSplitDetail {
        file_format: crate::FileFormat::Lance as i32,
        read_unit: Some(crate::ReadUnit::ScanPartition(partition)),
        ..Default::default()
    }
    .encode_to_vec();
    let uri = std::ffi::CString::new(uri).unwrap();
    let handle = crate::lance_ffi_reader_open(
        &storage,
        uri.as_ptr(),
        version,
        detail.as_ptr(),
        detail.len(),
        0,
    );
    assert!(
        !handle.is_null(),
        "{}",
        unsafe { std::ffi::CStr::from_ptr(crate::lance_ffi_last_error()) }.to_string_lossy()
    );
    let reader = Reader(handle);
    let names = [
        std::ffi::CString::new(POSITION).unwrap(),
        std::ffi::CString::new("id").unwrap(),
    ];
    let pointers = names.iter().map(|name| name.as_ptr()).collect::<Vec<_>>();
    assert_eq!(
        crate::lance_ffi_reader_set_projection(handle, pointers.as_ptr(), pointers.len()),
        0
    );
    reader
}

fn read_all(reader: &Reader) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    for requested in [1, 3, 2].into_iter().cycle() {
        let mut array = crate::FFI_ArrowArray::empty();
        let mut schema = crate::FFI_ArrowSchema::empty();
        let rc = crate::lance_ffi_reader_next(reader.0, &mut array, &mut schema, requested);
        if rc == 0 {
            return result;
        }
        assert_eq!(
            rc,
            1,
            "{}",
            unsafe { std::ffi::CStr::from_ptr(crate::lance_ffi_last_error()) }.to_string_lossy()
        );
        let data = unsafe { arrow::ffi::from_ffi(array, &schema) }.unwrap();
        let batch = RecordBatch::from(arrow_array::StructArray::from(data));
        assert_eq!(batch.schema().field(0).name(), POSITION);
        let offsets = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            result.push((offsets.value(row), ids.value(row)));
        }
    }
    unreachable!()
}

#[test]
fn real_dataset_deleted_holes_partition_identity_and_historical_manifest_are_exact() {
    let path = std::env::temp_dir().join(format!(
        "pangrova-row-address-{}.lance",
        uuid::Uuid::new_v4()
    ));
    let fixture = Fixture(path);
    let uri = fixture.0.to_str().unwrap();
    let (before, after) = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut dataset = Dataset::write(
            input(),
            uri,
            Some(WriteParams {
                max_rows_per_file: 8,
                max_rows_per_group: 4,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(dataset.manifest().fragments.len(), 2);
        let storage = StorageContext::test_local(uri);
        let before = canonical_partition(&dataset, &storage, 0).await;
        dataset.delete("id IN (1, 3, 10)").await.unwrap();
        let after = canonical_partition(&dataset, &storage, 0).await;
        assert!(scan::select(&storage, uri, 2, &dataset, &before)
            .await
            .is_err());
        (before, after)
    });
    assert_eq!(
        read_all(&open_reader(uri, 1, before)),
        (0..8).map(|id| (id, id)).collect::<Vec<_>>()
    );
    assert_eq!(
        read_all(&open_reader(uri, 2, after)),
        vec![(0, 0), (2, 2), (4, 4), (5, 5), (6, 6), (7, 7)]
    );
    let second = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let storage = StorageContext::test_local(uri);
        let dataset = crate::open_atomic_dataset(uri, &storage)
            .await
            .unwrap()
            .checkout_version(2)
            .await
            .unwrap();
        canonical_partition(&dataset, &storage, 1).await
    });
    assert_eq!(
        read_all(&open_reader(uri, 2, second)),
        vec![(0, 8), (1, 9), (3, 11), (4, 12), (5, 13), (6, 14), (7, 15)]
    );
}
