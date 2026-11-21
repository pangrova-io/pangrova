use super::*;
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field};
use lance::dataset::WriteParams;
use std::sync::Arc;

struct Fixture(std::path::PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

async fn dataset(uri: &str) -> Dataset {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..6))],
    )
    .unwrap();
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
        uri,
        Some(WriteParams {
            max_rows_per_file: 1,
            max_rows_per_group: 1,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

struct SdkPage {
    version: u64,
    total_partitions: u64,
    partitions: Vec<Partition>,
    has_more: bool,
    next_partition_id: u64,
    manifest_sha256: Option<CanonicalChecksum>,
    physical_schema_sha256: Option<CanonicalChecksum>,
}

// Pagination leaf proof: actual SDK history, not an accepted-source wire page.
async fn page(uri: &str, version: u64, after: Option<u64>) -> SdkPage {
    let pinned = crate::sdk_read_test_support::snapshot(uri, version).await;
    let schema: Schema = pinned.schema().into();
    let manifest_sha256 = Some(crate::sdk_read_test_support::checksum(
        crate::sdk_read_test_support::manifest_digest(&pinned).await));
    let physical_schema_sha256 = Some(checksum(&canonical_schema_fingerprint(&schema)));
    let (fragments, has_more) = fragment_page(&pinned.manifest().fragments, after, 2).unwrap();
    let partitions: Vec<_> = fragments.iter().map(|fragment| Partition {
        partition_id: fragment.id, physical_rows: fragment.physical_rows.unwrap() as u64,
        manifest_sha256: manifest_sha256.clone(), physical_schema_sha256: physical_schema_sha256.clone(),
        accepted_snapshot: None,
    }).collect();
    SdkPage { version, total_partitions: pinned.manifest().fragments.len() as u64,
        next_partition_id: if has_more { partitions.last().unwrap().partition_id } else { 0 },
        partitions, has_more, manifest_sha256, physical_schema_sha256 }
}

#[test]
fn real_sdk_fragment_gaps_page_without_sorting_or_refreshing_the_selected_version() {
    let fixture = Fixture(std::env::temp_dir().join(format!(
        "pangrova-fragment-pages-{}.lance",
        uuid::Uuid::new_v4()
    )));
    let uri = fixture.0.to_str().unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut written = dataset(uri).await;
        assert_eq!(written.manifest().fragments.len(), 6);
        let initial = page(uri, 1, None).await;
        assert_eq!(
            initial
                .partitions
                .iter()
                .map(|p| p.partition_id)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(initial.has_more);
        assert_eq!(initial.next_partition_id, 1);

        // The official delete operation removes complete fragments and persists real ID gaps.
        written.delete("id IN (1, 3)").await.unwrap();
        let fragments = written.manifest().fragments.as_slice();
        assert_eq!(
            fragments.iter().map(|f| f.id).collect::<Vec<_>>(),
            vec![0, 2, 4, 5]
        );
        let (borrowed, more) = fragment_page(fragments, Some(2), 2).unwrap();
        assert_eq!(borrowed.as_ptr(), fragments[2..].as_ptr());
        assert!(!more);
        assert!(fragment_page(fragments, Some(1), 2).is_err());
        let first = page(uri, 2, None).await;
        assert_eq!(first.total_partitions, 4);
        assert_eq!(
            first
                .partitions
                .iter()
                .map(|p| p.partition_id)
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert!(first.has_more);
        assert_eq!(first.next_partition_id, 2);
        let last = page(uri, 2, Some(first.next_partition_id)).await;
        assert_eq!(
            last.partitions
                .iter()
                .map(|p| p.partition_id)
                .collect::<Vec<_>>(),
            vec![4, 5]
        );
        assert!(!last.has_more);
        assert_eq!(first.manifest_sha256, last.manifest_sha256);
        assert_eq!(first.physical_schema_sha256, last.physical_schema_sha256);
        let old_next = page(uri, 1, Some(initial.next_partition_id)).await;
        assert_eq!(old_next.version, 1);
        assert_eq!(old_next.total_partitions, 6);
        assert_eq!(
            old_next
                .partitions
                .iter()
                .map(|p| p.partition_id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(initial.manifest_sha256, old_next.manifest_sha256);

        written.delete("id >= 0").await.unwrap();
        let empty = page(uri, 3, None).await;
        assert_eq!(empty.total_partitions, 0);
        assert!(empty.partitions.is_empty());
        assert!(!empty.has_more);
    });
}

#[test]
fn actual_sdk_metadata_rejects_duplicate_or_unordered_ids_and_invalid_page_bounds() {
    let fixture = Fixture(std::env::temp_dir().join(format!(
        "pangrova-fragment-invalid-{}.lance",
        uuid::Uuid::new_v4()
    )));
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let written = dataset(fixture.0.to_str().unwrap()).await;
        let fragments = written.manifest().fragments.as_slice();
        assert!(fragment_page(fragments, None, 0).is_err());
        assert!(fragment_page(fragments, None, 257).is_err());
        let mut duplicate = fragments.to_vec();
        duplicate.insert(1, fragments[0].clone());
        assert!(fragment_page(&duplicate, None, 2)
            .unwrap_err()
            .contains("duplicated"));
        let mut unordered = fragments.to_vec();
        unordered.swap(0, 1);
        assert!(fragment_page(&unordered, None, 2)
            .unwrap_err()
            .contains("ordered"));
    });
}
