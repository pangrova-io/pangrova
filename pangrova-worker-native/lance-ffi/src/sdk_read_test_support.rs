//! SDK leaf facts only. These helpers never fabricate accepted PG source membership.
use std::sync::Arc;
use arrow_schema::Schema;
use lance::Dataset;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use crate::{canonical_schema_fingerprint, scan, sha256, CanonicalChecksum, ChecksumAlgorithm};

pub(crate) async fn snapshot(uri: &str, version: u64) -> Dataset {
    Dataset::open(uri).await.unwrap().checkout_version(version).await.unwrap()
}

pub(crate) async fn manifest_digest(dataset: &Dataset) -> Vec<u8> {
    let (_, base) = ObjectStore::from_uri_and_params(Arc::new(ObjectStoreRegistry::default()),
        dataset.uri(), &ObjectStoreParams::default()).await.unwrap();
    let path = dataset.manifest_naming_scheme.manifest_path(&base, dataset.version().version);
    let bytes = dataset.object_store.inner.get(&path).await.unwrap().bytes().await.unwrap();
    sha256(&bytes)
}

pub(crate) async fn partition(dataset: &Dataset, id: u64) -> scan::Partition {
    let schema: Schema = dataset.schema().into();
    scan::Partition {
        partition_id: id,
        physical_rows: dataset.manifest().fragments.iter().find(|fragment| fragment.id == id)
            .unwrap().physical_rows.unwrap() as u64,
        physical_schema_sha256: Some(checksum(canonical_schema_fingerprint(&schema).to_vec())),
        manifest_sha256: Some(checksum(manifest_digest(dataset).await)),
        accepted_snapshot: None,
    }
}

pub(crate) fn checksum(digest: Vec<u8>) -> CanonicalChecksum {
    CanonicalChecksum { algorithm: ChecksumAlgorithm::Sha256 as i32, digest }
}
