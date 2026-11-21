//! Official SDK resolver and preparation handler for catalog-owned source manifests.
//! No method delegates to filesystem version discovery or publishes a live SDK tag.
use std::sync::Arc;
use sha2::Digest;

use async_trait::async_trait;
use futures::{stream, stream::BoxStream, StreamExt};
use lance::dataset::builder::DatasetBuilder;
use lance::Dataset;
use lance_io::object_store::ObjectStore;
use lance_table::format::{Index, Manifest};
use lance_table::io::commit::{CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme, ManifestWriter};
use object_store::{path::Path, ObjectStore as PhysicalStore, PutMode, PutOptions, PutPayload};

use crate::canonical::pangrova::protocol::v2::{checksum::Algorithm, AcceptedSourceSnapshotRef};
use crate::{sha256, AtomicCommitStore, StorageContext, MAX_MANIFEST_BYTES};

pub(crate) fn validate(reference: &AcceptedSourceSnapshotRef, uri: &str, version: u64) -> Result<(), String> {
    if reference.dataset_uri != uri || reference.version != version || version == 0
        || reference.table_id.is_empty() || reference.table_generation == 0 || reference.tenant_id.is_empty()
        || reference.catalog.is_none() || reference.accepted_origin_catalog_revision == 0
        || reference.catalog.as_ref().is_some_and(|catalog| reference.accepted_origin_catalog_revision > catalog.catalog_revision)
        || reference.size_bytes == 0 || reference.size_bytes > MAX_MANIFEST_BYTES as u64
        || !matches!(reference.naming_scheme.as_str(), "V1" | "V2")
        || reference.membership_id.len() != 64
        || !reference.membership_id.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || !reference.manifest_relative_path.starts_with("_pangrova/prepared/")
        || reference.manifest_relative_path != format!("_pangrova/prepared/{}.manifest", reference.membership_id)
        || [&reference.manifest_sha256, &reference.physical_schema_sha256].iter().any(|value| {
            !value.as_ref().is_some_and(|checksum| checksum.algorithm == Algorithm::Sha256 as i32 && checksum.digest.len() == 32)
        })
    {
        return Err("Managed Dataset requires its exact accepted manifest membership".into());
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct CatalogCommitHandler {
    pub(crate) accepted: Option<AcceptedSourceSnapshotRef>,
    pub(crate) candidate: Option<(String, u64, String)>,
}

impl CatalogCommitHandler {
    fn location(&self, base: &Path, version: Option<u64>) -> lance::Result<ManifestLocation> {
        let reference = self.accepted.as_ref().filter(|reference| version.is_none_or(|v| reference.version == v))
            .ok_or_else(|| lance::Error::DatasetNotFound {
                path: base.to_string(), source: "No accepted source membership for this exact version".into(),
                location: Default::default(),
            })?;
        let path = crate::object_path(base, &reference.manifest_relative_path)
            .map_err(std::io::Error::other)?;
        Ok(ManifestLocation { version: reference.version, path,
            size: Some(reference.size_bytes), naming_scheme: if reference.naming_scheme == "V1" {
                ManifestNamingScheme::V1
            } else { ManifestNamingScheme::V2 } })
    }
}

#[async_trait]
impl CommitHandler for CatalogCommitHandler {
    async fn resolve_latest_location(&self, base: &Path, _: &ObjectStore) -> lance::Result<ManifestLocation> {
        self.location(base, None)
    }
    async fn resolve_latest_version(&self, base: &Path, _: &ObjectStore) -> lance::Result<Path> {
        Ok(self.location(base, None)?.path)
    }
    async fn resolve_latest_version_id(&self, base: &Path, _: &ObjectStore) -> lance::Result<u64> {
        Ok(self.location(base, None)?.version)
    }
    async fn resolve_version(&self, base: &Path, version: u64, _: &dyn PhysicalStore) -> lance::Result<Path> {
        Ok(self.location(base, Some(version))?.path)
    }
    async fn resolve_version_location(&self, base: &Path, version: u64, _: &dyn PhysicalStore)
            -> lance::Result<ManifestLocation> {
        self.location(base, Some(version))
    }
    async fn list_manifests<'a>(&self, base: &Path, _: &'a dyn PhysicalStore) -> lance::Result<BoxStream<'a, lance::Result<Path>>> {
        let paths = self.accepted.as_ref().map(|_| self.location(base, None).map(|location| location.path)).into_iter();
        Ok(stream::iter(paths.collect::<Vec<_>>()).boxed())
    }
    async fn delete(&self, _: &Path) -> lance::Result<()> {
        Err(std::io::Error::other("Managed source membership is removed only by metadata authority").into())
    }
    async fn commit(&self, manifest: &mut Manifest, indices: Option<Vec<Index>>, base: &Path,
            store: &ObjectStore, writer: ManifestWriter, _: ManifestNamingScheme) -> Result<Path, CommitError> {
        let (relative, version, fingerprint) = self.candidate.as_ref().ok_or_else(|| CommitError::OtherError(
            std::io::Error::other("Frozen accepted source resolver cannot publish manifests").into()))?;
        if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || relative != &format!("_pangrova/prepared/{fingerprint}.manifest") || *version == 0
            || manifest.version != *version || manifest.tag.as_ref() != Some(fingerprint) || manifest.blob_dataset_version.is_some() {
            return Err(CommitError::CommitConflict);
        }
        crate::verify_conditional_create(store, base).await?;
        let final_path = crate::object_path(base, relative)
            .map_err(|error| CommitError::OtherError(std::io::Error::other(error).into()))?;
        let temp_path = crate::object_path(base, &format!("_pangrova/prepared/.{}-{}.tmp", fingerprint, uuid::Uuid::new_v4()))
            .map_err(|error| CommitError::OtherError(std::io::Error::other(error).into()))?;
        let result = async {
            writer(store, manifest, indices, &temp_path).await?;
            let bytes = store.inner.get(&temp_path).await.map_err(|error| CommitError::OtherError(error.into()))?
                .bytes().await.map_err(|error| CommitError::OtherError(error.into()))?;
            if bytes.is_empty() || bytes.len() > MAX_MANIFEST_BYTES {
                return Err(CommitError::OtherError(std::io::Error::other("Prepared manifest size violates its bound").into()));
            }
            store.inner.put_opts(&final_path, PutPayload::from(bytes), PutOptions::from(PutMode::Create)).await
                .map_err(|error| match error {
                    object_store::Error::AlreadyExists { .. } => CommitError::CommitConflict,
                    other => CommitError::OtherError(other.into()),
                })?;
            Ok(final_path)
        }.await;
        let cleanup = store.inner.delete(&temp_path).await;
        match (result, cleanup) {
            (Ok(path), Ok(_)) => Ok(path),
            (Ok(_), Err(error)) => Err(CommitError::OtherError(error.into())),
            (Err(error), _) => Err(error),
        }
    }
}

/// Serves the verified manifest bytes to the SDK, including dictionary metadata.
/// DatasetBuilder's local-file fast path would otherwise reopen bytes after their digest was checked.
#[derive(Debug)]
struct FrozenManifestStore {
    physical: Arc<dyn PhysicalStore>,
    frozen: object_store::memory::InMemory,
    path: Option<Path>,
    transactions: Path,
}
impl std::fmt::Display for FrozenManifestStore {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(output, "ManagedManifestStore") }
}
impl FrozenManifestStore {
    fn require_mutable(&self, path: &Path) -> object_store::Result<()> {
        if self.path.as_ref() == Some(path) || path.prefix_matches(&self.transactions) {
            Err(object_store::Error::Generic { store: "managed source", source:
                std::io::Error::other("Accepted source manifest is immutable").into() })
        } else { Ok(()) }
    }
}
#[async_trait]
impl PhysicalStore for FrozenManifestStore {
    async fn put_opts(&self, path: &Path, bytes: PutPayload, options: PutOptions) -> object_store::Result<object_store::PutResult> {
        if path.prefix_matches(&self.transactions) {
            if bytes.content_length() == 0 || bytes.content_length() > crate::MAX_AGGREGATE_TRANSACTION_BYTES {
                return Err(object_store::Error::Generic { store: "managed source", source:
                    std::io::Error::other("SDK aggregate transaction violates its immutable metadata bound").into() });
            }
            // The SDK writes aggregate transactions with an unconditional put. Turn
            // that write into exact immutable create without copying SDK serialization.
            match self.physical.put_opts(path, bytes.clone(), PutOptions { mode: PutMode::Create, ..options }).await {
                Ok(result) => Ok(result),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let metadata = self.physical.head(path).await?;
                    if metadata.size != bytes.content_length() {
                        return Err(object_store::Error::Generic { store: "managed source", source:
                            std::io::Error::other("Existing SDK aggregate transaction has a different size").into() });
                    }
                    let existing = self.physical.get(path).await?;
                    let result = object_store::PutResult { e_tag: existing.meta.e_tag.clone(), version: existing.meta.version.clone() };
                    let actual = existing.bytes().await?;
                    let mut expected = sha2::Sha256::new();
                    for chunk in &bytes { sha2::Digest::update(&mut expected, chunk); }
                    if actual.len() != bytes.content_length() || sha256(&actual) != sha2::Digest::finalize(expected).as_slice() {
                        return Err(object_store::Error::Generic { store: "managed source", source:
                            std::io::Error::other("Existing SDK aggregate transaction has different bytes").into() });
                    }
                    Ok(result)
                }
                Err(error) => Err(error),
            }
        } else {
            self.require_mutable(path)?; self.physical.put_opts(path, bytes, options).await
        }
    }
    async fn put_multipart_opts(&self, path: &Path, options: object_store::PutMultipartOpts)
            -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.require_mutable(path)?; self.physical.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        if self.path.as_ref() == Some(path) { self.frozen.get_opts(path, options).await }
        else { self.physical.get_opts(path, options).await }
    }
    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.require_mutable(path)?; self.physical.delete(path).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.physical.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
        self.physical.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.require_mutable(to)?; self.physical.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.require_mutable(to)?; self.physical.copy_if_not_exists(from, to).await
    }
}

pub(crate) async fn open(storage: &StorageContext, reference: &AcceptedSourceSnapshotRef) -> Result<Dataset, String> {
    open_with_handler(storage, reference, Arc::new(CatalogCommitHandler {
        accepted: Some(reference.clone()), candidate: None,
    })).await
}

pub(crate) async fn open_with_handler(storage: &StorageContext, reference: &AcceptedSourceSnapshotRef,
        handler: Arc<dyn CommitHandler>) -> Result<Dataset, String> {
    validate(reference, &reference.dataset_uri, reference.version)?;
    storage.require_binding(reference.catalog.as_ref().unwrap(), &reference.tenant_id)?;
    storage.require_uri(&reference.dataset_uri, None)?;
    let params = storage.params(AtomicCommitStore::for_uri(&reference.dataset_uri)?.store_params());
    let (store, base) = ObjectStore::from_uri_and_params(Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
            &reference.dataset_uri, &params).await.map_err(|error| error.to_string())?;
    let path = crate::object_path(&base, &reference.manifest_relative_path)?;
    let metadata = store.inner.head(&path).await.map_err(|error| error.to_string())?;
    if metadata.size as u64 != reference.size_bytes || metadata.size > MAX_MANIFEST_BYTES {
        return Err("Accepted source manifest size differs from its frozen membership".into());
    }
    let bytes = store.inner.get(&path).await.map_err(|error| error.to_string())?.bytes().await.map_err(|error| error.to_string())?;
    if bytes.len() as u64 != reference.size_bytes || sha256(&bytes) != reference.manifest_sha256.as_ref().unwrap().digest {
        return Err("Accepted source manifest bytes differ from its frozen membership".into());
    }
    let frozen = object_store::memory::InMemory::new();
    frozen.put(&path, PutPayload::from(bytes)).await.map_err(|error| error.to_string())?;
    let cache = Arc::new(FrozenManifestStore { physical: store.inner.clone(), frozen, path: Some(path), transactions: base.child("_transactions") });
    // This object-store location keeps the exact original base path and explicitly
    // uses the SDK object reader. It cannot take the local-file fast path around the frozen bytes.
    let location = url::Url::parse(&format!("pangrova-managed:///{}", base)).map_err(|error| error.to_string())?;
    let dataset = DatasetBuilder::from_uri(&reference.dataset_uri).with_version(reference.version)
            .with_object_store(cache, location, handler).load().await.map_err(|error| error.to_string())?;
    if dataset.version().version != reference.version
            || dataset.manifest().tag.as_ref() != Some(&reference.membership_id) {
        return Err("SDK manifest version or intent tag differs from accepted membership".into());
    }
    let schema: arrow_schema::Schema = dataset.schema().into();
    if crate::canonical_schema_fingerprint(&schema).as_slice() != reference.physical_schema_sha256.as_ref().unwrap().digest {
        return Err("Accepted source physical schema differs from its frozen membership".into());
    }
    Ok(dataset)
}

/// Uses the same immutable physical-store adapter when preparing the first SDK version.
pub(crate) fn preparation_store(store: &ObjectStore, base: &Path) -> Result<(Arc<dyn PhysicalStore>, url::Url), String> {
    let cache = Arc::new(FrozenManifestStore { physical: store.inner.clone(), frozen: object_store::memory::InMemory::new(),
        path: None, transactions: base.child("_transactions") });
    let location = url::Url::parse(&format!("pangrova-managed:///{}", base)).map_err(|error| error.to_string())?;
    Ok((cache, location))
}
