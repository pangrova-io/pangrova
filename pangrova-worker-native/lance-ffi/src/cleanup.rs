//! Deletes only an exact bounded authority reservation; never discovers storage objects.
use std::sync::Arc;
use lance_io::object_store::{ObjectStore, ObjectStoreRegistry};
use prost::Message;
use sha2::{Digest, Sha256};
use crate::canonical::pangrova::protocol::v2 as wire;
use crate::{AtomicCommitStore, StorageContext};

pub(crate) async fn cleanup(storage: &StorageContext, request: wire::CleanupSourceObjectsRequest)
        -> Result<wire::CleanupSourceObjectsResult, String> {
    if request.encoded_len() > 8 * 1024 * 1024 || request.objects.is_empty() || request.objects.len() > 512
        || request.execution_id.is_empty() || request.control_epoch == 0
        || request.cleanup_id.len() != 32 || request.commit_fingerprint.len() != 64
        || !request.cleanup_id.bytes().chain(request.commit_fingerprint.bytes())
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)) {
        return Err("Incomplete authoritative cleanup reservation".into());
    }
    let owner = request.execution_owner.as_ref().ok_or("Missing cleanup owner")?;
    if owner.owner_id.is_empty() || owner.owner_incarnation.is_empty() || owner.fencing_token == 0 {
        return Err("Incomplete cleanup execution owner".into());
    }
    let security = request.security.as_ref().ok_or("Missing cleanup security")?;
    let catalog = request.catalog.as_ref().ok_or("Missing cleanup catalog")?;
    if security.tenant_id.is_empty() || security.principal.is_empty() { return Err("Missing cleanup identity".into()); }
    storage.require_binding(catalog, &security.tenant_id)?;
    storage.require_uri(&request.dataset_uri, Some(&security.tenant_id))?;
    let mut digest = Sha256::new();
    let mut previous = "";
    for object in &request.objects {
        crate::validate_relative_object_key(&object.relative_path, "authoritative cleanup object")?;
        if !object.relative_path.is_ascii() || object.relative_path.as_str() <= previous
            || (object.purpose != wire::SourceObjectPurpose::Source as i32
                && object.purpose != wire::SourceObjectPurpose::WinnerSidecar as i32) {
            return Err("Cleanup objects have unknown purpose or conflicting order".into());
        }
        digest.update((object.relative_path.len() as u32).to_be_bytes()); digest.update(object.relative_path.as_bytes());
        digest.update(object.size_bytes.to_be_bytes()); digest.update((object.purpose as u32).to_be_bytes());
        previous = &object.relative_path;
    }
    let expected = request.objects_sha256.as_ref().ok_or("Missing cleanup SHA-256")?;
    if expected.algorithm != wire::checksum::Algorithm::Sha256 as i32 || expected.digest != digest.finalize().to_vec() {
        return Err("Cleanup object proof differs from the authority reservation".into());
    }
    let params = storage.params(AtomicCommitStore::for_uri(&request.dataset_uri)?.store_params());
    let (store, base) = ObjectStore::from_uri_and_params(Arc::new(ObjectStoreRegistry::default()),
        &request.dataset_uri, &params).await.map_err(|error| error.to_string())?;
    // Every SDK call is awaited. A deadline/HTTP cancellation cannot manufacture a joined receipt.
    for object in &request.objects {
        let path = crate::object_path(&base, &object.relative_path)?;
        match store.inner.head(&path).await {
            Ok(metadata) if metadata.size as u64 != object.size_bytes => return Err("Cleanup object size changed".into()),
            Ok(_) => {},
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(error) => return Err(format!("Cleanup HEAD outcome unknown: {error}")),
        }
        match store.inner.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {},
            Err(error) => return Err(format!("Cleanup DELETE outcome unknown: {error}")),
        }
    }
    Ok(wire::CleanupSourceObjectsResult { cleanup_id: request.cleanup_id,
        objects_sha256: request.objects_sha256, deleted_count: request.objects.len() as u32, sdk_joined: true })
}
