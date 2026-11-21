//! Canonical manifest partition metadata and exact single-partition readers.
use crate::{
    canonical_schema_fingerprint, managed,
    remaining_commit_time, CanonicalChecksum, StorageContext,
};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::StreamExt;
use lance_io::stream::BatchStream;
use lance::Dataset;
use lance_table::format::Fragment;
use prost::Message;

pub(crate) use crate::canonical::pangrova::protocol::v2::ScanPartitionDetail as Partition;
use crate::canonical::pangrova::protocol::v2::{
    checksum::Algorithm, DatasetScanPartitionsRequest as Request,
    DatasetScanPartitionsResult as Page,
};

fn checksum(bytes: &[u8]) -> CanonicalChecksum {
    CanonicalChecksum {
        algorithm: Algorithm::Sha256 as i32,
        digest: bytes.to_vec(),
    }
}

fn require_checksum(value: Option<&CanonicalChecksum>, expected: &[u8]) -> Result<(), String> {
    if !value.is_some_and(|value| {
        value.algorithm == Algorithm::Sha256 as i32 && value.digest == expected
    }) {
        return Err(
            "Partition manifest or physical schema fingerprint differs from the pinned dataset"
                .into(),
        );
    }
    Ok(())
}

pub(crate) async fn enumerate(storage: &StorageContext, bytes: &[u8]) -> Result<Vec<u8>, String> {
    let request =
        Request::decode(bytes).map_err(|_| "Malformed canonical dataset partition request")?;
    if request.version == 0
        || request.page_size == 0
        || request.page_size > 256
        || request.catalog.is_none()
        || request
            .security
            .as_ref()
            .is_none_or(|value| value.tenant_id.is_empty() || value.principal.is_empty())
    {
        return Err("Dataset scan requires an exact bounded manifest page request".into());
    }
    storage.require_binding(
        request.catalog.as_ref().unwrap(),
        &request.security.as_ref().unwrap().tenant_id,
    )?;
    storage.require_uri(&request.dataset_uri, None)?;
    let remaining = remaining_commit_time(request.deadline_epoch_ms)?;
    tokio::time::timeout(remaining, enumerate_page(storage, request))
        .await
        .map_err(|_| "Dataset manifest enumeration deadline exceeded".to_owned())?
}

async fn enumerate_page(storage: &StorageContext, request: Request) -> Result<Vec<u8>, String> {
    let reference = request.accepted_snapshot.as_ref().ok_or("Dataset scan lacks accepted membership")?;
    managed::validate(reference, &request.dataset_uri, request.version)?;
    if reference.catalog != request.catalog || reference.tenant_id != request.security.as_ref().unwrap().tenant_id
        || reference.physical_schema_sha256 != request.physical_schema_sha256 {
        return Err("Dataset scan differs from its accepted identity, deployment or schema".into());
    }
    let dataset = managed::open(storage, reference).await?;
    let schema: Schema = dataset.schema().into();
    let schema = canonical_schema_fingerprint(&schema);
    require_checksum(request.physical_schema_sha256.as_ref(), &schema)?;
    let manifest = reference.manifest_sha256.as_ref().unwrap().digest.as_slice();
    let fragments = dataset.manifest().fragments.as_slice();
    let total_partitions = fragments.len() as u64;
    let (selected, has_more) = fragment_page(
        fragments,
        request.has_cursor.then_some(request.after_partition_id),
        request.page_size as usize,
    )?;
    let partitions: Result<Vec<_>, String> = selected
        .iter()
        .map(|fragment| {
            Ok(Partition {
                partition_id: fragment.id,
                physical_rows: fragment
                    .physical_rows
                    .ok_or("Manifest partition lacks its physical row bounds")?
                    as u64,
                manifest_sha256: Some(checksum(&manifest)),
                physical_schema_sha256: Some(checksum(&schema)),
                accepted_snapshot: Some(reference.clone()),
            })
        })
        .collect();
    let partitions = partitions?;
    let next_partition_id = if has_more {
        partitions
            .last()
            .ok_or("Empty continuation page")?
            .partition_id
    } else {
        0
    };
    Ok(Page {
        version: request.version,
        manifest_sha256: Some(checksum(&manifest)),
        physical_schema_sha256: Some(checksum(&schema)),
        total_partitions,
        partitions,
        has_more,
        next_partition_id,
        accepted_snapshot: Some(reference.clone()),
    }
    .encode_to_vec())
}

/// The SDK manifest stores fragments in ID order, with gaps allowed. Borrow its
/// bounded page directly; sorting a second full manifest on every RPC is redundant
/// and would conceal a malformed manifest order. Validate once before searching.
fn fragment_page(
    fragments: &[Fragment],
    after: Option<u64>,
    page_size: usize,
) -> Result<(&[Fragment], bool), String> {
    if page_size == 0 || page_size > 256 {
        return Err("Dataset partition page size violates its bound".into());
    }
    if fragments.windows(2).any(|pair| pair[0].id >= pair[1].id) {
        return Err("Pinned manifest partition IDs are duplicated or not strictly ordered".into());
    }
    let start = match after {
        Some(id) => fragments
            .binary_search_by_key(&id, |fragment| fragment.id)
            .map(|index| index + 1)
            .map_err(|_| "Dataset partition cursor is absent from the exact manifest")?,
        None => 0,
    };
    let end = start
        .checked_add(page_size)
        .ok_or("Dataset partition page offset overflow")?
        .min(fragments.len());
    Ok((&fragments[start..end], end < fragments.len()))
}

pub(crate) async fn select(
    storage: &StorageContext,
    uri: &str,
    version: u64,
    dataset: &Dataset,
    partition: &Partition,
) -> Result<Fragment, String> {
    let reference = partition.accepted_snapshot.as_ref().ok_or("Scan partition lacks accepted membership")?;
    managed::validate(reference, uri, version)?;
    storage.require_binding(reference.catalog.as_ref().unwrap(), &reference.tenant_id)?;
    if dataset.version().version != reference.version || dataset.manifest().tag.as_ref() != Some(&reference.membership_id) {
        return Err("Reader Dataset differs from accepted membership".into());
    }
    select_manifest_fragment(dataset, reference.version,
        &reference.manifest_sha256.as_ref().unwrap().digest, partition)
}

/// Physical SDK metadata validation after the caller has established snapshot authority.
/// This function cannot open a Dataset or establish accepted source membership.
pub(crate) fn select_manifest_fragment(dataset: &Dataset, version: u64, manifest_digest: &[u8],
        partition: &Partition) -> Result<Fragment, String> {
    if dataset.version().version != version {
        return Err("Dataset version differs from its pinned manifest".into());
    }
    let schema: Schema = dataset.schema().into();
    require_checksum(partition.physical_schema_sha256.as_ref(), &canonical_schema_fingerprint(&schema))?;
    require_checksum(partition.manifest_sha256.as_ref(), manifest_digest)?;
    let mut matches = dataset
        .manifest()
        .fragments
        .iter()
        .filter(|fragment| fragment.id == partition.partition_id);
    let fragment = matches
        .next()
        .ok_or("Scan partition is absent from the pinned manifest")?;
    if matches.next().is_some()
        || fragment.physical_rows.map(|rows| rows as u64) != Some(partition.physical_rows)
    {
        return Err(
            "Scan partition ID or physical row bounds differ from the pinned manifest".into(),
        );
    }
    Ok(fragment.clone())
}

/// Starts the single physical partition scanner used by the accepted reader.
/// Snapshot identity and catalog authorization remain the caller's mandatory admission.
pub(crate) async fn start_dataset_scan(dataset: &Dataset, partition: &Fragment,
        columns: Option<&[String]>) -> Result<BatchStream, String> {
    let mut scanner = dataset.scan();
    scanner.with_fragments(vec![partition.clone()]);
    if crate::scan_identity::requested(columns) {
        let schema: Schema = dataset.schema().into();
        if schema.index_of(crate::scan_identity::POSITION).is_ok()
                || schema.index_of(lance_core::ROW_ADDR).is_ok() {
            return Err("Dataset schema collides with reserved physical identity metadata".into());
        }
        scanner.with_row_address().scan_in_order(true).use_scalar_index(false);
    }
    if let Some(columns) = columns {
        let projected: Vec<&str> = columns.iter()
            .filter(|column| column.as_str() != crate::scan_identity::POSITION)
            .map(String::as_str).collect();
        scanner.project(&projected).map_err(|error| format!("projection error: {error}"))?;
    }
    scanner.batch_size(8192);
    scanner.try_into_stream().await.map(|stream| stream.boxed())
        .map_err(|error| format!("scan error: {error}"))
}

/// Shares Arrow buffers while preserving the caller's current batch limit and remainder.
pub(crate) fn bounded_batch(batch: RecordBatch, requested: usize) -> (RecordBatch, Option<RecordBatch>) {
    let returned = batch.num_rows().min(requested);
    let remainder = (returned < batch.num_rows()).then(|| batch.slice(returned, batch.num_rows() - returned));
    (batch.slice(0, returned), remainder)
}

#[cfg(test)]
mod tests;
