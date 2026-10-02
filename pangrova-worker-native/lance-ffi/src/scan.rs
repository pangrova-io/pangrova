//! Canonical manifest partition metadata and exact single-partition readers.
use crate::{
    canonical_schema_fingerprint, committed_manifest_digest, open_atomic_dataset,
    remaining_commit_time, CanonicalChecksum, StorageContext,
};
use arrow_schema::Schema;
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
    let dataset = open_atomic_dataset(&request.dataset_uri, storage)
        .await
        .map_err(|error| format!("Failed to open partition dataset: {error}"))?
        .checkout_version(request.version)
        .await
        .map_err(|error| format!("Pinned scan version is unavailable: {error}"))?;
    let schema: Schema = dataset.schema().into();
    let schema = canonical_schema_fingerprint(&schema);
    require_checksum(request.physical_schema_sha256.as_ref(), &schema)?;
    let manifest =
        committed_manifest_digest(storage, &request.dataset_uri, request.version).await?;
    let mut fragments: Vec<&Fragment> = dataset.manifest().fragments.iter().collect();
    fragments.sort_unstable_by_key(|fragment| fragment.id);
    if fragments.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err("Pinned manifest contains duplicate physical partition IDs".into());
    }
    if request.has_cursor
        && !fragments
            .iter()
            .any(|fragment| fragment.id == request.after_partition_id)
    {
        return Err("Dataset partition cursor is absent from the exact manifest".into());
    }
    let total_partitions = fragments.len() as u64;
    let selected: Vec<_> = fragments
        .into_iter()
        .filter(|fragment| !request.has_cursor || fragment.id > request.after_partition_id)
        .take(request.page_size as usize + 1)
        .collect();
    let has_more = selected.len() > request.page_size as usize;
    let partitions: Result<Vec<_>, String> = selected
        .into_iter()
        .take(request.page_size as usize)
        .map(|fragment| {
            Ok(Partition {
                partition_id: fragment.id,
                physical_rows: fragment
                    .physical_rows
                    .ok_or("Manifest partition lacks its physical row bounds")?
                    as u64,
                manifest_sha256: Some(checksum(&manifest)),
                physical_schema_sha256: Some(checksum(&schema)),
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
    }
    .encode_to_vec())
}

pub(crate) async fn select(
    storage: &StorageContext,
    uri: &str,
    version: u64,
    dataset: &Dataset,
    partition: &Partition,
) -> Result<Fragment, String> {
    let schema: Schema = dataset.schema().into();
    require_checksum(
        partition.physical_schema_sha256.as_ref(),
        &canonical_schema_fingerprint(&schema),
    )?;
    require_checksum(
        partition.manifest_sha256.as_ref(),
        &committed_manifest_digest(storage, uri, version).await?,
    )?;
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
