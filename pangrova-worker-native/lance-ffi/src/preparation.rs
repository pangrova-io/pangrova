//! Immutable SDK candidate preparation. Visibility remains exclusively in PostgreSQL.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use lance::dataset::{CommitBuilder, transaction::{Operation, Transaction}};
use lance_io::object_store::{ObjectStore, ObjectStoreRegistry};
use object_store::path::Path;
use prost::Message;
use sha2::{Digest, Sha256};

use crate::canonical::pangrova::protocol::v2::{self as wire, write_output_binding::Binding};
use crate::{managed, sha256, hex_digest, decode_transaction, operation_fragments,
    transaction_row_count, validate_fragment_reference, AtomicCommitStore, StorageContext,
    LanceWriteOperation, MAX_AGGREGATE_TRANSACTION_BYTES, MAX_MANIFEST_BYTES};

const PAGE_SIZE: usize = 512;
const MAX_PAGES: usize = 4096;
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

fn checksum(bytes: Vec<u8>) -> wire::Checksum {
    wire::Checksum { algorithm: wire::checksum::Algorithm::Sha256 as i32, digest: bytes }
}

fn proof(object: &wire::PreparedSourceObject, digest: &mut Sha256) -> Result<usize, String> {
    crate::validate_relative_object_key(&object.relative_path, "prepared source object")?;
    if !object.relative_path.is_ascii() { return Err("Source object paths must be ASCII".into()); }
    let len = u32::try_from(object.relative_path.len()).map_err(|_| "Source object path exceeds u32")?;
    digest.update(len.to_be_bytes());
    digest.update(object.relative_path.as_bytes());
    digest.update(object.size_bytes.to_be_bytes());
    if object.purpose != wire::SourceObjectPurpose::Source as i32
        && object.purpose != wire::SourceObjectPurpose::WinnerSidecar as i32 { return Err("Invalid source object purpose".into()); }
    digest.update((object.purpose as u32).to_be_bytes());
    Ok(object.relative_path.len() + 16)
}

struct Inputs {
    binding: wire::LanceWriteBinding,
    reservation: wire::SourcePublicationReservation,
    operation: LanceWriteOperation,
    tenant: String,
    references: Vec<crate::ValidatedFragmentReference>,
}

fn validate(request: &wire::PrepareDistributedWriteRequest) -> Result<Inputs, String> {
    if request.encoded_len() > MAX_REQUEST_BYTES || request.fragments.is_empty()
        || request.fragments.len() > 4096 || request.object_page_size as usize != PAGE_SIZE
        || request.object_page_index as usize >= MAX_PAGES || request.execution_id.is_empty()
        || request.control_epoch == 0 || request.commit_fingerprint.len() != 64
        || !request.commit_fingerprint.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    { return Err("Incomplete or oversized canonical source preparation".into()); }
    let security = request.security.as_ref().ok_or("Source preparation requires security")?;
    if security.tenant_id.is_empty() || security.principal.is_empty() { return Err("Missing source identity".into()); }
    let owner = request.execution_owner.as_ref().ok_or("Missing current execution owner")?;
    let original = request.intent_execution_owner.as_ref().ok_or("Missing durable intent owner")?;
    for value in [owner, original] {
        if value.owner_id.is_empty() || value.owner_incarnation.is_empty() || value.fencing_token == 0 {
            return Err("Incomplete execution owner".into());
        }
    }
    if owner.fencing_token < original.fencing_token || (owner.fencing_token == original.fencing_token && owner != original) {
        return Err("Source preparation authority conflicts with durable intent".into());
    }
    let binding = match request.output_binding.as_ref().and_then(|output| output.binding.as_ref()) {
        Some(Binding::Lance(binding)) => binding.clone(), _ => return Err("Source preparation requires Lance binding".into()),
    };
    let operation = LanceWriteOperation::parse(binding.operation)?;
    if (operation == LanceWriteOperation::Create) != (binding.expected_read_version == 0)
        || binding.expected_read_version == u64::MAX || binding.write_id.is_empty() || binding.generation.is_empty()
        || binding.transaction_staging_prefix != binding.allocation_staging_prefix {
        return Err("Source preparation allocation is invalid".into());
    }
    let fence: [u8; 32] = binding.schema_fence_sha256.as_ref()
        .filter(|c| c.algorithm == wire::checksum::Algorithm::Sha256 as i32)
        .ok_or("Missing physical schema fence")?.digest.as_slice().try_into().map_err(|_| "Invalid schema fence")?;
    let reservation = request.reservation.clone().ok_or("Missing metadata reservation")?;
    if reservation.table_id.is_empty() || reservation.table_generation == 0
        || reservation.tenant_id != security.tenant_id
        || reservation.manifest_relative_path != format!("_pangrova/prepared/{}.manifest", request.commit_fingerprint)
        || reservation.base_snapshot != binding.base_snapshot
        || (binding.expected_read_version == 0) != reservation.base_snapshot.is_none() {
        return Err("Source preparation differs from its reserved identity".into());
    }
    if let Some(base) = reservation.base_snapshot.as_ref() {
        managed::validate(base, &binding.dataset_uri, binding.expected_read_version)?;
        if base.table_id != reservation.table_id || base.table_generation != reservation.table_generation
            || base.tenant_id != security.tenant_id || base.catalog != binding.catalog
            || base.physical_schema_sha256 != binding.schema_fence_sha256 {
            return Err("Source preparation accepted base differs from allocation".into());
        }
    }
    let mut references = Vec::with_capacity(request.fragments.len());
    let mut attempts = HashSet::new();
    let mut partitions = HashSet::new();
    let mut tasks = HashSet::new();
    let mut epoch = None;
    let mut total = 0usize;
    for winner in &request.fragments {
        let attempt = winner.task_attempt.as_ref().ok_or("Missing winner identity")?;
        attempt.validate()?;
        if attempt.query_or_job_id != request.execution_id || attempt.execution_owner.as_ref() != Some(original)
            || attempt.control_epoch > request.control_epoch || !attempts.insert(attempt.encode_to_vec())
            || !partitions.insert((attempt.stage_id, attempt.partition_id)) || !tasks.insert(attempt.task_id.clone())
            || epoch.is_some_and(|value| value != attempt.plan_epoch) {
            return Err("Preparation winner provenance, uniqueness or plan epoch is invalid".into());
        }
        epoch = Some(attempt.plan_epoch);
        if winner.sha256.as_ref() != Some(&checksum(sha256(&winner.commit_metadata))) {
            return Err("Winner metadata checksum mismatch".into());
        }
        let prefix = format!("{}/attempt-{}", binding.allocation_staging_prefix, hex_digest(&sha256(&attempt.encode_to_vec())));
        let reference = validate_fragment_reference(&winner.commit_metadata, &prefix, &binding.dataset_uri,
            &binding.write_id, &security.tenant_id, &binding.generation, operation, binding.expected_read_version, &fence)?;
        if reference.fragment.task_attempt.as_ref() != Some(attempt) {
            return Err("Winner sidecar identity differs from receipt".into());
        }
        total = total.checked_add(reference.transaction_size).filter(|v| *v <= MAX_AGGREGATE_TRANSACTION_BYTES)
            .ok_or("Aggregate sidecar metadata exceeds its bound")?;
        references.push(reference);
    }
    references.sort_by_key(|reference| reference.fragment.task_attempt.as_ref().unwrap().encode_to_vec());
    Ok(Inputs { binding, reservation, operation, tenant: security.tenant_id.clone(), references })
}

fn data_objects(fragments: &[lance_table::format::Fragment]) -> Result<HashSet<String>, String> {
    let mut objects = HashSet::new();
    for fragment in fragments {
        // SQL CREATE/APPEND/OVERWRITE emits only immutable data files. Other SDK
        // operations must supply a complete object contract before they can be accepted.
        if fragment.deletion_file.is_some() || fragment.row_id_meta.is_some() {
            return Err("SQL candidate contains unsupported deletion or external row-id objects".into());
        }
        for file in &fragment.files {
            crate::validate_relative_object_key(&file.path, "Lance data file")?;
            let path = format!("data/{}", file.path);
            if !objects.insert(path) { return Err("SDK fragments share a data object unexpectedly".into()); }
        }
    }
    Ok(objects)
}

async fn aggregate(storage: &StorageContext, inputs: &Inputs, fingerprint: &str)
        -> Result<(Transaction, HashSet<String>), String> {
    let mut combined = Vec::new();
    let mut overwrite_schema = None;
    let mut owned = HashSet::new();
    for reference in &inputs.references {
        let bytes = crate::read_transaction_sidecar(storage, &inputs.binding.dataset_uri,
            &reference.relative_object_key, reference.transaction_size).await?;
        if sha256(&bytes) != reference.transaction_digest { return Err("Sidecar SHA-256 differs from winner receipt".into()); }
        let transaction = decode_transaction(&bytes)?;
        if transaction.read_version != inputs.binding.expected_read_version || transaction.blobs_op.is_some()
            || transaction_row_count(&transaction)? != reference.fragment.rows_written {
            return Err("Sidecar version, rows or blob operation violates allocation".into());
        }
        match (&transaction.operation, inputs.operation) {
            (Operation::Append { .. }, LanceWriteOperation::Append)
            | (Operation::Overwrite { .. }, LanceWriteOperation::Create | LanceWriteOperation::Overwrite) => {},
            _ => return Err("Sidecar operation differs from allocation".into()),
        }
        owned.insert(reference.relative_object_key.clone());
        for path in data_objects(operation_fragments(&transaction.operation)?)? {
            if !owned.insert(path) { return Err("Winning attempts reuse a data object".into()); }
        }
        match transaction.operation {
            Operation::Append { fragments } => combined.extend(fragments),
            Operation::Overwrite { fragments, schema, config_upsert_values } => {
                if config_upsert_values.is_some_and(|values| !values.is_empty()) { return Err("SQL sidecar mutates SDK config".into()); }
                if overwrite_schema.as_ref().is_some_and(|value| value != &schema) { return Err("Winning schemas disagree".into()); }
                overwrite_schema = Some(schema);
                combined.extend(fragments);
            }, _ => unreachable!("validated sidecar operation"),
        }
    }
    let operation = if inputs.operation == LanceWriteOperation::Append { Operation::Append { fragments: combined } }
        else { Operation::Overwrite { fragments: combined, schema: overwrite_schema.ok_or("Overwrite has no schema")?, config_upsert_values: None } };
    let mut transaction = Transaction::new(inputs.binding.expected_read_version, operation, None, None);
    transaction.uuid = fingerprint.to_owned();
    transaction.tag = Some(fingerprint.to_owned());
    Ok((transaction, owned))
}

async fn candidate_reference(store: &ObjectStore, base: &Path, inputs: &Inputs, fingerprint: &str)
        -> Result<Option<wire::AcceptedSourceSnapshotRef>, String> {
    let path = crate::object_path(base, &inputs.reservation.manifest_relative_path)?;
    let metadata = match store.inner.head(&path).await {
        Ok(value) => value, Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(format!("Candidate HEAD outcome is unknown: {error}")),
    };
    if metadata.size == 0 || metadata.size > MAX_MANIFEST_BYTES { return Err("Candidate manifest exceeds its bound".into()); }
    let bytes = store.inner.get(&path).await.map_err(|e| e.to_string())?.bytes().await.map_err(|e| e.to_string())?;
    if bytes.len() != metadata.size { return Err("Candidate manifest changed while reading".into()); }
    Ok(Some(wire::AcceptedSourceSnapshotRef { catalog: inputs.binding.catalog.clone(),
        table_id: inputs.reservation.table_id.clone(), table_generation: inputs.reservation.table_generation,
        dataset_uri: inputs.binding.dataset_uri.clone(), version: inputs.binding.expected_read_version + 1,
        manifest_relative_path: inputs.reservation.manifest_relative_path.clone(), manifest_sha256: Some(checksum(sha256(&bytes))),
        physical_schema_sha256: inputs.binding.schema_fence_sha256.clone(), size_bytes: metadata.size as u64,
        naming_scheme: "V2".into(), membership_id: fingerprint.into(), tenant_id: inputs.tenant.clone(),
        accepted_origin_catalog_revision: inputs.binding.catalog.as_ref().ok_or("Missing candidate catalog")?.catalog_revision }))
}

pub(crate) async fn prepare(storage: &StorageContext, request: wire::PrepareDistributedWriteRequest)
        -> Result<wire::PrepareDistributedWriteResult, String> {
    let inputs = validate(&request)?;
    storage.require_binding(inputs.binding.catalog.as_ref().ok_or("Missing catalog binding")?, &inputs.tenant)?;
    storage.require_uri(&inputs.binding.dataset_uri, Some(&inputs.tenant))?;
    let params = storage.params(AtomicCommitStore::for_uri(&inputs.binding.dataset_uri)?.store_params());
    let (store, base) = ObjectStore::from_uri_and_params(Arc::new(ObjectStoreRegistry::default()),
        &inputs.binding.dataset_uri, &params).await.map_err(|e| e.to_string())?;
    let (transaction, mut owned) = aggregate(storage, &inputs, &request.commit_fingerprint).await?;
    let mut reference = candidate_reference(&store, &base, &inputs, &request.commit_fingerprint).await?;
    if reference.is_none() {
        let handler = Arc::new(managed::CatalogCommitHandler { accepted: inputs.reservation.base_snapshot.clone(),
            candidate: Some((inputs.reservation.manifest_relative_path.clone(), inputs.binding.expected_read_version + 1,
                request.commit_fingerprint.clone())) });
        let result = if let Some(accepted) = inputs.reservation.base_snapshot.as_ref() {
            let dataset = managed::open_with_handler(storage, accepted, handler).await?;
            CommitBuilder::new(Arc::new(dataset)).with_max_retries(1).execute(transaction.clone()).await
        } else {
            let mut immutable_params = params;
            immutable_params.object_store = Some(managed::preparation_store(&store, &base)?);
            CommitBuilder::new(inputs.binding.dataset_uri.as_str()).with_store_params(immutable_params)
                .with_commit_handler(handler).with_max_retries(1).execute(transaction.clone()).await
        };
        // SDK ObjectWriter drops an unfinished multipart owner by spawning abort.
        // An Err cannot prove those physical owners joined, even if another caller
        // created the same immutable candidate. Retain this issuance as UNKNOWN;
        // a later read-only invocation may reconcile the exact candidate.
        result.map_err(|error| format!("SDK preparation owner drain is unproved after failure: {error}"))?;
        reference = candidate_reference(&store, &base, &inputs, &request.commit_fingerprint).await?;
        if reference.is_none() { return Err("Successful SDK preparation has no immutable candidate".into()); }
    }
    let reference = reference.unwrap();
    let candidate = managed::open(storage, &reference).await?;
    let actual = candidate.read_transaction().await.map_err(|e| e.to_string())?.ok_or("Candidate has no SDK transaction")?;
    if lance_table::format::pb::Transaction::from(&actual) != lance_table::format::pb::Transaction::from(&transaction) {
        return Err("Reserved candidate contains a different canonical winning transaction".into());
    }
    if candidate.manifest().index_section.is_some() || candidate.manifest().blob_dataset_version.is_some() {
        return Err("SQL candidate unexpectedly references SDK index/blob objects".into());
    }
    let actual_data = data_objects(&candidate.manifest().fragments)?;
    let new_data = data_objects(operation_fragments(&transaction.operation)?)?;
    let mut expected_data = new_data;
    if inputs.operation == LanceWriteOperation::Append {
        let accepted = inputs.reservation.base_snapshot.as_ref().ok_or("APPEND has no accepted base")?;
        let base_dataset = managed::open(storage, accepted).await?;
        if base_dataset.manifest().index_section.is_some() || base_dataset.manifest().blob_dataset_version.is_some() {
            return Err("SQL accepted base contains unsupported SDK index/blob objects".into());
        }
        for path in data_objects(&base_dataset.manifest().fragments)? {
            if !expected_data.insert(path) { return Err("APPEND winner aliases an accepted data object".into()); }
        }
    }
    if actual_data != expected_data { return Err("SDK candidate membership differs from accepted base and exact winners".into()); }
    owned.insert(inputs.reservation.manifest_relative_path.clone());
    let transaction_file = candidate.manifest().transaction_file.as_ref().ok_or("Candidate lacks aggregate transaction object")?;
    crate::validate_relative_object_key(transaction_file, "aggregate transaction file")?;
    owned.insert(format!("_transactions/{transaction_file}"));
    if owned.len() > PAGE_SIZE * MAX_PAGES { return Err("New source object membership exceeds its total bound".into()); }
    let mut objects = BTreeMap::new();
    for path in owned {
        let object_path = crate::object_path(&base, &path)?;
        let metadata = store.inner.head(&object_path).await.map_err(|e| format!("Source object HEAD: {e}"))?;
        let purpose = if inputs.references.iter().any(|reference| reference.relative_object_key == path) {
            wire::SourceObjectPurpose::WinnerSidecar
        } else { wire::SourceObjectPurpose::Source };
        objects.insert(path.clone(), wire::PreparedSourceObject { relative_path: path,
            size_bytes: metadata.size as u64, purpose: purpose as i32 });
    }
    let objects: Vec<_> = objects.into_values().collect();
    let mut total_digest = Sha256::new();
    for object in &objects { proof(object, &mut total_digest)?; }
    let offset = request.object_page_index as usize * PAGE_SIZE;
    if offset >= objects.len() || (offset == 0 && !request.after_object_path.is_empty())
        || (offset > 0 && request.after_object_path != objects[offset - 1].relative_path) {
        return Err("Source object page sequence or cursor differs from immutable membership".into());
    }
    let end = (offset + PAGE_SIZE).min(objects.len());
    let page = objects[offset..end].to_vec();
    let mut page_digest = Sha256::new();
    let mut page_bytes = 0usize;
    for object in &page { page_bytes += proof(object, &mut page_digest)?; }
    if page_bytes > MAX_PAGE_BYTES { return Err("Source object page exceeds its byte bound".into()); }
    let rows = candidate.manifest().fragments.iter().try_fold(0u64, |sum, fragment| {
        sum.checked_add(u64::try_from(fragment.physical_rows.ok_or("Candidate fragment has no physical row count")?)
            .map_err(|_| "Candidate row count exceeds u64")?).ok_or("Candidate row count overflow")
    })?;
    Ok(wire::PrepareDistributedWriteResult { outcome: wire::WritePreparationOutcome::Prepared as i32,
        commit_fingerprint: request.commit_fingerprint, publication: Some(wire::PreparedSourcePublication {
            candidate: Some(reference), row_count: rows, objects_count: objects.len() as u64,
            objects_sha256: Some(checksum(total_digest.finalize().to_vec())), inherited_membership:
                if inputs.operation == LanceWriteOperation::Append { inputs.reservation.base_snapshot.unwrap().membership_id } else { String::new() },
        }), object_page: Some(wire::PreparedSourceObjectPage { page_index: request.object_page_index,
            after_path: request.after_object_path, objects: page, sha256: Some(checksum(page_digest.finalize().to_vec())), is_last: end == objects.len() }),
        invocation_id: request.invocation_id, sdk_joined: true })
}
