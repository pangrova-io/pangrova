use std::sync::{atomic::Ordering, Arc};

use arrow_array::{RecordBatch, RecordBatchIterator};
use futures::TryStreamExt;
use lance::dataset::{drain_on_error, open_writer, InsertBuilder, WriteParams};
use lance_core::datatypes::Schema;
use lance_core::utils::tokio::CPU_RUNTIME;
use lance_encoding::version::LanceFileVersion;
use lance_io::object_writer::ObjectWritePublication;
use object_store::path::Path;

use crate::fixture::{owned_batch, owned_batch_rows, LocalFile};
use crate::remote_fixture::RemoteStore;
use crate::writer_tests::{poll_pending, Gate, GateOwner};

#[test]
fn retained_dataset_writer_cancellation_awaits_actual_encoding_before_abort_returns() {
    assert_eq!(std::env::var("LANCE_CPU_THREADS").unwrap(), "1");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let gate = GateOwner(Arc::new(Gate::default()));
        let blocked = gate.0.clone();
        let (started, entered) = tokio::sync::oneshot::channel();
        let blocker = CPU_RUNTIME.spawn_blocking(move || {
            started.send(()).unwrap();
            blocked.wait();
        });
        entered.await.unwrap();
        let file = LocalFile::new();
        let base = Path::from_absolute_path(&file.directory).unwrap();
        // Exceeds the real default SDK per-column encoder cache, causing real
        // CPU work to be queued. No production concurrency default is changed.
        let (batch, releases) = owned_batch_rows(2, 2 * 1024 * 1024);
        let schema = Schema::try_from(batch.schema().as_ref()).unwrap();
        let mut writer = open_writer(&file.store, &schema, &base, LanceFileVersion::V2_1)
            .await
            .unwrap();
        let batches = [batch];
        let mut writing = Box::pin(writer.write(&batches));
        poll_pending(&mut writing).await;
        drop(writing);
        drop(batches);
        assert_eq!(releases.load(Ordering::Acquire), 0);
        let mut aborting = Box::pin(writer.abort());
        poll_pending(&mut aborting).await;
        drop(aborting); // The retained SDK owner must survive cancelled cleanup.
        assert_eq!(releases.load(Ordering::Acquire), 0);
        gate.0.release();
        blocker.await.unwrap();
        let observed = writer.abort().await;
        assert_eq!(observed.publication, ObjectWritePublication::NotPublished);
        assert!(observed.failures.is_empty(), "{:?}", observed.failures);
        assert_eq!(releases.load(Ordering::Acquire), 1);
        assert!(writer.finish().await.is_err());
        let objects: Vec<_> = file
            .store
            .inner
            .list(Some(&base))
            .try_collect()
            .await
            .unwrap();
        assert!(objects.is_empty());
    });
}

#[test]
fn dataset_stream_construction_error_returns_only_after_real_writer_owners_release() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let file = LocalFile::new();
        let (batch, releases) = owned_batch(1);
        let schema = batch.schema();
        // Actual Arrow construction rejects missing arrays for the schema.
        // This is the real producer error, not a fabricated success or writer.
        let error = RecordBatch::try_new(schema.clone(), vec![]).unwrap_err();
        let reader = RecordBatchIterator::new(vec![Ok(batch), Err(error)].into_iter(), schema);
        let params = WriteParams {
            max_rows_per_file: 65536,
            ..Default::default()
        };
        let target = file.directory.to_str().unwrap();
        let result = InsertBuilder::new(target)
            .with_params(&params)
            .execute_uncommitted_stream(reader)
            .await;
        assert!(result.is_err());
        assert_eq!(releases.load(Ordering::Acquire), 1);
        assert!(!file.directory.join("_latest.manifest").exists());
    });
}

#[test]
fn dataset_error_after_real_finish_preserves_published_file_bytes() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let file = LocalFile::new();
        let base = Path::from_absolute_path(&file.directory).unwrap();
        let (batch, releases) = owned_batch(1);
        let schema = Schema::try_from(batch.schema().as_ref()).unwrap();
        let mut writer = open_writer(&file.store, &schema, &base, LanceFileVersion::V2_1)
            .await
            .unwrap();
        writer.write(&[batch]).await.unwrap();
        let (rows, data_file) = writer.finish().await.unwrap();
        assert_eq!(rows, 65536);
        let object = base.child("data").child(data_file.path);
        let before = file
            .store
            .inner
            .get(&object)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(!before.is_empty());
        let actual_error =
            std::fs::read(file.directory.join("missing-progress-record")).unwrap_err();
        let error = lance::Error::from(actual_error);
        let diagnostic = error.to_string();
        let result: lance::Result<()> = drain_on_error(writer.as_mut(), Err(error)).await;
        assert_eq!(result.unwrap_err().to_string(), diagnostic);
        let observed = writer.abort().await;
        assert_eq!(observed.publication, ObjectWritePublication::Published);
        assert!(observed.failures.is_empty());
        let after = file
            .store
            .inner
            .get(&object)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(after, before);
        assert_eq!(releases.load(Ordering::Acquire), 1);
    });
}

#[test]
fn dataset_legacy_abort_joins_actual_minio_multipart_upload() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = RemoteStore::start().await;
        let (store, base) = fixture.store(false, "dataset").await;
        let (batch, releases) = owned_batch_rows(1, 3 * 1024 * 1024);
        let schema = Schema::try_from(batch.schema().as_ref()).unwrap();
        let mut writer = open_writer(&store, &schema, &base, LanceFileVersion::Legacy)
            .await
            .unwrap();
        writer.write(&[batch]).await.unwrap();
        fixture.assert_actual_incomplete_upload_exists();
        let observed = writer.abort().await;
        assert_eq!(observed.publication, ObjectWritePublication::NotPublished);
        assert!(observed.failures.is_empty(), "{:?}", observed.failures);
        fixture.assert_no_incomplete_uploads();
        let objects: Vec<_> = store.inner.list(Some(&base)).try_collect().await.unwrap();
        assert!(objects.is_empty());
        assert_eq!(releases.load(Ordering::Acquire), 1);
        assert!(writer.write(&[owned_batch(1).0]).await.is_err());
    });
}
