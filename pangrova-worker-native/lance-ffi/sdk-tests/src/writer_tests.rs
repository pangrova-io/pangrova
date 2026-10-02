use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;

use futures::future::poll_fn;
use lance_core::utils::tokio::CPU_RUNTIME;
use lance_encoding::version::LanceFileVersion;
use lance_io::object_writer::{ObjectWritePublication, ObjectWriter};
use tokio::io::AsyncWriteExt;
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::fixture::{owned_batch, LocalFile};

#[derive(Default)]
pub(crate) struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    pub(crate) fn wait(&self) {
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    pub(crate) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

pub(crate) struct GateOwner(pub(crate) Arc<Gate>);
impl Drop for GateOwner {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(crate) async fn poll_pending<F: Future>(future: &mut std::pin::Pin<Box<F>>) {
    poll_fn(|context| {
        assert!(
            future.as_mut().poll(context).is_pending(),
            "actual operation must be blocked"
        );
        Poll::Ready(())
    })
    .await;
}

#[test]
fn stopped_local_writer_refuses_new_io_and_does_not_publish() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let file = LocalFile::new();
        let mut writer = ObjectWriter::new(&file.store, &file.object).await.unwrap();
        writer.write_all(b"actual unpublished bytes").await.unwrap();
        let observed = writer.abort().await;
        assert_eq!(observed.publication, ObjectWritePublication::NotPublished);
        assert!(observed.failures.is_empty());
        file.assert_not_published().await;
        assert!(writer.write_all(b"late write").await.is_err());
        assert!(writer.shutdown().await.is_err());
        assert_eq!(writer.abort().await.publication, observed.publication);
    });
}

#[test]
fn published_local_object_is_observed_and_never_deleted_by_abort() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let file = LocalFile::new();
        let mut writer = ObjectWriter::new(&file.store, &file.object).await.unwrap();
        writer.write_all(b"actual published bytes").await.unwrap();
        writer.shutdown().await.unwrap();
        let observed = writer.abort().await;
        assert_eq!(observed.publication, ObjectWritePublication::Published);
        assert!(observed.failures.is_empty());
        let bytes = file
            .store
            .inner
            .get(&file.object)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"actual published bytes");
        assert!(writer.write_all(b"must not replace").await.is_err());
        let after = file
            .store
            .inner
            .get(&file.object)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(after, bytes);
    });
}

#[test]
fn cancelled_write_and_cancelled_abort_keep_real_cpu_array_owner_until_drain() {
    assert_eq!(
        std::env::var("LANCE_CPU_THREADS").unwrap(),
        "1",
        "this oracle requires its own explicitly one-slot SDK process"
    );
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
        let mut writer = file.writer(LanceFileVersion::V2_1).await;
        let (batch, releases) = owned_batch(2);
        let mut writing = Box::pin(writer.write_batch(&batch));
        poll_pending(&mut writing).await;
        drop(writing); // Cancel the actual SDK write future, not a synthetic task.
        drop(batch);
        assert_eq!(writer.pending_encoding_tasks(), 2);
        assert_eq!(releases.load(Ordering::Acquire), 0);
        let mut aborting = Box::pin(writer.abort());
        poll_pending(&mut aborting).await;
        drop(aborting); // Cleanup ownership must survive cancellation of its caller.
        assert_eq!(writer.pending_encoding_tasks(), 2);
        assert_eq!(releases.load(Ordering::Acquire), 0);
        gate.0.release();
        blocker.await.unwrap();
        let observed = writer.abort().await;
        assert!(observed.failures.is_empty(), "{:?}", observed.failures);
        assert_eq!(observed.publication, ObjectWritePublication::NotPublished);
        assert_eq!(writer.pending_encoding_tasks(), 0);
        assert_eq!(releases.load(Ordering::Acquire), 1);
        file.assert_not_published().await;
        assert!(writer.finish().await.is_err());
    });
}

struct EncodingObservation {
    gate: Arc<Gate>,
    started: tokio::sync::mpsc::UnboundedSender<()>,
    threads: Arc<Mutex<HashSet<std::thread::ThreadId>>>,
    entered: Arc<AtomicUsize>,
    exited: Arc<AtomicUsize>,
}

impl<S: Subscriber + for<'lookup> LookupSpan<'lookup>> Layer<S> for EncodingObservation {
    fn on_enter(&self, id: &tracing::span::Id, context: Context<'_, S>) {
        if context.span(id).unwrap().metadata().name() != "encode_page" {
            return;
        }
        self.threads
            .lock()
            .unwrap()
            .insert(std::thread::current().id());
        self.entered.fetch_add(1, Ordering::AcqRel);
        self.started.send(()).unwrap();
        self.gate.wait(); // Test subscriber only observes and controls real closure scheduling.
    }
    fn on_exit(&self, id: &tracing::span::Id, context: Context<'_, S>) {
        if context.span(id).unwrap().metadata().name() == "encode_page" {
            self.exited.fetch_add(1, Ordering::AcqRel);
        }
    }
}

#[test]
fn two_cpu_slots_encode_actual_columns_concurrently_and_finish_real_file() {
    if std::env::var_os("PANGROVA_SDK_PARALLEL_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "writer_tests::two_cpu_slots_encode_actual_columns_concurrently_and_finish_real_file", "--nocapture"])
            .env("PANGROVA_SDK_PARALLEL_CHILD", "1").env("LANCE_CPU_THREADS", "2")
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        return;
    }
    assert_eq!(std::env::var("LANCE_CPU_THREADS").unwrap(), "2");
    let gate = GateOwner(Arc::new(Gate::default()));
    let (started, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let threads = Arc::new(Mutex::new(HashSet::new()));
    let entered = Arc::new(AtomicUsize::new(0));
    let exited = Arc::new(AtomicUsize::new(0));
    let subscriber = tracing_subscriber::registry().with(EncodingObservation {
        gate: gate.0.clone(),
        started,
        threads: threads.clone(),
        entered: entered.clone(),
        exited: exited.clone(),
    });
    let _subscriber = tracing::subscriber::set_default(subscriber);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let file = LocalFile::new();
        let mut writer = file.writer(LanceFileVersion::V2_1).await;
        let (batch, releases) = owned_batch(2);
        let mut writing = Box::pin(writer.write_batch(&batch));
        poll_pending(&mut writing).await;
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(10), entered_rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(entered.load(Ordering::Acquire), 2);
        assert_eq!(exited.load(Ordering::Acquire), 0);
        assert_eq!(threads.lock().unwrap().len(), 2);
        assert_eq!(releases.load(Ordering::Acquire), 0);
        gate.0.release();
        writing.await.unwrap();
        assert_eq!(writer.finish().await.unwrap(), batch.num_rows() as u64);
        assert_eq!(entered.load(Ordering::Acquire), 2);
        assert_eq!(exited.load(Ordering::Acquire), 2);
        let actual = file
            .store
            .inner
            .get(&file.object)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(actual.len() > 40);
        assert_eq!(
            &actual[actual.len() - 8..],
            &[2, 0, 1, 0, b'L', b'A', b'N', b'C']
        );
        drop(batch);
        assert_eq!(releases.load(Ordering::Acquire), 1);
    });
}
