//! Private, bounded file staging. Only the final storage PUT creates the allocated object.
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use lance_io::object_store::ObjectStore as LanceStore;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use sha2::{Digest, Sha256};

/// A write-before byte ledger for this one private writer directory. This is
/// not yet a shared task/global spill lease covering other operators.
pub(crate) struct FileStage {
    directory: PathBuf,
    file: PathBuf,
    store: LanceStore,
    object: Path,
    limit: u64,
}

impl FileStage {
    pub(crate) fn new(parent: &FsPath, spill_bytes: u64) -> std::result::Result<Self, String> {
        if !parent.is_absolute() || spill_bytes == 0 {
            return Err(
                "Physical file staging requires an absolute directory and explicit byte grant"
                    .into(),
            );
        }
        let parent = parent.canonicalize().map_err(|error| error.to_string())?;
        if !parent.is_dir() {
            return Err("Task spill directory is not a directory".into());
        }
        let directory = parent.join(format!("physical-{}", uuid::Uuid::new_v4()));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&directory)
            .map_err(|error| error.to_string())?;
        let file = directory.join("data.lance");
        let object = Path::from_absolute_path(&file).map_err(|error| error.to_string())?;
        let mut store = LanceStore::local();
        store.inner = Arc::new(BoundedStore {
            inner: store.inner.clone(),
            object: object.clone(),
            quota: Arc::new(Quota {
                used: AtomicU64::new(0),
                limit: spill_bytes,
            }),
            opened: AtomicBool::new(false),
        });
        store.use_constant_size_upload_parts = true;
        Ok(Self {
            directory,
            file,
            store,
            object,
            limit: spill_bytes,
        })
    }

    pub(crate) fn store(&self) -> &LanceStore {
        &self.store
    }
    pub(crate) fn object(&self) -> &Path {
        &self.object
    }

    /// Read directly into the caller's real accounted payload allocation, without a file-sized Vec.
    pub(crate) fn load(&self, output: &mut [u8]) -> std::result::Result<[u8; 32], String> {
        let size = self.size()?;
        if output.len() as u64 != size {
            return Err("Payload allocation differs from closed file size".into());
        }
        let mut input = File::open(&self.file).map_err(|error| error.to_string())?;
        input
            .read_exact(output)
            .map_err(|error| error.to_string())?;
        let mut tail = [0u8; 1];
        if input.read(&mut tail).map_err(|error| error.to_string())? != 0 {
            return Err("Closed staged file changed while loading payload".into());
        }
        Ok(Sha256::digest(output).into())
    }

    pub(crate) fn size(&self) -> std::result::Result<u64, String> {
        let size = std::fs::metadata(&self.file)
            .map_err(|error| error.to_string())?
            .len();
        if size < 40 || size > self.limit {
            return Err("Closed physical file violates its staging byte grant".into());
        }
        Ok(size)
    }

    pub(crate) fn version(&self) -> std::result::Result<(u16, u16), String> {
        self.size()?;
        let mut input = File::open(&self.file).map_err(|error| error.to_string())?;
        input
            .seek(SeekFrom::End(-8))
            .map_err(|error| error.to_string())?;
        let mut footer = [0u8; 8];
        input
            .read_exact(&mut footer)
            .map_err(|error| error.to_string())?;
        if &footer[4..] != b"LANC" {
            return Err("Closed physical file has invalid footer magic".into());
        }
        Ok((
            u16::from_le_bytes(footer[..2].try_into().unwrap()),
            u16::from_le_bytes(footer[2..4].try_into().unwrap()),
        ))
    }

    pub(crate) fn close(&self) -> std::result::Result<(), String> {
        match std::fs::remove_dir_all(&self.directory) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }
}

impl Drop for FileStage {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.directory) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!("Physical file staging cleanup failed: {error}");
            }
        }
    }
}

#[derive(Debug)]
struct Quota {
    used: AtomicU64,
    limit: u64,
}
impl Quota {
    fn reserve(&self, bytes: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes as u64)
                    .filter(|next| *next <= self.limit)
            })
            .map(|_| ())
            .map_err(|_| error("Physical staging byte grant exhausted"))
    }
}

fn error(message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "task-bounded-physical-stage",
        source: std::io::Error::other(message).into(),
    }
}

#[derive(Debug)]
struct BoundedStore {
    inner: Arc<dyn ObjectStore>,
    object: Path,
    quota: Arc<Quota>,
    opened: AtomicBool,
}
impl BoundedStore {
    fn claim(&self, location: &Path) -> Result<()> {
        self.require(location)?;
        if self.opened.swap(true, Ordering::AcqRel) {
            return Err(error("Physical stage may only be written once"));
        }
        Ok(())
    }
    fn require(&self, location: &Path) -> Result<()> {
        if location != &self.object {
            return Err(error("Physical stage path escapes its task allocation"));
        }
        Ok(())
    }
}
impl Display for BoundedStore {
    fn fmt(&self, output: &mut Formatter<'_>) -> std::fmt::Result {
        write!(output, "TaskBoundedPhysicalStage")
    }
}

#[async_trait]
impl ObjectStore for BoundedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.claim(location)?;
        self.quota.reserve(payload.content_length())?;
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.claim(location)?;
        Ok(Box::new(BoundedUpload {
            inner: self.inner.put_multipart_opts(location, opts).await?,
            quota: self.quota.clone(),
        }))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.require(location)?;
        self.inner.get_opts(location, options).await
    }
    async fn delete(&self, location: &Path) -> Result<()> {
        self.require(location)?;
        self.inner.delete(location).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        if prefix != Some(&self.object) {
            return futures::stream::once(async {
                Err(error("Physical stage listing escapes its file allocation"))
            })
            .boxed();
        }
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        if prefix != Some(&self.object) {
            return Err(error("Physical stage listing escapes its file allocation"));
        }
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, _: &Path, _: &Path) -> Result<()> {
        Err(error("Physical staging does not support copy"))
    }
    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> Result<()> {
        Err(error("Physical staging does not support copy"))
    }
}

#[derive(Debug)]
struct BoundedUpload {
    inner: Box<dyn MultipartUpload>,
    quota: Arc<Quota>,
}
#[async_trait]
impl MultipartUpload for BoundedUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        if let Err(error) = self.quota.reserve(payload.content_length()) {
            return async move { Err(error) }.boxed();
        }
        self.inner.put_part(payload)
    }
    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
