use std::time::Duration;

use lance_io::object_writer::{ObjectWritePublication, ObjectWriter};
use tokio::io::AsyncWriteExt;

use crate::remote_fixture::RemoteStore;

#[test]
fn actual_minio_multipart_abort_joins_and_removes_unpublished_upload() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = RemoteStore::start().await;
        let (store, object) = fixture.store(false, "multipart.lance").await;
        let mut writer = ObjectWriter::new(&store, &object).await.unwrap();
        let actual = (0..21 * 1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        writer.write_all(&actual).await.unwrap();
        assert!(matches!(
            store.inner.head(&object).await,
            Err(object_store::Error::NotFound { .. })
        ));
        let observed = writer.abort().await;
        assert_eq!(observed.publication, ObjectWritePublication::NotPublished);
        assert!(observed.failures.is_empty(), "{:?}", observed.failures);
        assert!(matches!(
            store.inner.head(&object).await,
            Err(object_store::Error::NotFound { .. })
        ));
        fixture.assert_no_incomplete_uploads();
        assert!(writer.write_all(b"late part").await.is_err());
    });
}

#[test]
fn lost_actual_minio_put_response_is_unknown_and_never_deletes_published_bytes() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let fixture = RemoteStore::start().await;
        let (store, object) = fixture.store(true, "unknown.lance").await;
        let (direct, _) = fixture.store(false, "unknown.lance").await;
        fixture.control("/proxies/actual-write/toxics",
            r#"{"name":"lost-response","type":"timeout","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}"#).await;
        let payload = b"actual durable bytes without their PUT acknowledgement";
        let mut writer = ObjectWriter::new(&store, &object).await.unwrap();
        writer.write_all(payload).await.unwrap();
        let mut finishing = Box::pin(writer.shutdown());
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        let actual = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut finishing => panic!("response should still be withheld: {result:?}"),
                    _ = tick.tick() => {
                        match direct.inner.head(&object).await {
                            Ok(_) => break direct.inner.get(&object).await.unwrap().bytes().await.unwrap(),
                            Err(object_store::Error::NotFound { .. }) => {},
                            Err(error) => panic!("actual direct publication observation failed: {error}"),
                        }
                    }
                }
            }
        }).await.unwrap();
        assert_eq!(actual.as_ref(), payload);
        drop(finishing); // Cancel the SDK API future; retained request ownership must remain.
        fixture.control("/proxies/actual-write", r#"{"enabled":false}"#).await;
        let observed = writer.abort().await;
        assert_eq!(observed.publication, ObjectWritePublication::Unknown);
        assert!(!observed.failures.is_empty());
        assert!(writer.write_all(b"must not overwrite unknown commit").await.is_err());
        let after = direct.inner.get(&object).await.unwrap().bytes().await.unwrap();
        assert_eq!(after, actual);
    });
}
