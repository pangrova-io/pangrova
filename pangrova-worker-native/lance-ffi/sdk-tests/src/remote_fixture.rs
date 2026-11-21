use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use object_store::aws::AmazonS3Builder;
use object_store::{ClientOptions, RetryConfig};

pub struct RemoteStore {
    network: String,
    containers: Vec<String>,
    pub bucket: String,
    direct_endpoint: String,
    proxy_endpoint: String,
    control_endpoint: String,
}

fn docker(arguments: &[&str]) -> Output {
    Command::new("docker")
        .args(arguments)
        .output()
        .expect("Docker is required for real SDK cancellation tests")
}

fn checked(arguments: &[&str]) -> String {
    let result = docker(arguments);
    assert!(
        result.status.success(),
        "Docker operation failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}

fn mapped_endpoint(container: &str, port: &str) -> String {
    let mapping = checked(&["port", container, port]);
    let port = mapping.rsplit(':').next().unwrap().parse::<u16>().unwrap();
    format!("http://127.0.0.1:{port}")
}

impl RemoteStore {
    pub async fn start() -> Self {
        let network = format!("pangrova-sdk-drain-{}", uuid::Uuid::new_v4());
        checked(&["network", "create", &network]);
        let mut fixture = Self {
            network,
            containers: Vec::new(),
            bucket: format!("sdk-drain-{}", uuid::Uuid::new_v4()),
            direct_endpoint: String::new(),
            proxy_endpoint: String::new(),
            control_endpoint: String::new(),
        };
        let minio = checked(&[
            "run",
            "-d",
            "--rm",
            "--network",
            &fixture.network,
            "--network-alias",
            "actual-minio",
            "-p",
            "127.0.0.1::9000",
            "-e",
            "MINIO_ROOT_USER=minioadmin",
            "-e",
            "MINIO_ROOT_PASSWORD=minioadmin",
            "tobi312/minio:RELEASE.2025-02-28T09-55-16Z",
            "server",
            "/data",
        ]);
        fixture.containers.push(minio.clone());
        fixture.direct_endpoint = mapped_endpoint(&minio, "9000/tcp");
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let result = docker(&[
                    "exec",
                    &minio,
                    "mc",
                    "alias",
                    "set",
                    "pangrova",
                    "http://127.0.0.1:9000",
                    "minioadmin",
                    "minioadmin",
                ]);
                if result.status.success() {
                    break;
                }
                tick.tick().await;
            }
        })
        .await
        .expect("actual MinIO readiness timed out");
        checked(&[
            "exec",
            &minio,
            "mc",
            "mb",
            &format!("pangrova/{}", fixture.bucket),
        ]);
        let proxy = checked(&[
            "run",
            "-d",
            "--rm",
            "--network",
            &fixture.network,
            "-p",
            "127.0.0.1::8474",
            "-p",
            "127.0.0.1::8666",
            "ghcr.io/shopify/toxiproxy:2.9.0",
        ]);
        fixture.containers.push(proxy.clone());
        fixture.proxy_endpoint = mapped_endpoint(&proxy, "8666/tcp");
        fixture.control_endpoint = mapped_endpoint(&proxy, "8474/tcp");
        let client = reqwest::Client::new();
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Ok(response) = client
                    .get(format!("{}/version", fixture.control_endpoint))
                    .send()
                    .await
                {
                    if response.status().is_success() {
                        break;
                    }
                }
                tick.tick().await;
            }
        })
        .await
        .expect("actual Toxiproxy readiness timed out");
        fixture.control("/proxies", r#"{"name":"actual-write","listen":"0.0.0.0:8666","upstream":"actual-minio:9000","enabled":true}"#).await;
        fixture
    }

    pub async fn control(&self, path: &str, body: &str) {
        let response = reqwest::Client::new()
            .post(format!("{}{path}", self.control_endpoint))
            .header("Content-Type", "application/json")
            .body(body.to_owned())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let diagnostic = response.text().await.unwrap();
        assert!(
            status.is_success(),
            "actual proxy control failed: {status} {diagnostic}"
        );
    }

    pub async fn store(
        &self,
        proxied: bool,
        object: &str,
    ) -> (ObjectStore, object_store::path::Path) {
        let backend = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region("us-east-1")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_endpoint(if proxied {
                &self.proxy_endpoint
            } else {
                &self.direct_endpoint
            })
            .with_client_options(
                ClientOptions::default()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_secs(5)),
            )
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()
            .unwrap();
        let uri = format!("s3://{}/{object}", self.bucket);
        ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            &uri,
            &ObjectStoreParams {
                object_store: Some((Arc::new(backend), url::Url::parse(&uri).unwrap())),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    pub fn assert_no_incomplete_uploads(&self) {
        let output = checked(&[
            "exec",
            &self.containers[0],
            "mc",
            "ls",
            "--incomplete",
            "--recursive",
            &format!("pangrova/{}", self.bucket),
        ]);
        assert!(
            output.is_empty(),
            "actual multipart upload remains: {output}"
        );
    }

    pub fn assert_actual_incomplete_upload_exists(&self) {
        let output = checked(&[
            "exec",
            &self.containers[0],
            "mc",
            "ls",
            "--incomplete",
            "--recursive",
            &format!("pangrova/{}", self.bucket),
        ]);
        assert!(
            !output.is_empty(),
            "SDK must actually create a multipart upload before abort"
        );
    }
}

impl Drop for RemoteStore {
    fn drop(&mut self) {
        while let Some(id) = self.containers.pop() {
            assert!(id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()));
            checked(&["rm", "-f", &id]);
        }
        checked(&["network", "rm", &self.network]);
    }
}
