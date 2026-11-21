//! Decodes the canonical Worker-local ObjectStoreExecutionBinding, never ambient credentials.
use lance_io::object_store::ObjectStoreParams;
use prost::Message;
use std::collections::HashMap;

use crate::canonical::pangrova::protocol::v2::{
    CatalogExecutionBinding, ObjectStoreExecutionBinding as Binding,
};

#[derive(Clone)]
pub(crate) struct StorageContext {
    catalog: CatalogExecutionBinding,
    warehouse: String,
    tenant: String,
    options: HashMap<String, String>,
}

impl StorageContext {
    #[cfg(test)]
    pub(crate) fn test_local(uri: &str) -> Self {
        let warehouse = std::path::Path::new(uri)
            .parent()
            .unwrap()
            .to_string_lossy()
            .to_string();
        Self {
            catalog: CatalogExecutionBinding::default(),
            warehouse,
            tenant: "tenant".into(),
            options: HashMap::new(),
        }
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() || bytes.len() > 64 * 1024 {
            return Err("Worker-local storage binding violates its 64 KiB limit".into());
        }
        let binding =
            Binding::decode(bytes).map_err(|_| "Malformed Worker-local storage binding")?;
        if binding.catalog.is_none() || binding.tenant_id.trim().is_empty() {
            return Err("Worker-local storage binding requires catalog and tenant".into());
        }
        normalized(&binding.warehouse_uri)?;
        let mut options = HashMap::new();
        if binding.warehouse_uri.starts_with("s3://") {
            let s3 = binding
                .s3
                .ok_or("S3 requires explicit catalog-scoped storage settings")?;
            let scheme = if s3.tls_enabled {
                "https://"
            } else {
                "http://"
            };
            if !s3.endpoint.starts_with(scheme)
                || s3.endpoint.len() <= scheme.len()
                || s3.endpoint.contains(['@', '?', '#'])
                || s3.region.trim().is_empty()
                || s3.access_key.trim().is_empty()
                || s3.secret_key.trim().is_empty()
            {
                return Err(
                    "S3 endpoint/TLS/region/credentials are incomplete or inconsistent".into(),
                );
            }
            options.extend([
                ("aws_endpoint".into(), s3.endpoint),
                ("aws_region".into(), s3.region),
                ("aws_access_key_id".into(), s3.access_key),
                ("aws_secret_access_key".into(), s3.secret_key),
                ("aws_allow_http".into(), (!s3.tls_enabled).to_string()),
                (
                    "aws_virtual_hosted_style_request".into(),
                    (!s3.path_style_access).to_string(),
                ),
                ("aws_skip_signature".into(), "false".into()),
            ]);
            if !s3.security_token.is_empty() {
                options.insert("aws_session_token".into(), s3.security_token);
            }
        } else if binding.s3.is_some() {
            return Err("Local warehouse must not carry S3 credentials".into());
        }
        Ok(Self {
            catalog: binding.catalog.unwrap(),
            warehouse: binding.warehouse_uri,
            tenant: binding.tenant_id,
            options,
        })
    }

    pub(crate) fn require_binding(
        &self,
        catalog: &CatalogExecutionBinding,
        tenant: &str,
    ) -> Result<(), String> {
        if catalog != &self.catalog || tenant != self.tenant {
            return Err(
                "Metadata request differs from its Worker-local catalog or tenant binding".into(),
            );
        }
        Ok(())
    }

    pub(crate) fn require_uri(&self, uri: &str, tenant: Option<&str>) -> Result<(), String> {
        let (kind, path) = location(uri)?;
        let (root_kind, root) = location(&self.warehouse)?;
        if kind != root_kind
            || path == root
            || !path.starts_with(&root)
            || tenant.is_some_and(|value| value != self.tenant)
        {
            return Err(
                "Dataset URI or tenant escapes its catalog-scoped storage authority".into(),
            );
        }
        Ok(())
    }

    pub(crate) fn params(&self, mut params: ObjectStoreParams) -> ObjectStoreParams {
        params
            .storage_options
            .get_or_insert_with(HashMap::new)
            .extend(self.options.clone());
        params
    }
}

fn normalized(uri: &str) -> Result<(), String> {
    location(uri).map(|_| ())
}

fn location(uri: &str) -> Result<(String, std::path::PathBuf), String> {
    if uri.is_empty()
        || uri.len() > 8192
        || uri.contains(['\\', '?', '#'])
        || !(uri.starts_with('/') || uri.starts_with("file:/") || uri.starts_with("s3://"))
    {
        return Err("Storage location is not a supported normalized absolute URI".into());
    }
    let decoded = percent_encoding::percent_decode_str(uri)
        .decode_utf8()
        .map_err(|_| "Storage location has invalid UTF-8 encoding")?;
    let lower = uri.to_ascii_lowercase();
    if ["%2f", "%5c", "%00", "%25"]
        .iter()
        .any(|value| lower.contains(value))
    {
        return Err("Storage location contains encoded separators".into());
    }
    let path = decoded.as_ref();
    if path
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err("Storage location contains traversal segments".into());
    }
    if uri.starts_with('/') {
        return Ok(("file".into(), std::path::PathBuf::from(path)));
    }
    let parsed = url::Url::parse(uri).map_err(|_| "Malformed storage URI")?;
    if parsed.scheme() == "file" {
        if parsed.host_str().is_some() {
            return Err("File URI has a remote authority".into());
        }
        return Ok((
            "file".into(),
            parsed
                .to_file_path()
                .map_err(|_| "File URI is not absolute")?,
        ));
    }
    let bucket = parsed.host_str().ok_or("S3 URI requires a bucket")?;
    if !parsed.username().is_empty() || parsed.password().is_some() || parsed.port().is_some() {
        return Err("S3 URI has invalid authority".into());
    }
    Ok((
        format!("s3://{bucket}"),
        std::path::PathBuf::from(
            percent_encoding::percent_decode_str(parsed.path())
                .decode_utf8()
                .map_err(|_| "S3 path has invalid UTF-8")?
                .as_ref(),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::pangrova::protocol::v2::S3ExecutionSettings as S3Settings;

    fn binding(warehouse: &str) -> Binding {
        Binding {
            catalog: Some(CatalogExecutionBinding {
                catalog_name: "lake".into(),
                connector_provider: "lance".into(),
                catalog_revision: 1,
                ..Default::default()
            }),
            tenant_id: "tenant-a".into(),
            warehouse_uri: warehouse.into(),
            s3: None,
        }
    }

    #[test]
    fn exact_catalog_tenant_and_path_are_fenced_before_io() {
        let value = binding("/data/foo");
        let storage = StorageContext::decode(&value.encode_to_vec()).unwrap();
        storage
            .require_uri("/data/foo/table.lance", Some("tenant-a"))
            .unwrap();
        storage
            .require_uri("file:/data/foo/table.lance", None)
            .unwrap();
        storage
            .require_uri("file:///data/foo/table.lance", None)
            .unwrap();
        storage
            .require_uri("file:/data/foo/caf%C3%A9.lance", None)
            .unwrap();
        assert!(storage
            .require_uri("file://remote/data/foo/table.lance", None)
            .is_err());
        assert!(storage
            .require_uri("file:/data/foo/%2fescape", None)
            .is_err());
        storage
            .require_binding(value.catalog.as_ref().unwrap(), "tenant-a")
            .unwrap();
        assert!(storage
            .require_uri("/data/foobar/table.lance", None)
            .is_err());
        assert!(storage
            .require_uri("/data/foo/../bar/table.lance", None)
            .is_err());
        assert!(storage
            .require_uri("/data/foo/%2e%2e/table.lance", None)
            .is_err());
        assert!(storage
            .require_uri("/data/foo/table.lance", Some("tenant-b"))
            .is_err());
        assert!(storage
            .require_binding(&CatalogExecutionBinding::default(), "tenant-a")
            .is_err());
    }

    #[test]
    fn s3_requires_explicit_credentials_and_sets_only_scoped_options() {
        let mut value = binding("s3://bucket/warehouse");
        assert!(StorageContext::decode(&value.encode_to_vec()).is_err());
        value.s3 = Some(S3Settings {
            endpoint: "http://minio.example.invalid:9000".into(),
            region: "test-region".into(),
            path_style_access: true,
            tls_enabled: false,
            access_key: "catalog-access".into(),
            secret_key: "catalog-secret".into(),
            security_token: String::new(),
        });
        let storage = StorageContext::decode(&value.encode_to_vec()).unwrap();
        let params = storage.params(ObjectStoreParams::default());
        let options = params.storage_options.unwrap();
        assert_eq!(
            options.get("aws_endpoint").unwrap(),
            "http://minio.example.invalid:9000"
        );
        assert_eq!(options.get("aws_access_key_id").unwrap(), "catalog-access");
        assert_eq!(
            options.get("aws_secret_access_key").unwrap(),
            "catalog-secret"
        );
        assert_eq!(options.get("aws_region").unwrap(), "test-region");
        assert_eq!(options.get("aws_allow_http").unwrap(), "true");
        assert_eq!(
            options.get("aws_virtual_hosted_style_request").unwrap(),
            "false"
        );
        assert!(!options.contains_key("aws_session_token"));
        value.s3.as_mut().unwrap().tls_enabled = true;
        assert!(StorageContext::decode(&value.encode_to_vec()).is_err());
        value.s3.as_mut().unwrap().tls_enabled = false;
        value.s3.as_mut().unwrap().secret_key.clear();
        assert!(StorageContext::decode(&value.encode_to_vec()).is_err());
    }

    #[test]
    fn malformed_or_oversized_storage_never_falls_back_to_environment() {
        assert!(StorageContext::decode(&[]).is_err());
        assert!(StorageContext::decode(&vec![0; 64 * 1024 + 1]).is_err());
        assert!(StorageContext::decode(&binding("relative/path").encode_to_vec()).is_err());
        assert!(StorageContext::decode(&binding("gs://bucket/table").encode_to_vec()).is_err());
    }
}
