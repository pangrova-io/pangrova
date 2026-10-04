package org.pangrova.worker.jni.write.catalog;

import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.ObjectStoreExecutionBinding;
import org.pangrova.protocol.v2.S3ExecutionSettings;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

/** Builds scoped JNI-only storage settings from an already authorized, locally resolved deployment. */
final class NativeObjectStoreBindings {
    private static final int MAX_PROPERTY_LENGTH = 4096;
    private static final int MAX_URI_LENGTH = 8192;

    private NativeObjectStoreBindings() {}

    static ObjectStoreExecutionBinding create(CatalogExecutionBinding catalog, String tenant,
            Map<String, String> resolved) {
        String warehouse = required(resolved, "warehouse");
        URI uri = normalized(warehouse);
        var binding = ObjectStoreExecutionBinding.newBuilder().setCatalog(catalog).setTenantId(tenant)
                .setWarehouseUri(warehouse);
        if ("s3".equals(uri.getScheme())) binding.setS3(s3(resolved));
        else if (uri.getScheme() != null && !"file".equals(uri.getScheme())) {
            throw new IllegalArgumentException("Native dataset warehouse scheme is unsupported");
        }
        return binding.build();
    }

    private static S3ExecutionSettings s3(Map<String, String> values) {
        String endpoint = required(values, "native.storage.s3.endpoint");
        URI uri = URI.create(endpoint);
        boolean tls = bool(values, "native.storage.s3.tls-enabled");
        if (uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null
                || !(tls ? "https" : "http").equals(uri.getScheme())) {
            throw new IllegalArgumentException("Native S3 endpoint and TLS settings are inconsistent");
        }
        return S3ExecutionSettings.newBuilder().setEndpoint(endpoint)
                .setRegion(required(values, "native.storage.s3.region"))
                .setPathStyleAccess(bool(values, "native.storage.s3.path-style-access"))
                .setTlsEnabled(tls).setAccessKey(required(values, "native.storage.s3.access-key"))
                .setSecretKey(required(values, "native.storage.s3.secret-key"))
                .setSecurityToken(values.getOrDefault("native.storage.s3.security-token", "")).build();
    }

    static void requireLocation(ObjectStoreExecutionBinding binding, String location) {
        URI warehouse = normalized(binding.getWarehouseUri());
        URI dataset = normalized(location);
        if (!java.util.Objects.equals(storageScheme(warehouse), storageScheme(dataset))
                || !java.util.Objects.equals(warehouse.getAuthority(), dataset.getAuthority())) {
            throw new SecurityException("Native dataset URI differs from its catalog warehouse");
        }
        Path root = Path.of(warehouse.getPath()).normalize();
        Path path = Path.of(dataset.getPath()).normalize();
        if (!path.startsWith(root) || path.equals(root)) {
            throw new SecurityException("Native dataset URI escapes its catalog warehouse");
        }
    }

    private static URI normalized(String value) {
        URI uri = URI.create(value);
        if (!uri.normalize().equals(uri) || uri.getQuery() != null || uri.getFragment() != null
                || uri.getUserInfo() != null || uri.getPath() == null || uri.getPath().contains("\\")) {
            throw new IllegalArgumentException("Native warehouse and dataset locations must be normalized");
        }
        requireAuthority(uri, value);
        return uri;
    }

    private static void requireAuthority(URI uri, String value) {
        requireSupportedUri(uri, value);
        if ("file".equals(uri.getScheme()) && !Path.of(uri.getPath()).isAbsolute()) {
            throw new IllegalArgumentException("Native file warehouse requires an absolute file URI");
        }
        if (uri.getScheme() == null && !Path.of(value).isAbsolute()) {
            throw new IllegalArgumentException("Native local warehouse must be absolute");
        }
        if ("s3".equals(uri.getScheme()) && uri.getHost() == null) {
            throw new IllegalArgumentException("Native S3 warehouse requires a bucket");
        }
    }

    private static void requireSupportedUri(URI uri, String value) {
        requireDecodedPath(uri, value);
        if (uri.getScheme() != null && !"s3".equals(uri.getScheme()) && !"file".equals(uri.getScheme())) {
            throw new IllegalArgumentException("Native storage URI scheme is unsupported");
        }
        if ("file".equals(uri.getScheme()) && uri.getAuthority() != null) {
            throw new IllegalArgumentException("Native file URI must have no remote authority");
        }
    }

    private static void requireDecodedPath(URI uri, String value) {
        String raw = uri.getRawPath();
        if (value.length() > MAX_URI_LENGTH || raw == null || encodedSeparator(raw)
                || !Path.of(uri.getPath()).normalize().toString().equals(uri.getPath())) {
            throw new IllegalArgumentException("Native storage URI is oversized or contains encoded traversal");
        }
    }

    private static String storageScheme(URI uri) {
        return uri.getScheme() == null ? "file" : uri.getScheme();
    }

    private static boolean encodedSeparator(String path) {
        String lower = path.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("%2f") || lower.contains("%5c") || lower.contains("%00") || lower.contains("%25");
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank() || value.length() > MAX_PROPERTY_LENGTH) {
            throw new IllegalArgumentException("Missing or oversized Native storage property: " + key);
        }
        return value;
    }
    private static boolean bool(Map<String, String> values, String key) {
        String value = required(values, key);
        if (!"true".equals(value) && !"false".equals(value)) {
            throw new IllegalArgumentException("Invalid boolean: " + key);
        }
        return Boolean.parseBoolean(value);
    }
}
