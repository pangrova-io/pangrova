package org.pangrova.worker.jni.write.catalog;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.ObjectStoreExecutionBinding;
import org.pangrova.protocol.v2.TaskRequest;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeObjectStoreBindingsTest {
    private static final CatalogExecutionBinding CATALOG = CatalogExecutionBinding.newBuilder()
            .setCatalogName("lake").setCatalogRevision(3).setCatalogIncarnationId("deployment-a")
            .setConnectorProvider("lance").build();

    @Test void rawLocalNamesAndEncodedFileUrisResolveToTheSameScopedPath() {
        String root = "/data/warehouse 中文\"";
        String location = root + "/Orders.中文\"; select/tag/tag-release";
        var binding = NativeObjectStoreBindings.create(CATALOG, "tenant-a", Map.of("warehouse", root));
        NativeObjectStoreBindings.requireLocation(binding, location);
        NativeObjectStoreBindings.requireLocation(binding, java.nio.file.Path.of(location).toUri().toString());
        var uriBinding = NativeObjectStoreBindings.create(CATALOG, "tenant-a",
                Map.of("warehouse", java.nio.file.Path.of(root).toUri().toString()));
        NativeObjectStoreBindings.requireLocation(uriBinding, location);
        assertThrows(SecurityException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, root + "-other/table"));
        assertThrows(IllegalArgumentException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, root + "/../outside/tag"));
    }

    @Test void localWarehouseRequiresAnExactNormalizedDescendant() {
        var binding = NativeObjectStoreBindings.create(CATALOG, "tenant-a", Map.of("warehouse", "/data/foo"));
        NativeObjectStoreBindings.requireLocation(binding, "/data/foo/table.lance");
        assertFalse(binding.hasS3());
        assertThrows(SecurityException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, "/data/foobar/table.lance"));
        assertThrows(SecurityException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, "/data/foo"));
        assertThrows(IllegalArgumentException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, "/data/foo/../other/table.lance"));
        assertThrows(IllegalArgumentException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, "/data/foo/%2e%2e/table.lance"));
        assertThrows(IllegalArgumentException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, "file://remote/data/foo/table.lance"));
        NativeObjectStoreBindings.requireLocation(binding, "file:/data/foo/table.lance");
        NativeObjectStoreBindings.requireLocation(binding, "file:///data/foo/table.lance");
        var fileBinding = NativeObjectStoreBindings.create(CATALOG, "tenant-a", Map.of("warehouse", "file:/data/foo"));
        NativeObjectStoreBindings.requireLocation(fileBinding, "/data/foo/table.lance");
        assertThrows(IllegalArgumentException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, "file:/data/foo/%2fescape"));
    }

    @Test void s3SettingsComeOnlyFromResolvedCatalogAndFailClosed() {
        var properties = s3Properties();
        var binding = NativeObjectStoreBindings.create(CATALOG, "tenant-a", properties);
        NativeObjectStoreBindings.requireLocation(binding, "s3://bucket/warehouse/table.lance");
        assertEquals(CATALOG, binding.getCatalog());
        assertEquals("tenant-a", binding.getTenantId());
        assertEquals("http://minio.example.invalid:9000", binding.getS3().getEndpoint());
        assertEquals("catalog-access", binding.getS3().getAccessKey());
        assertEquals("catalog-secret", binding.getS3().getSecretKey());
        assertTrue(binding.getS3().getPathStyleAccess());
        assertFalse(binding.getS3().getTlsEnabled());
        assertThrows(SecurityException.class,
                () -> NativeObjectStoreBindings.requireLocation(binding, "s3://other/warehouse/table.lance"));
        var missing = s3Properties();
        missing.remove("native.storage.s3.secret-key");
        assertThrows(IllegalArgumentException.class,
                () -> NativeObjectStoreBindings.create(CATALOG, "tenant-a", missing));
        var inconsistent = s3Properties();
        inconsistent.put("native.storage.s3.tls-enabled", "true");
        assertThrows(IllegalArgumentException.class,
                () -> NativeObjectStoreBindings.create(CATALOG, "tenant-a", inconsistent));
    }

    @Test void networkTaskCannotSupplyWorkerLocalSecrets() {
        var request = TaskRequest.newBuilder().addWorkerLocalStorageBindings(
                ObjectStoreExecutionBinding.newBuilder().setTenantId("tenant-a")).build();
        assertThrows(SecurityException.class, () -> WorkerNativeStorageTasks.prepare(request, null));
        var ordinary = TaskRequest.newBuilder().build();
        assertEquals(ordinary, WorkerNativeStorageTasks.prepare(ordinary, null));
    }

    private static HashMap<String, String> s3Properties() {
        return new HashMap<>(Map.of("warehouse", "s3://bucket/warehouse",
                "native.storage.s3.endpoint", "http://minio.example.invalid:9000",
                "native.storage.s3.region", "test-region", "native.storage.s3.path-style-access", "true",
                "native.storage.s3.tls-enabled", "false", "native.storage.s3.access-key", "catalog-access",
                "native.storage.s3.secret-key", "catalog-secret"));
    }
}
