package org.pangrova.metadata.catalog.federation;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.catalog.FederatedCatalogFactory;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FederationProviderDiscoveryTest {
    @Test
    void duplicateIdsFailDuringDiscoveryBeforeAnyCatalogIsOpened(@TempDir Path root) throws Exception {
        Path descriptor = root.resolve("META-INF/services/" + FederatedCatalogFactory.class.getName());
        Files.createDirectories(descriptor.getParent());
        Files.writeString(descriptor, First.class.getName() + "\n" + Second.class.getName() + "\n");
        try (var loader = new URLClassLoader(new URL[]{root.toUri().toURL()}, getClass().getClassLoader())) {
            assertThatThrownBy(() -> new PluginFederationHandleFactory(loader, ref -> ref.key()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Duplicate federated catalog provider 'warehouse'")
                    .hasMessageContaining(First.class.getName()).hasMessageContaining(Second.class.getName());
        }
    }

    public static class First implements FederatedCatalogFactory {
        @Override
        public String name() {
            return "warehouse";
        }

        @Override
        public FederatedCatalog create(Map<String, String> properties) {
            throw new AssertionError("Discovery opened a catalog before validating the factory inventory");
        }
    }

    public static class Second extends First {}
}
