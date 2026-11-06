package org.pangrova.filesystem.api;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilesystemApiIsolationTest {
    @Test
    void actualClasspathAndAllApiTypesLoadWithoutSecurityRuntimeJdbcOrCloudProviders() throws Exception {
        var forbidden = List.of("org.pangrova.observability.noop.NoOpMetricsProvider",
                "com.fasterxml.jackson.databind.ObjectMapper", "org.postgresql.Driver",
                "com.zaxxer.hikari.HikariDataSource", "org.pangrova.security.api.identity.UserIdentity",
                "org.pangrova.persistence.security.api.policy.RoleStore",
                "org.pangrova.file.core.access.FileSpaceAccessService",
                "org.pangrova.protocol.execution.AiTaskLifecycle",
                "org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry",
                "software.amazon.awssdk.services.s3.S3Client", "com.aliyun.sdk.service.oss2.OSSClient",
                "org.pangrova.worker.file.ObjectStoreAccessorFactory",
                "org.pangrova.filesystem.s3.object.S3ObjectStoreAccessorProvider",
                "org.pangrova.filesystem.local.object.LocalObjectStoreAccessorProvider");
        for (String name : forbidden) {
            assertThatThrownBy(() -> Class.forName(name)).isInstanceOf(ClassNotFoundException.class);
        }
        URL api = ObjectStoreAccessor.class.getProtectionDomain().getCodeSource().getLocation();
        URL common = Experimental.class.getProtectionDomain().getCodeSource().getLocation();
        Path classes = Path.of(api.toURI());
        try (var loader = new URLClassLoader(new URL[]{api, common}, ClassLoader.getPlatformClassLoader());
             var files = Files.walk(classes.resolve("org/pangrova/filesystem/api"))) {
            var names = files.filter(path -> path.toString().endsWith(".class"))
                    .filter(path -> !path.getFileName().toString().equals("package-info.class"))
                    .map(path -> classes.relativize(path).toString().replace(java.io.File.separatorChar, '.')
                            .replaceFirst("\\.class$", "")).toList();
            assertThat(names).isNotEmpty();
            for (String name : names) {
                Class<?> type = loader.loadClass(name);
                assertThat(type.getClassLoader()).isSameAs(loader);
                type.getDeclaredMethods();
                type.getDeclaredConstructors();
                type.getRecordComponents();
                type.getPermittedSubclasses();
                type.getAnnotations();
            }
            for (String name : forbidden) {
                assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
