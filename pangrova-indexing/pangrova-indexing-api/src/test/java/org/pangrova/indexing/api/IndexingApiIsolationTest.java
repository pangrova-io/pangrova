package org.pangrova.indexing.api;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexingApiIsolationTest {
    @Test
    void actualClasspathAndAllApiTypesLoadWithoutSecurityRuntimeJdbcOrCloudProviders() throws Exception {
        var forbidden = List.of("org.pangrova.indexing.core.descriptor.IndexDescriptorTransitions",
                "org.pangrova.indexing.builtin.bloom.provider.BloomFilterStrategyProvider",
                "org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore",
                "org.apache.calcite.rel.RelNode", "org.roaringbitmap.RoaringBitmap",
                "com.github.benmanes.caffeine.cache.Caffeine", "org.pangrova.observability.noop.NoOpMetricsProvider",
                "com.fasterxml.jackson.databind.ObjectMapper", "org.postgresql.Driver",
                "com.zaxxer.hikari.HikariDataSource", "org.pangrova.persistence.security.api.policy.RoleStore",
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
        URL api = IndexDescriptor.class.getProtectionDomain().getCodeSource().getLocation();
        URL filesystem = ObjectStoreAccessor.class.getProtectionDomain().getCodeSource().getLocation();
        URL logging = LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation();
        URL common = Experimental.class.getProtectionDomain().getCodeSource().getLocation();
        URL types = org.pangrova.types.Type.class.getProtectionDomain().getCodeSource().getLocation();
        URL batch = org.pangrova.batch.api.page.Page.class.getProtectionDomain().getCodeSource().getLocation();
        URL query = org.pangrova.query.api.lifecycle.QueryState.class.getProtectionDomain().getCodeSource().getLocation();
        Path classes = Path.of(api.toURI());
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        for (String artifact : List.of("pangrova-spi", "pangrova-indexing-core", "pangrova-indexing-codec", "pangrova-indexing-strategies-builtin", "pangrova-indexing-planner-adapter",
                "pangrova-persistence-generation-api", "pangrova-persistence-generation-postgresql", "pangrova-worker-core", "pangrova-planner-core")) {
            assertThat(classpath).doesNotContain(artifact);
            assertThat(java.util.Collections.list(getClass().getClassLoader().getResources(
                    "META-INF/maven/org.pangrova/" + artifact + "/pom.properties"))).isEmpty();
        }
        try (var loader = new URLClassLoader(new URL[]{api, common, types, batch, filesystem, logging, query,
                org.pangrova.connector.api.commit.WriteDeclarationSnapshot.class.getProtectionDomain().getCodeSource().getLocation()}, ClassLoader.getPlatformClassLoader());
             var files = Files.walk(classes.resolve("org/pangrova/indexing"))) {
            var names = files.filter(path -> path.toString().endsWith(".class"))
                    .filter(path -> !path.getFileName().toString().equals("package-info.class"))
                    .map(path -> classes.relativize(path).toString().replace(java.io.File.separatorChar, '.')
                            .replaceFirst("\\.class$", "")).toList();
            assertThat(names).isNotEmpty();
            for (String name : names) {
                Class<?> type = loader.loadClass(name);
                assertThat(type.getClassLoader()).isSameAs(loader);
                type.getGenericSuperclass();
                type.getGenericInterfaces();
                for (var method : type.getDeclaredMethods()) {
                    method.getGenericReturnType();
                    method.getGenericParameterTypes();
                    method.getGenericExceptionTypes();
                    method.getAnnotations();
                }
                for (var constructor : type.getDeclaredConstructors()) {
                    constructor.getGenericParameterTypes();
                    constructor.getGenericExceptionTypes();
                }
                for (var field : type.getDeclaredFields()) field.getGenericType();
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
