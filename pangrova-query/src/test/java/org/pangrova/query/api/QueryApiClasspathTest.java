package org.pangrova.query.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Experimental;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryApiClasspathTest {
    @Test
    void contractsLinkWithoutRuntimeOrCentralSpi() throws Exception {
        URL types = org.pangrova.query.api.enums.QueryState.class.getProtectionDomain().getCodeSource().getLocation();
        URL common = Experimental.class.getProtectionDomain().getCodeSource().getLocation();
        URL lineage = org.pangrova.governance.api.enums.ColumnDerivationKind.class.getProtectionDomain().getCodeSource().getLocation();
        URL connector = org.pangrova.connector.api.query.ConnectorQueryAuthority.class.getProtectionDomain().getCodeSource().getLocation();
        URL security = org.pangrova.security.api.identity.UserIdentity.class.getProtectionDomain().getCodeSource().getLocation();
        URL observability = org.pangrova.observability.api.tracing.ExecutionTraceContext.class.getProtectionDomain().getCodeSource().getLocation();
        Path classes = Path.of(types.toURI());
        try (var loader = new URLClassLoader(new URL[]{types, common, lineage, connector, security, observability,
                org.pangrova.execution.api.identity.QueryId.class.getProtectionDomain().getCodeSource().getLocation()}, ClassLoader.getPlatformClassLoader());
             var files = Files.walk(classes.resolve("org/pangrova/query/api"))) {
            List<Path> classFiles = files.filter(path -> path.toString().endsWith(".class")).toList();
            assertThat(classFiles).isNotEmpty();
            for (Path path : classFiles) {
                String relative = classes.relativize(path).toString().replace(path.getFileSystem().getSeparator(), ".");
                Class<?> type = Class.forName(relative.substring(0, relative.length() - 6), true, loader);
                assertThat(type.getClassLoader()).isSameAs(loader);
                type.getGenericSuperclass();
                type.getGenericInterfaces();
                type.getAnnotations();
                type.getRecordComponents();
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
                for (var field : type.getDeclaredFields()) {
                    field.getGenericType();
                }
            }
            for (String excluded : List.of("org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory",
                    "org.pangrova.client.http.PangrovaRestClient",
                    "org.pangrova.governance.runtime.lineage.LineageQueryEventListener",
                    "org.pangrova.connector.runtime.lifecycle.PluginManager",
                    "org.pangrova.worker.function.NativeFunctionCallbacks",
                    "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap", "org.apache.arrow.vector.VectorSchemaRoot", "com.fasterxml.jackson.databind.ObjectMapper",
                    "org.apache.calcite.rel.type.RelDataType", "org.postgresql.Driver")) {
                assertThatThrownBy(() -> loader.loadClass(excluded)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
