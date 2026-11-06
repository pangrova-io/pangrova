package org.pangrova.query.core.lifecycle;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The actual lifecycle owner links and transitions without Coordinator or execution providers. */
class QueryLifecycleIsolationTest {
    @Test
    void lifecycleLinksOnContractClasspathAndOwnsItsState() throws Exception {
        URL[] urls = {
                location(QueryStateMachine.class),
                location(org.pangrova.query.api.lifecycle.QueryState.class),
                location(org.pangrova.connector.api.query.ConnectorQueryAuthority.class),
                location(org.pangrova.common.api.annotation.Internal.class),
                location(org.pangrova.transport.api.result.ResultEndpoint.class),
                location(org.pangrova.security.api.identity.UserIdentity.class),
                location(org.pangrova.observability.api.tracing.ExecutionTraceContext.class),
                location(org.slf4j.Logger.class)
        };
        try (var loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of(
                    "org.pangrova.query.core.lifecycle.QueryStateMachine",
                    "org.pangrova.query.core.lifecycle.QueryConnectorAuthority",
                    "org.pangrova.query.core.lifecycle.QueryTerminalCleanup",
                    "org.pangrova.query.core.lifecycle.QueryTerminalCleanup$Context",
                    "org.pangrova.query.core.retention.QueryRetentionScheduler")) {
                Class<?> type = Class.forName(name, true, loader);
                assertThat(type.getClassLoader()).isSameAs(loader);
                type.getDeclaredConstructors();
                type.getDeclaredMethods();
                type.getDeclaredFields();
                type.getAnnotations();
                type.getRecordComponents();
            }
            var stateType = loader.loadClass("org.pangrova.query.core.lifecycle.QueryStateMachine");
            Object state = stateType.getConstructor().newInstance();
            Runnable finish = (Runnable) stateType.getMethod("toFinisher").invoke(state);
            finish.run();
            assertThat(stateType.getMethod("getState").invoke(state).toString()).isEqualTo("FINISHED");
            assertThat(stateType.getMethod("physicalCleanupApplied").invoke(state)).isEqualTo(false);
            for (String forbidden : List.of(
                    "org.pangrova.coordinator.query.QueryManager",
                    "org.pangrova.coordinator.query.QueryStateMachine",
                    "org.pangrova.connector.runtime.lifecycle.PluginManager",
                    "org.pangrova.worker.nativebridge.jni.NativeFunctionBridge",
                    "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.postgresql.Driver", "io.opentelemetry.api.trace.Span",
                    "org.apache.calcite.rel.RelNode", "org.apache.arrow.vector.VectorSchemaRoot")) {
                assertThatThrownBy(() -> loader.loadClass(forbidden)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }

    private static URL location(Class<?> type) {
        return type.getProtectionDomain().getCodeSource().getLocation();
    }
}
