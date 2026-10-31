package org.pangrova.testing.integration.e2e.arch;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.pangrova.connector.spi.plugin.ConnectorFactory;
import org.pangrova.function.api.registry.FunctionFactory;

import java.util.Queue;
import java.util.ServiceLoader;
import java.util.Set;

/** Adds only providers discovered by a reachable production ServiceLoader site. */
final class ServiceLoaderReachability {

    private static final String NATIVE_FUNCTION_BOOTSTRAP =
            "org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap";
    private static final Set<String> CONNECTOR_LOADERS = Set.of(
            "org.pangrova.worker.nativebridge.jni.write.catalog.WorkerCatalogRuntime",
            "org.pangrova.coordinator.metadata.PluginConnectorHandleFactory",
            "org.pangrova.connector.runtime.lifecycle.PluginManager");

    private ServiceLoaderReachability() {}

    static void enqueueProviders(
            String current, JavaClasses importedClasses,
            Queue<String> queue, Set<String> visited) {
        if (NATIVE_FUNCTION_BOOTSTRAP.equals(current)
                && callsServiceLoader(importedClasses, current)) {
            enqueue(ServiceLoader.load(FunctionFactory.class), queue, visited);
        }
        if (CONNECTOR_LOADERS.contains(current)
                && callsServiceLoader(importedClasses, current)) {
            enqueue(ServiceLoader.load(ConnectorFactory.class), queue, visited);
        }
    }

    private static boolean callsServiceLoader(JavaClasses classes, String className) {
        return classes.stream()
                .filter(cls -> cls.getName().equals(className))
                .findFirst()
                .map(JavaClass::getMethodCallsFromSelf)
                .stream()
                .flatMap(Set::stream)
                .anyMatch(call -> call.getTarget().getOwner().getName()
                        .equals(ServiceLoader.class.getName())
                        && call.getTarget().getName().equals("load"));
    }

    private static <T> void enqueue(
            ServiceLoader<T> loader, Queue<String> queue, Set<String> visited) {
        loader.stream()
                .map(provider -> provider.type().getName())
                .filter(name -> name.startsWith("org.pangrova."))
                .filter(name -> !visited.contains(name))
                .forEach(queue::add);
    }
}
