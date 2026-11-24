package org.pangrova.shuffle.pangrova.client;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The actual remote implementation links on its own Maven closure, without a Worker or JNI. */
class PangrovaShuffleProviderIsolationTest {
    @Test
    void providerLinksWithoutCoreWorkerNativePlannerOrCoordinatorImplementations() throws Exception {
        ClassLoader loader = PangrovaShuffleClient.class.getClassLoader();
        for (String excluded : List.of("org.pangrova.shuffle.exchange.ExchangeTransport",
                "org.pangrova.shuffle.exchange.SpoolBackedExchangeManager",
                "org.pangrova.shuffle.exchange.enums.PartitionMergeStrategy",
                "org.pangrova.shuffle.exchange.ExchangeTaskAttempt",
                "org.pangrova.worker.execution.NativeTaskExecutor",
                "org.pangrova.worker.jni.NativeRuntime",
                "org.pangrova.worker.bootstrap.WorkerBootstrap",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.planner.analysis.PlannerContext",
                "org.pangrova.worker.exchange.remote.RemoteShuffleClient",
                "org.pangrova.worker.exchange.codec.CanonicalBatchChunks")) {
            assertThatThrownBy(() -> loader.loadClass(excluded)).isInstanceOf(ClassNotFoundException.class);
        }
        Path artifact = Path.of(PangrovaShuffleClient.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> classes = implementationClasses(artifact);
        assertThat(classes).contains(PangrovaShuffleClient.class.getName(), RemoteShuffleByteEgress.class.getName(),
                RemoteShuffleExchangeSource.class.getName(), RemoteShuffleClientPool.class.getName());
        for (String name : classes) {
            Class<?> type = Class.forName(name, false, loader);
            type.getGenericInterfaces();
            type.getGenericSuperclass();
            for (var constructor : type.getDeclaredConstructors()) constructor.getGenericParameterTypes();
            for (var field : type.getDeclaredFields()) field.getGenericType();
            for (var method : type.getDeclaredMethods()) {
                method.getGenericReturnType();
                method.getGenericParameterTypes();
            }
        }
    }

    private static List<String> implementationClasses(Path artifact) throws Exception {
        String prefix = "org/pangrova/shuffle/pangrova/";
        if (Files.isDirectory(artifact)) {
            try (var entries = Files.walk(artifact.resolve(prefix))) {
                return entries.filter(Files::isRegularFile).map(artifact::relativize).map(Path::toString)
                        .filter(name -> name.endsWith(".class"))
                        .map(name -> name.substring(0, name.length() - 6).replace('/', '.')).toList();
            }
        }
        try (var jar = new JarFile(artifact.toFile())) {
            return jar.stream().map(entry -> entry.getName())
                    .filter(name -> name.startsWith(prefix) && name.endsWith(".class"))
                    .map(name -> name.substring(0, name.length() - 6).replace('/', '.')).toList();
        }
    }
}
