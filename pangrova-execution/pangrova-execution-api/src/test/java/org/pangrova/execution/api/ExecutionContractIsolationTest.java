package org.pangrova.execution.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.Preconditions;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Resolves every contract signature with only the API, Common and JDK available. */
class ExecutionContractIsolationTest {
    @Test
    void everyContractLinksWithoutCoreProtocolOrNativeImplementation() throws Exception {
        URL api = org.pangrova.execution.api.stage.TaskStage.class.getProtectionDomain().getCodeSource().getLocation();
        URL common = Preconditions.class.getProtectionDomain().getCodeSource().getLocation();
        try (var loader = new URLClassLoader(new URL[] {api, common}, ClassLoader.getPlatformClassLoader())) {
            for (String unavailable : List.of("org.pangrova.execution.binding.TaskRequestFactory",
                    "org.pangrova.protocol.v2.TaskRequest", "com.fasterxml.jackson.databind.ObjectMapper",
                    "org.apache.arrow.vector.VectorSchemaRoot")) {
                assertThatThrownBy(() -> loader.loadClass(unavailable)).isInstanceOf(ClassNotFoundException.class);
            }
            List<String> classes = contractClasses(Path.of(api.toURI()));
            assertThat(classes).contains(org.pangrova.execution.api.stage.TaskStage.class.getName(), org.pangrova.execution.api.deadline.StageTaskDeadline.class.getName());
            for (String name : classes) {
                Class<?> type = Class.forName(name, false, loader);
                type.getGenericSuperclass();
                type.getGenericInterfaces();
                type.getDeclaredAnnotations();
                for (var field : type.getDeclaredFields()) {
                    field.getGenericType();
                    field.getDeclaredAnnotations();
                }
                for (var constructor : type.getDeclaredConstructors()) {
                    constructor.getGenericParameterTypes();
                    constructor.getGenericExceptionTypes();
                    constructor.getParameterAnnotations();
                }
                for (var method : type.getDeclaredMethods()) {
                    method.getGenericReturnType();
                    method.getGenericParameterTypes();
                    method.getGenericExceptionTypes();
                    method.getParameterAnnotations();
                }
            }
        }
    }

    private static List<String> contractClasses(Path artifact) throws Exception {
        String prefix = "org/pangrova/execution/api/";
        if (Files.isDirectory(artifact)) {
            try (var entries = Files.walk(artifact.resolve(prefix))) {
                return entries.filter(Files::isRegularFile).map(artifact::relativize)
                        .map(Path::toString).filter(name -> name.endsWith(".class"))
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
