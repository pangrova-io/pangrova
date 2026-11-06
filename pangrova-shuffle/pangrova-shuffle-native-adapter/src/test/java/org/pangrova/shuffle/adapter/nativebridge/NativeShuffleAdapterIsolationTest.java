package org.pangrova.shuffle.adapter.nativebridge;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.pangrova.common.util.Preconditions;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.shuffle.api.ShuffleServerHandle;
import org.pangrova.worker.nativebridge.jni.NativeCommandBuilder;
import org.slf4j.Logger;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native command binding must link without Worker task orchestration or Coordinator code. */
class NativeShuffleAdapterIsolationTest {
    @Test
    void adapterLinksOnItsOwnDependencyClosureWithoutLoadingNativeRuntime() throws Exception {
        URL[] urls = List.of(NativeShuffleTransport.class, Preconditions.class, RuntimeCommand.class,
                        ShuffleServerHandle.class, NativeCommandBuilder.class, Message.class, Logger.class)
                .stream().map(type -> type.getProtectionDomain().getCodeSource().getLocation())
                .distinct().toArray(URL[]::new);
        try (var loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            for (String excluded : List.of("org.pangrova.worker.execution.NativeTaskExecutor",
                    "org.pangrova.worker.execution.TaskExecutor", "org.pangrova.worker.bootstrap.WorkerBootstrap",
                    "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.pangrova.shuffle.api.ShuffleTransport")) {
                assertThatThrownBy(() -> loader.loadClass(excluded)).isInstanceOf(ClassNotFoundException.class);
            }
            for (String name : List.of(NativeShuffleTransport.class.getName(),
                    NativeShuffleTransport.CommandChannel.class.getName())) {
                Class<?> type = Class.forName(name, true, loader);
                assertThat(type.getClassLoader()).isSameAs(loader);
                type.getGenericInterfaces();
                for (var field : type.getDeclaredFields()) field.getGenericType();
                for (var constructor : type.getDeclaredConstructors()) constructor.getGenericParameterTypes();
                for (var method : type.getDeclaredMethods()) {
                    method.getGenericReturnType();
                    method.getGenericParameterTypes();
                }
            }
        }
    }
}
