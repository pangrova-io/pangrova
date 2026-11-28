package org.pangrova.worker.shuffle;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.pangrova.common.validation.Preconditions;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.shuffle.api.ShuffleServerHandle;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.slf4j.Logger;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native command binding must link without Worker task orchestration or Coordinator code. */
class NativeShuffleProducerRegistrationIsolationTest {
    @Test
    void commandBindingLinksWithoutStartingTheWorkerOrLoadingNativeCode() throws Exception {
        URL[] urls = List.of(NativeShuffleProducerRegistration.class, Preconditions.class, RuntimeCommand.class,
                        ShuffleServerHandle.class, NativeCommandBuilder.class, Message.class, Logger.class)
                .stream().map(type -> type.getProtectionDomain().getCodeSource().getLocation())
                .distinct().toArray(URL[]::new);
        try (var loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            for (String excluded : List.of("org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.pangrova.shuffle.api.ShuffleTransport")) {
                assertThatThrownBy(() -> loader.loadClass(excluded)).isInstanceOf(ClassNotFoundException.class);
            }
            for (String name : List.of(NativeShuffleProducerRegistration.class.getName(),
                    NativeShuffleProducerRegistration.CommandChannel.class.getName())) {
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
