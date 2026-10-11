package org.pangrova.worker.jni;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.TestShuffleServiceConfig;
import org.pangrova.worker.bootstrap.WorkerBootstrapConfig;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.phase.NativeRuntimeBootstrap;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks that the packaged Native callback generation requires the typed AI failure ABI. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class NativeAiFailureProjectionRegistrationTest {

    @Test
    void packagedNativeRegistersTheCurrentCallbackGeneration() {
        var refs = new WorkerComponentRefs();
        NativeRuntimeBootstrap.initialize(
                TestShuffleServiceConfig.config(
                        WorkerBootstrapConfig.withDefaults("ai-failure-projection-registration")),
                refs);
        try {
            var config = new NativeFunctionConfig(true, Map.of("ai-inference", Map.ofEntries(
                    Map.entry("models.chat.provider", "openai-compatible"),
                    Map.entry("models.chat.provider-model", "chat-deployment-v1"),
                    Map.entry("models.chat.version", "v1"),
                    Map.entry("models.chat.endpoint", "https://models.example/v1/chat/completions"),
                    Map.entry("models.chat.secret-reference", "env://CHAT_KEY"),
                    Map.entry("models.chat.capabilities", "inference"),
                    Map.entry("models.chat.max-input-tokens", "8192"),
                    Map.entry("models.chat.max-output-tokens", "1024"),
                    Map.entry("models.chat.max-concurrency", "4"),
                    Map.entry("models.chat.max-provider-attempts-per-task", "400"),
                    Map.entry("models.chat.max-request-bytes", "262144"),
                    Map.entry("models.chat.max-response-bytes", "1048576"),
                    Map.entry("models.chat.max-provider-items-per-task", "100"))));
            NativeFunctionBootstrap.initialize(refs, config);
            assertThat(refs.nativeFunctionBridge().scalarCount()).isEqualTo(14);
            assertThat(NativeRuntime.isInitialized()).isTrue();
        } finally {
            try {
                NativeFunctionBootstrap.close(refs);
            } finally {
                NativeRuntime.shutdownNativeRuntime();
            }
        }
    }
}
