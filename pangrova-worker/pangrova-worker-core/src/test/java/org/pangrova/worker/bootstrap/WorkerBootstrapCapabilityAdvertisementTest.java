package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.worker.nativebridge.jni.NativeFunctionConfig;
import org.pangrova.testing.testkit.function.ActualNativeCatalogue;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerBootstrapCapabilityAdvertisementTest {

    @Test
    void exposesOnlyTheCanonicalInventoryAndEngineIdentity() {
        assertThat(Arrays.stream(NativeCapabilityAdvertisement.Advertisement.class
                        .getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName))
                .containsExactly("inventory", "engineVersion");
    }

    @Test
    void resolvesPlanContractToTheUniqueNativeBuildIdentity() {
        CapabilityRef nativeRef = BuiltInCapabilityCatalog.aiTextGenerationPlanRef()
                .toBuilder()
                .setImplementationBuildId("native-build-a")
                .build();

        CapabilityInventory advertised = NativeCapabilityAdvertisement
                .addConfiguredModels(inventory(nativeRef), functionConfig());

        assertThat(advertised.getModelsList()).singleElement().satisfies(model -> {
            assertThat(model.getModel().getId()).isEqualTo("catalog-chat");
            assertThat(model.getCapabilityContractsList()).singleElement()
                    .satisfies(contract -> {
                        assertThat(contract.getCapability()).isEqualTo(nativeRef);
                        assertThat(contract.getOutputType().getCanonicalSql())
                                .isEqualTo("VARCHAR");
                    });
            assertThat(model.getLimits().getMaxProviderItemsPerTask()).isEqualTo(100);
        });
    }

    @Test
    void advertisesExactEmbeddingVectorDimension() {
        CapabilityRef nativeRef = BuiltInCapabilityCatalog.aiEmbeddingPlanRef()
                .toBuilder().setImplementationBuildId("native-build-a").build();

        CapabilityInventory advertised = NativeCapabilityAdvertisement
                .addConfiguredModels(inventory(nativeRef), embeddingFunctionConfig());

        assertThat(advertised.getModels(0).getCapabilityContracts(0)
                .getOutputType().getCanonicalSql()).isEqualTo("VECTOR(384)");
        assertThat(advertised.getModels(0).getCapabilityContracts(0)
                .getOutputType().getConstraints(0).getIntegerValue()).isEqualTo(384);
    }

    @Test
    void doesNotAdvertiseModelsWhenFunctionRegistrationIsDisabled() {
        CapabilityInventory advertised = NativeCapabilityAdvertisement.addConfiguredModels(
                inventory(BuiltInCapabilityCatalog.aiTextGenerationPlanRef()),
                new NativeFunctionConfig(false, functionConfig().pluginConfigs()));

        assertThat(advertised.getModelsList()).isEmpty();
    }

    @Test
    void rejectsMultimodalModelWhenFileRuntimeIsNotEnabled() {
        Map<String, String> model = new java.util.LinkedHashMap<>(
                functionConfig().configForPlugin("ai-inference"));
        model.put("models.chat.capabilities", "inference,multimodal");
        model.put("models.chat.modalities", "text,image");
        model.put("models.chat.max-media-bytes", "1048576");
        NativeFunctionConfig withoutFile = new NativeFunctionConfig(
                true, Map.of("ai-inference", Map.copyOf(model)));

        assertThatThrownBy(() -> NativeCapabilityAdvertisement.addConfiguredModels(
                inventory(BuiltInCapabilityCatalog.aiTextGenerationPlanRef()), withoutFile))
                .hasMessageContaining("no governed FILE content loader");
    }

    @Test
    void failsClosedWhenNativeHasNoOrMultipleMatchingBuilds() {
        CapabilityRef required = BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
        assertThatThrownBy(() -> NativeCapabilityAdvertisement.addConfiguredModels(
                CapabilityInventory.getDefaultInstance(), functionConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advertised 0 matching implementations");

        CapabilityRef first = required.toBuilder()
                .setImplementationBuildId("native-build-a")
                .build();
        CapabilityRef second = required.toBuilder()
                .setImplementationBuildId("native-build-b")
                .build();
        CapabilityInventory ambiguous = actualInventory()
                .addCapabilities(CapabilityDescriptor.newBuilder().setRef(first))
                .addCapabilities(CapabilityDescriptor.newBuilder().setRef(second))
                .build();
        assertThatThrownBy(() -> NativeCapabilityAdvertisement.addConfiguredModels(
                ambiguous, functionConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advertised 2 matching implementations");
    }

    @Test
    void refusesToAdvertiseUnsupportedChatCompletionDocumentModality() {
        CapabilityRef text = BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
        CapabilityRef multimodal = BuiltInCapabilityCatalog
                .aiMultimodalGenerationPlanRef();
        CapabilityInventory nativeInventory = actualInventory()
                .addCapabilities(CapabilityDescriptor.newBuilder().setRef(text))
                .addCapabilities(CapabilityDescriptor.newBuilder().setRef(multimodal))
                .build();

        assertThatThrownBy(() -> NativeCapabilityAdvertisement.addConfiguredModels(
                nativeInventory, multimodalFunctionConfig()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("supports only TEXT and IMAGE");
    }

    private static CapabilityInventory inventory(CapabilityRef capability) {
        return actualInventory()
                .addCapabilities(CapabilityDescriptor.newBuilder().setRef(capability))
                .build();
    }

    private static CapabilityInventory.Builder actualInventory() {
        try {
            return CapabilityInventory.parseFrom(ActualNativeCatalogue.bytes()).toBuilder()
                    .clearCapabilities().clearModels();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Actual Native catalogue exporter artifact is malformed", failure);
        }
    }

    private static NativeFunctionConfig functionConfig() {
        Map<String, String> model = Map.ofEntries(
                Map.entry("models.chat.name", "catalog-chat"),
                Map.entry("models.chat.provider", "openai-compatible"),
                Map.entry("models.chat.provider-model", "chat-deployment-v1"),
                Map.entry("models.chat.version", "v7"),
                Map.entry("models.chat.endpoint",
                        "https://models.example/v1/chat/completions"),
                Map.entry("models.chat.secret-reference", "env://CHAT_KEY"),
                Map.entry("models.chat.capabilities", "inference"),
                Map.entry("models.chat.max-batch-size", "16"),
                Map.entry("models.chat.max-input-tokens", "8192"),
                Map.entry("models.chat.max-output-tokens", "1024"),
                Map.entry("models.chat.max-concurrency", "4"),
                Map.entry("models.chat.max-provider-attempts-per-task", "400"),
                Map.entry("models.chat.max-request-bytes", "262144"),
                Map.entry("models.chat.max-response-bytes", "1048576"),
                Map.entry("models.chat.max-provider-items-per-task", "100"));
        return new NativeFunctionConfig(
                true, Map.of("ai-inference", model));
    }

    private static NativeFunctionConfig multimodalFunctionConfig() {
        Map<String, String> model = new java.util.LinkedHashMap<>(
                functionConfig().configForPlugin("ai-inference"));
        model.put("models.chat.capabilities", "inference,multimodal");
        model.put("models.chat.modalities", "text,image,document");
        model.put("models.chat.max-media-bytes", "1048576");
        return new NativeFunctionConfig(
                true, Map.of("ai-inference", Map.copyOf(model)));
    }

    private static NativeFunctionConfig embeddingFunctionConfig() {
        Map<String, String> model = new java.util.LinkedHashMap<>(
                functionConfig().configForPlugin("ai-inference"));
        model.put("models.chat.capabilities", "embedding");
        model.put("models.chat.endpoint", "https://models.example/v1/embeddings");
        model.put("models.chat.embedding-dimension", "384");
        model.put("models.chat.max-output-tokens", "0");
        return new NativeFunctionConfig(
                true, Map.of("ai-inference", Map.copyOf(model)));
    }
}
