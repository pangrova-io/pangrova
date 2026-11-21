package org.pangrova.worker.bootstrap;

import org.pangrova.ai.api.enums.AiOperation;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.capability.inventory.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.ai.api.model.AiModelDescriptor;
import org.pangrova.ai.api.model.AiModelIdentity;
import org.pangrova.ai.api.embedding.EmbeddingModelBindingIdentity;
import org.pangrova.ai.api.model.AiModelCatalog;
import org.pangrova.worker.jni.NativeCapabilityLoader;
import org.pangrova.worker.function.NativeFunctionConfig;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Builds the exact Worker capability advertisement after Native functions have
 * been registered successfully.
 *
 * <p>This is the sole conversion from the Native inventory and configured AI
 * model descriptors into the registration payload consumed by the Coordinator.
 * Keeping it outside the Worker process orchestrator lets production-style
 * integration harnesses reuse the same fail-closed identity resolution without
 * duplicating protocol construction.</p>
 */
public final class NativeCapabilityAdvertisement {

    private NativeCapabilityAdvertisement() {}

    /** Loads the canonical Native inventory and enriches it with configured models. */
    public static Advertisement load(NativeFunctionConfig functionConfig) {
        return create(
                NativeCapabilityLoader.loadInventory(),
                Objects.requireNonNull(functionConfig, "functionConfig must not be null"));
    }

    static Advertisement create(
            NativeCapabilityLoader.ParsedCapabilities parsed,
            NativeFunctionConfig functionConfig) {
        Objects.requireNonNull(parsed, "parsed must not be null");
        Objects.requireNonNull(functionConfig, "functionConfig must not be null");

        CapabilityInventory inventory = addConfiguredModels(
                parsed.inventory(), functionConfig);
        CapabilityInventoryCodec.validate(inventory);
        return new Advertisement(inventory, parsed.engineVersion());
    }

    static CapabilityInventory addConfiguredModels(
            CapabilityInventory nativeInventory,
            NativeFunctionConfig functionConfig) {
        return addConfiguredModels(nativeInventory, functionConfig,
                ServiceLoader.load(AiModelCatalog.class, NativeCapabilityAdvertisement.class.getClassLoader()));
    }

    static CapabilityInventory addConfiguredModels(
            CapabilityInventory nativeInventory,
            NativeFunctionConfig functionConfig,
            Iterable<AiModelCatalog> catalogs) {
        List<CapabilityRef> nativeCapabilities = nativeInventory
                .getCapabilitiesList().stream()
                .map(org.pangrova.protocol.v2.CapabilityDescriptor::getRef)
                .toList();
        var result = nativeInventory.toBuilder().clearModels();
        if (!functionConfig.autoRegister()) {
            return result.build();
        }
        var aiConfig = functionConfig.configForPlugin("ai-inference");
        var candidates = catalogs.iterator();
        if (!candidates.hasNext()) {
            if (aiConfig.keySet().stream().anyMatch(key -> key.startsWith("models."))) {
                throw new IllegalStateException("Configured AI models require an AiModelCatalog provider");
            }
            return result.build();
        }
        AiModelCatalog catalog = Objects.requireNonNull(candidates.next(), "AI model catalog");
        if (candidates.hasNext()) {
            throw new IllegalStateException("Multiple AiModelCatalog providers cannot own model advertisement");
        }
        catalog.describeModels(aiConfig).stream()
                .sorted(Comparator
                        .comparing(AiModelDescriptor::name)
                        .thenComparing(AiModelDescriptor::version))
                .forEach(model -> result.addModels(modelAvailability(
                        model, nativeCapabilities)));
        return result.build();
    }

    private static ModelAvailability modelAvailability(
            AiModelDescriptor model, List<CapabilityRef> nativeCapabilities) {
        List<ModelCapabilityContract> capabilities = model.capabilities().stream()
                .map(capability -> {
                    CapabilityRef required = switch (capability.operation()) {
                        case TEXT_GENERATION ->
                                BuiltInCapabilityCatalog.aiTextGenerationPlanRef();
                        case MULTIMODAL_GENERATION ->
                                BuiltInCapabilityCatalog.aiMultimodalGenerationPlanRef();
                        case EMBEDDING ->
                                BuiltInCapabilityCatalog.aiEmbeddingPlanRef();
                        case RERANKING ->
                                BuiltInCapabilityCatalog.crossEncoderRerankPlanRef();
                    };
                    ModelCapabilityContract.Builder contract =
                            ModelCapabilityContract.newBuilder()
                            .setCapability(resolveNativeCapability(
                                    model.name(), required, nativeCapabilities))
                            .setOutputType(outputType(model, capability.operation()));
                    if (capability.operation()
                            == AiOperation.EMBEDDING) {
                        contract.setBindingContractSha256(ByteString.copyFrom(
                                EmbeddingModelBindingIdentity.sha256(model)));
                    } else {
                        contract.setBindingContractSha256(ByteString.copyFrom(
                                AiModelIdentity.signatureSha256(model)));
                    }
                    return contract.build();
                })
                .distinct()
                .toList();

        return ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder()
                        .setId(model.name())
                        .setVersion(model.version())
                        .setSignatureSha256(ByteString.copyFrom(
                                AiModelIdentity.signatureSha256(model))))
                .setLimits(modelLimits(model))
                .addAllCapabilityContracts(capabilities)
                .build();
    }

    private static org.pangrova.protocol.v2.CapabilityLimits modelLimits(AiModelDescriptor model) {
        var limits = model.capabilities().iterator().next().resourceLimits();
        if (model.capabilities().stream().anyMatch(capability -> !limits.equals(capability.resourceLimits()))) {
            throw new IllegalArgumentException("Model availability cannot merge different exact resource envelopes");
        }
        var request = limits.requestConstraints();
        return org.pangrova.protocol.v2.CapabilityLimits.newBuilder()
                .setMaxBatchItems(integerProperty(model, "max-batch-size"))
                .setMaxInputTokens(request.maxInputTokens()).setMaxOutputTokens(request.maxOutputTokens())
                .setMaxRequestBytes(request.maxRequestBytes()).setMaxResponseBytes(request.maxResponseBytes())
                .setMaxMediaBytes(limits.maxMediaBytes()).setMaxConcurrency(limits.maxConcurrency())
                .setMaxProviderItemsPerTask(limits.maxProviderItemsPerTask())
                .setMaxProviderAttemptsPerTask(limits.maxProviderAttemptsPerTask()).build();
    }

    private static LogicalType outputType(
            AiModelDescriptor model, AiOperation operation) {
        return switch (operation) {
            case EMBEDDING -> VectorLogicalType.create(
                    integerProperty(model, "embedding-dimension"));
            case RERANKING -> scalarOutput("DOUBLE", io.substrait.proto.Type
                    .newBuilder().setFp64(io.substrait.proto.Type.FP64.newBuilder()
                            .setNullability(io.substrait.proto.Type.Nullability
                                    .NULLABILITY_NULLABLE)).build());
            case TEXT_GENERATION, MULTIMODAL_GENERATION -> scalarOutput(
                    "VARCHAR", io.substrait.proto.Type.newBuilder()
                            .setString(io.substrait.proto.Type.String.newBuilder()
                                    .setNullability(io.substrait.proto.Type.Nullability
                                            .NULLABILITY_NULLABLE)).build());
        };
    }

    private static LogicalType scalarOutput(
            String canonicalSql, io.substrait.proto.Type physicalType) {
        return LogicalType.newBuilder()
                .setCanonicalSql(canonicalSql)
                .setSubstraitType(physicalType)
                .build();
    }

    private static int integerProperty(AiModelDescriptor model, String property) {
        return Integer.parseInt(model.properties().get(property));
    }

    private static long longProperty(AiModelDescriptor model, String property) {
        return Long.parseLong(model.properties().get(property));
    }

    private static CapabilityRef resolveNativeCapability(
            String modelName,
            CapabilityRef required,
            List<CapabilityRef> nativeCapabilities) {
        List<CapabilityRef> matches = nativeCapabilities.stream()
                .filter(advertised -> advertised.getCapabilityId()
                        .equals(required.getCapabilityId()))
                .filter(advertised -> advertised.getSemanticVersion()
                        .equals(required.getSemanticVersion()))
                .filter(advertised -> advertised.getSignatureSha256()
                        .equals(required.getSignatureSha256()))
                .filter(advertised -> required.getImplementationBuildId().isBlank()
                        || advertised.getImplementationBuildId()
                                .equals(required.getImplementationBuildId()))
                .toList();
        if (matches.size() != 1) {
            throw new IllegalStateException(
                    "Configured AI model '" + modelName + "' requires capability "
                            + required.getCapabilityId() + "@"
                            + required.getSemanticVersion()
                            + " but Native advertised " + matches.size()
                            + " matching implementations");
        }
        return matches.get(0);
    }

    /** Immutable Worker registration view of the negotiated Native runtime. */
    public record Advertisement(
            CapabilityInventory inventory,
            String engineVersion) {

        public Advertisement {
            Objects.requireNonNull(inventory, "inventory must not be null");
            if (engineVersion == null || engineVersion.isBlank()) {
                throw new IllegalArgumentException("engineVersion must not be blank");
            }
        }

        /** Canonical Base64 payload used by the Worker registration envelope. */
        public String encodedInventory() {
            return CapabilityInventoryCodec.encode(inventory);
        }
    }
}
