package org.pangrova.protocol;

import org.pangrova.protocol.descriptor.CanonicalProtocolDescriptors;
import org.pangrova.protocol.function.catalog.FunctionCatalogContracts;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityKind;
import org.pangrova.protocol.v2.ExecutionPlacement;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.type.VectorLogicalType;

import java.util.Base64;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.security.MessageDigest;

/**
 * Encodes and validates the canonical v2 Native capability inventory carried by
 * the Worker registration control message.
 *
 * <p>The JSON/HTTP registration endpoint is only a transport envelope. This
 * class deliberately preserves the protobuf payload byte-for-byte so that the
 * Coordinator schedules against the same signed capability and artifact-format
 * identities that Native advertised to the Worker during its startup handshake.
 */
public final class CapabilityInventoryCodec {

    /** Inclusive bound for the canonical Base64 inventory, before its transport envelope. */
    public static final int MAX_ENCODED_BYTES = 4 * 1024 * 1024;
    private static final String RELATION_ENVELOPE_TYPE_URL =
            "type.googleapis.com/pangrova.protocol.v2.RelationDetailEnvelope";

    private CapabilityInventoryCodec() {}

    /** Returns a standard Base64 representation suitable for JSON and metadata storage. */
    public static String encode(CapabilityInventory inventory) {
        validate(inventory);
        return Base64.getEncoder().encodeToString(inventory.toByteArray());
    }

    /** Decodes and fail-closed validates a canonical inventory. */
    public static CapabilityInventory decode(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalArgumentException("capabilityInventory must not be blank");
        }
        if (encoded.length() > MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("capabilityInventory exceeds maximum encoded size");
        }
        final byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("capabilityInventory is not valid Base64", failure);
        }
        final CapabilityInventory inventory;
        try {
            inventory = CapabilityInventory.parseFrom(bytes);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "capabilityInventory is not a canonical v2 CapabilityInventory", failure);
        }
        validate(inventory);
        return inventory;
    }

    /** Validates protocol identity and every schedulable capability descriptor. */
    public static void validate(CapabilityInventory inventory) {
        Objects.requireNonNull(inventory, "inventory must not be null");
        long encodedBytes = 4L * ((inventory.getSerializedSize() + 2L) / 3L);
        if (encodedBytes > MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("capabilityInventory exceeds maximum encoded size");
        }
        validateProtocol(inventory);
        validateEnvelopeVersions(inventory);
        CapabilityValidation capabilities = validateCapabilities(inventory);
        validateRetrievalContract(inventory, capabilities.hasRetrieval());
        validateModels(inventory, capabilities.refs());
        FunctionCatalogContracts.validate(inventory);
    }

    private static void validateProtocol(CapabilityInventory inventory) {
        var protocol = inventory.getProtocol();
        if (protocol.getMajor() != 2 || protocol.getMinor() != 0) {
            throw new IllegalArgumentException(
                    "Unsupported capability protocol: "
                            + protocol.getMajor() + "." + protocol.getMinor());
        }
        if (protocol.getRolloutEpoch().isBlank()) {
            throw new IllegalArgumentException("Capability inventory has no rollout epoch");
        }
        byte[] actualDescriptorHash = protocol.getDescriptorSetSha256().toByteArray();
        if (actualDescriptorHash.length != 32
                || !MessageDigest.isEqual(
                        CanonicalProtocolDescriptors.sha256(), actualDescriptorHash)) {
            throw new IllegalArgumentException(
                    "Capability inventory is not bound to the canonical descriptor set");
        }
    }

    private static void validateEnvelopeVersions(CapabilityInventory inventory) {
        if (inventory.getResultEnvelopeVersion() == 0) {
            throw new IllegalArgumentException("Capability inventory has no result envelope version");
        }
        if (inventory.getNativeEngineVersion().isBlank()) {
            throw new IllegalArgumentException("Capability inventory has no Native engine version");
        }
    }

    private static CapabilityValidation validateCapabilities(CapabilityInventory inventory) {
        Set<String> identities = new HashSet<>();
        Set<String> capabilityRefs = new HashSet<>();
        boolean hasRetrievalCapability = false;
        for (CapabilityDescriptor descriptor : inventory.getCapabilitiesList()) {
            String identity = validateDescriptor(descriptor);
            if (!identities.add(identity)) {
                throw new IllegalArgumentException(
                        "Capability inventory has a duplicate capability identity: " + identity);
            }
            capabilityRefs.add(refIdentity(descriptor.getRef()));
            if (isRetrieval(descriptor.getKind())) {
                hasRetrievalCapability = true;
                if (descriptor.getLimits().getMaxCandidateRows() == 0) {
                    throw new IllegalArgumentException(
                            "Native retrieval capability has no candidate bound: " + identity);
                }
            }
        }
        return new CapabilityValidation(Set.copyOf(capabilityRefs), hasRetrievalCapability);
    }

    private static void validateRetrievalContract(
            CapabilityInventory inventory, boolean hasRetrievalCapability) {
        if (hasRetrievalCapability && (inventory.getCandidateSchemaVersion() == 0
                || !inventory.getSupportedRelationTypeUrlsList()
                        .contains(RELATION_ENVELOPE_TYPE_URL))) {
            throw new IllegalArgumentException(
                    "Native retrieval capabilities are missing their typed relation contract");
        }
    }

    private static void validateModels(
            CapabilityInventory inventory, Set<String> capabilityRefs) {
        Set<String> modelIdentities = new HashSet<>();
        for (ModelAvailability availability : inventory.getModelsList()) {
            validateModel(availability, modelIdentities, capabilityRefs);
        }
    }

    private static void validateModel(
            ModelAvailability availability,
            Set<String> modelIdentities,
            Set<String> capabilityRefs) {
        var model = availability.getModel();
        String modelIdentity = model.getId() + "@" + model.getVersion()
                + ":" + Base64.getEncoder().encodeToString(
                        model.getSignatureSha256().toByteArray());
        validateModelIdentity(availability, modelIdentity);
        if (!modelIdentities.add(modelIdentity)) {
            throw new IllegalArgumentException(
                    "Capability inventory has a duplicate model: " + modelIdentity);
        }
        validateModelCapabilities(availability, capabilityRefs);
    }

    private static void validateModelIdentity(
            ModelAvailability availability, String modelIdentity) {
        validateModelDescriptor(availability, modelIdentity);
        validateModelLimits(availability, modelIdentity);
    }

    private static void validateModelDescriptor(
            ModelAvailability availability, String modelIdentity) {
        var model = availability.getModel();
        if (model.getId().isBlank() || model.getVersion().isBlank()
                || model.getSignatureSha256().size() != 32
                || availability.getCapabilityContractsList().isEmpty()
                || !availability.hasLimits()) {
            throw new IllegalArgumentException(
                    "Capability inventory has an invalid model: " + modelIdentity);
        }
    }

    private static void validateModelLimits(
            ModelAvailability availability, String modelIdentity) {
        try {
            ModelResourceEnvelopes.validate(availability.getLimits());
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "Capability inventory model has incomplete limits: " + modelIdentity, failure);
        }
    }

    private static void validateModelCapabilities(
            ModelAvailability availability, Set<String> capabilityRefs) {
        Set<String> modelCapabilities = new HashSet<>();
        for (var contract : availability.getCapabilityContractsList()) {
            if (!hasCompleteContract(contract)) {
                throw new IllegalArgumentException(
                        "Model availability has an incomplete capability output contract");
            }
            var capability = contract.getCapability();
            String capabilityIdentity = refIdentity(capability);
            if (!modelCapabilities.add(capabilityIdentity)) {
                throw new IllegalArgumentException(
                        "Model availability has a duplicate capability: "
                                + capability.getCapabilityId());
            }
            if (!capabilityRefs.contains(capabilityIdentity)) {
                throw new IllegalArgumentException(
                        "Model availability references a capability absent from "
                                + "the same inventory: " + capability.getCapabilityId());
            }
            if (capability.getCapabilityId().equals(
                    BuiltInCapabilityCatalog.AI_EMBEDDING_ID)) {
                VectorLogicalType.requireDimension(contract.getOutputType());
            }
        }
    }

    private static boolean hasCompleteContract(ModelCapabilityContract contract) {
        return contract.hasCapability()
                && contract.hasOutputType()
                && !contract.getOutputType().getCanonicalSql().isBlank()
                && contract.getOutputType().hasSubstraitType()
                && contract.getBindingContractSha256().size() == 32;
    }

    private record CapabilityValidation(Set<String> refs, boolean hasRetrieval) {}

    private static String validateDescriptor(CapabilityDescriptor descriptor) {
        var ref = descriptor.getRef();
        String identity = exactIdentity(
                ref.getCapabilityId(), ref.getSemanticVersion(),
                ref.getImplementationBuildId());
        if (ref.getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException(
                    "Capability has no exact 32-byte signature: " + identity);
        }
        if (descriptor.getKind() == CapabilityKind.CAPABILITY_KIND_UNSPECIFIED
                || descriptor.getPlacement()
                == ExecutionPlacement.EXECUTION_PLACEMENT_UNSPECIFIED
                || descriptor.getExecutionModesList().isEmpty()
                || descriptor.getRequiredPrivilege().isBlank()) {
            throw new IllegalArgumentException(
                    "Capability has an incomplete execution contract: " + identity);
        }
        Set<String> formats = new HashSet<>();
        for (ArtifactFormatRef format : descriptor.getSupportedArtifactFormatsList()) {
            String formatIdentity = validateFormat(format);
            if (!formats.add(formatIdentity)) {
                throw new IllegalArgumentException(
                        "Capability has a duplicate artifact format: " + formatIdentity);
            }
        }
        return identity;
    }

    private static String validateFormat(ArtifactFormatRef format) {
        String identity = format.getFormatId() + "@" + format.getMajorVersion()
                + "." + format.getMinorVersion() + "+" + format.getWriterBuildId();
        if (format.getFormatId().isBlank()
                || format.getMajorVersion() == 0
                || format.getSchemaSha256().size() != 32
                || format.getWriterBuildId().isBlank()) {
            throw new IllegalArgumentException(
                    "Capability has an incomplete artifact format identity: " + identity);
        }
        return identity;
    }

    private static String exactIdentity(String id, String version, String buildId) {
        if (id.isBlank() || version.isBlank() || buildId.isBlank()) {
            throw new IllegalArgumentException("Capability has an incomplete exact identity");
        }
        return id + "@" + version + "+" + buildId;
    }

    private static String refIdentity(
            org.pangrova.protocol.v2.CapabilityRef ref) {
        return ref.getCapabilityId() + "@" + ref.getSemanticVersion()
                + ":" + Base64.getEncoder().encodeToString(
                        ref.getSignatureSha256().toByteArray())
                + "+" + ref.getImplementationBuildId();
    }

    private static boolean isRetrieval(CapabilityKind kind) {
        return kind == CapabilityKind.CAPABILITY_KIND_RETRIEVAL_SOURCE
                || kind == CapabilityKind.CAPABILITY_KIND_RETRIEVAL_MERGE
                || kind == CapabilityKind.CAPABILITY_KIND_RERANK;
    }
}
