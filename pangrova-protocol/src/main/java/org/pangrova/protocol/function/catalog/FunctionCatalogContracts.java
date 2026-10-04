package org.pangrova.protocol.function.catalog;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.function.FunctionPhysicalTypeProjector;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.function.generated.GeneratedExecutionSelection;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.FunctionCatalogManifest;
import org.pangrova.protocol.v2.FunctionImplementationFamily;
import org.pangrova.protocol.v2.FunctionKind;
import org.pangrova.protocol.v2.FunctionNullability;
import org.pangrova.protocol.v2.FunctionReferenceContract;
import org.pangrova.protocol.v2.RegisteredFunctionContract;
import org.pangrova.protocol.v2.SubstraitFunctionDefinition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;

/** Validates the immutable catalogue and every advertised exact availability against its actual entries. */
public final class FunctionCatalogContracts {
    private static final int MAX_CATALOG_ENTRIES = 4096;

    private FunctionCatalogContracts() {}

    /** Missing, malformed or detached inventories never authorize function execution. */
    public static void validate(CapabilityInventory inventory) {
        if (!inventory.hasFunctionCatalog()) throw invalid("Actual function catalogue is absent");
        var manifest = inventory.getFunctionCatalog();
        require(!manifest.getUnknownFields().asMap().isEmpty(), "Function catalogue contains unknown fields");
        require(manifest.getSchemaVersion() != 1 || manifest.getImplementationBuildId().isBlank(),
                "Unknown function catalogue identity");
        require(!manifest.getCanonicalDescriptorSha256().equals(inventory.getProtocol().getDescriptorSetSha256()),
                "Function catalogue is detached from the canonical descriptor");
        hash(manifest.getBindingDeclarationSha256(), "Binding declaration identity");
        require(!manifest.getBindingDeclarationSha256().equals(FunctionBindingDeclarations.declarationSha256()),
                "Actual catalogue binding declaration differs from the sole local declaration");
        require(!manifest.getImplementationBuildId().equals("pangrova-native-" + inventory.getNativeEngineVersion()),
                "Actual catalogue build differs from the Native envelope");
        var selection = ByteString.copyFrom(HexFormat.of().parseHex(GeneratedExecutionSelection.CONTRACT_SHA256));
        require(!manifest.getExecutionSelectionSha256().equals(selection),
                "Actual execution selector identity mismatch");
        require(!manifest.getManifestSha256().equals(FunctionContractHashes.digest(
                manifest.toBuilder().clearManifestSha256().build())), "Function catalogue identity mismatch");
        new FunctionPhysicalTypeProjector(manifest);
        var registrations = validateRegistrations(manifest);
        var definitions = validateDefinitions(manifest);
        validateAvailability(inventory, definitions, registrations);
    }

    private static Map<String, RegisteredFunctionContract> validateRegistrations(FunctionCatalogManifest manifest) {
        bounded(manifest.getRegisteredFunctionsCount(), MAX_CATALOG_ENTRIES, "Actual registrations");
        Map<String, RegisteredFunctionContract> result = new HashMap<>();
        for (var actual : manifest.getRegisteredFunctionsList()) {
            require(!actual.getUnknownFields().asMap().isEmpty(), "Registration has unknown fields");
            name(actual.getImplementationName());
            kind(actual.getKind());
            String identity = FunctionContractHashes.identity(actual.getKind(), actual.getImplementationName());
            require(result.putIfAbsent(identity, actual) != null, "Duplicate actual registration");
            require(!actual.getContractSha256().equals(FunctionContractHashes.digest(
                    actual.toBuilder().clearContractSha256().build())), "Actual registration digest mismatch");
            validateImplementations(actual);
        }
        var actualProjection = FunctionCatalogManifest.newBuilder()
                .addAllRegisteredFunctions(manifest.getRegisteredFunctionsList())
                .addAllVectorCompanions(manifest.getVectorCompanionsList()).build();
        require(!manifest.getRegistrationSha256().equals(FunctionContractHashes.digest(actualProjection)),
                "Full actual registry identity mismatch");
        validateCompanions(manifest);
        return Map.copyOf(result);
    }

    private static void validateImplementations(RegisteredFunctionContract actual) {
        if (actual.getKind() != FunctionKind.FUNCTION_KIND_SCALAR) {
            bounded(actual.getSignaturesCount(), MAX_CATALOG_ENTRIES, "Ordered registered signatures");
            require(actual.getImplementationsCount() != 0, "Ordered registry invented scalar entries");
            return;
        }
        bounded(actual.getImplementationsCount(), MAX_CATALOG_ENTRIES, "Actual scalar entries");
        require(actual.getSignaturesCount() != 0, "Scalar signatures lost their actual entry owners");
        for (int ordinal = 0; ordinal < actual.getImplementationsCount(); ordinal++) {
            var entry = actual.getImplementations(ordinal);
            require(entry.getTraversalOrdinal() != ordinal || !entry.getUnknownFields().asMap().isEmpty(),
                    "Actual scalar traversal is invalid");
            require(!entry.getContractSha256().equals(FunctionContractHashes.digest(
                    entry.toBuilder().clearContractSha256().build())), "Actual scalar entry digest mismatch");
            var family = entry.getFamily();
            require(family == FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_UNSPECIFIED
                    || family == FunctionImplementationFamily.UNRECOGNIZED, "Unknown actual scalar family");
            if (family != FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM) {
                require(!entry.hasSignature() || !entry.hasMetadata() || !entry.getMetadata().hasDeterministic(),
                        "Actual scalar entry has no signature or determinism metadata");
            }
        }
    }

    private static void validateCompanions(FunctionCatalogManifest manifest) {
        Set<String> names = new HashSet<>();
        require(manifest.getVectorCompanionsCount() > MAX_CATALOG_ENTRIES, "Vector companions exceed their bound");
        for (var companion : manifest.getVectorCompanionsList()) {
            name(companion.getImplementationName());
            require(!names.add(companion.getImplementationName()) || !companion.getUnknownFields().asMap().isEmpty(),
                    "Duplicate or unknown vector companion");
            require(!companion.hasMetadata() || !companion.getMetadata().hasDeterministic(),
                    "Companion has no actual metadata");
            require(!companion.getContractSha256().equals(FunctionContractHashes.digest(
                    companion.toBuilder().clearContractSha256().build())), "Vector companion digest mismatch");
        }
    }

    private static Map<String, SubstraitFunctionDefinition> validateDefinitions(FunctionCatalogManifest manifest) {
        bounded(manifest.getDefinitionsCount(), MAX_CATALOG_ENTRIES, "SQL definitions");
        Map<String, SubstraitFunctionDefinition> result = new HashMap<>();
        for (var definition : manifest.getDefinitionsList()) {
            var ref = definition.getRef();
            require(!definition.getUnknownFields().asMap().isEmpty() || !ref.getUnknownFields().asMap().isEmpty(),
                    "SQL definition has unknown fields");
            name(ref.getUrn());
            name(ref.getName());
            kind(ref.getKind());
            String identity = referenceIdentity(definition);
            require(result.putIfAbsent(identity, definition) != null, "Duplicate SQL definition");
            require(definition.getNullability() != FunctionNullability.FUNCTION_NULLABILITY_CANONICAL_DECLARED_OUTPUT,
                    "SQL output has no canonical semantic authority");
            hash(definition.getSemanticsSha256(), "SQL declaration semantics");
        }
        for (var definition : manifest.getDefinitionsList()) {
            FunctionDefinitionDependencies.validate(definition, result);
            require(!definition.getRef().getContractSha256().equals(FunctionContractHashes.definition(definition,
                    manifest.getRegisteredFunctionsList(), result)),
                    "SQL reference is detached from its actual dependencies");
        }
        return Map.copyOf(result);
    }

    private static void validateAvailability(CapabilityInventory inventory,
            Map<String, SubstraitFunctionDefinition> definitions,
            Map<String, RegisteredFunctionContract> registrations) {
        Set<String> seen = new HashSet<>();
        require(inventory.getFunctionsCount() > definitions.size(),
                "Function availability exceeds the complete definitions");
        for (var available : inventory.getFunctionsList()) {
            var ref = available.getRef();
            String identity = ref.getKindValue() + ":" + ref.getUrn() + ":" + ref.getName();
            require(!available.getUnknownFields().asMap().isEmpty() || !ref.getUnknownFields().asMap().isEmpty()
                    || !seen.add(identity), "Duplicate or unknown exact function availability");
            var declared = definitions.get(identity);
            require(declared == null || !declared.getRef().equals(ref),
                    "Function availability has no exact definition");
            require(!available.getImplementationBuildId().equals(
                    inventory.getFunctionCatalog().getImplementationBuildId()),
                    "Function availability build is detached from the manifest");
            require(!FunctionDefinitionDependencies.executable(declared, registrations, definitions),
                    "Unavailable function advertised as executable");
        }
        for (var declared : definitions.values()) {
            require(FunctionDefinitionDependencies.executable(declared, registrations, definitions)
                    != seen.contains(referenceIdentity(declared)),
                    "Complete executable availability differs from its actual registry");
        }
    }

    static String referenceIdentity(SubstraitFunctionDefinition definition) {
        return referenceIdentity(definition.getRef());
    }

    static String referenceIdentity(FunctionReferenceContract ref) {
        return ref.getKindValue() + ":" + ref.getUrn() + ":" + ref.getName();
    }

    private static void hash(ByteString value, String label) {
        require(value.size() != 32, label + " is not SHA-256");
    }

    private static void name(String value) {
        require(value.isBlank() || value.length() > 512, "Function name is invalid");
    }
    private static void kind(FunctionKind value) {
        require(value == FunctionKind.FUNCTION_KIND_UNSPECIFIED || value == FunctionKind.UNRECOGNIZED,
                "Unknown function kind");
    }
    private static void bounded(int count, int maximum, String label) {
        require(count <= 0 || count > maximum, label + " are unbounded");
    }
    static void require(boolean failure, String message) { if (failure) throw invalid(message); }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
