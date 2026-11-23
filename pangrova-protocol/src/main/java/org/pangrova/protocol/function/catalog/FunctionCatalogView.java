package org.pangrova.protocol.function.catalog;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.function.FunctionPhysicalTypeProjector;
import org.pangrova.protocol.function.FunctionSignatureBinder;
import org.pangrova.protocol.function.selection.FunctionImplementationSelector;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.FunctionCatalogManifest;
import org.pangrova.protocol.v2.FunctionKind;
import org.pangrova.protocol.v2.FunctionReferenceContract;
import org.pangrova.protocol.v2.FunctionResolutionPolicy;
import org.pangrova.protocol.v2.FunctionSignatureContract;
import org.pangrova.protocol.v2.RegisteredFunctionContract;
import org.pangrova.protocol.v2.SubstraitFunctionDefinition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Immutable admitted actual catalogue; contract binding neither instantiates factories nor authenticates a binary. */
public final class FunctionCatalogView {
    public record OrderedBinding(RegisteredFunctionContract registration, int signatureOrdinal,
                                 FunctionSignatureContract signature, FunctionSignatureBinder.Binding binding) {}

    private final FunctionCatalogManifest manifest;
    private final Map<String, SubstraitFunctionDefinition> definitions;
    private final Map<String, RegisteredFunctionContract> registrations;
    private final Set<String> available;
    private final FunctionImplementationSelector selector;
    private final FunctionSignatureBinder signatures;

    /** Validates and captures the complete actual catalogue and its executable availability once. */
    public FunctionCatalogView(CapabilityInventory inventory) {
        FunctionCatalogContracts.validate(Objects.requireNonNull(inventory, "inventory"));
        manifest = inventory.getFunctionCatalog();
        Map<String, SubstraitFunctionDefinition> declared = new HashMap<>();
        manifest.getDefinitionsList().forEach(definition ->
                declared.put(FunctionCatalogContracts.referenceIdentity(definition), definition));
        definitions = Map.copyOf(declared);
        Map<String, RegisteredFunctionContract> actual = new HashMap<>();
        manifest.getRegisteredFunctionsList().forEach(registration -> actual.put(
                FunctionContractHashes.identity(registration.getKind(), registration.getImplementationName()),
                registration));
        registrations = Map.copyOf(actual);
        Set<String> executable = new HashSet<>();
        inventory.getFunctionsList().forEach(function ->
                executable.add(FunctionCatalogContracts.referenceIdentity(function.getRef())));
        available = Set.copyOf(executable);
        var projector = new FunctionPhysicalTypeProjector(manifest);
        selector = new FunctionImplementationSelector(projector);
        signatures = new FunctionSignatureBinder(projector);
    }

    public FunctionCatalogManifest manifest() {
        return manifest;
    }

    /** Resolves an IR anchor's exact kind/URN/name to its captured reference, without alias inference. */
    public SubstraitFunctionDefinition definition(FunctionKind kind, String urn, String name) {
        return findDefinition(FunctionReferenceContract.newBuilder().setKind(validKind(kind))
                .setUrn(Objects.requireNonNull(urn, "urn")).setName(Objects.requireNonNull(name, "name")).build());
    }

    public Optional<SubstraitFunctionDefinition> findDefinition(FunctionKind kind, String urn, String name) {
        var identity = FunctionReferenceContract.newBuilder().setKind(validKind(kind))
                .setUrn(Objects.requireNonNull(urn, "urn")).setName(Objects.requireNonNull(name, "name")).build();
        return Optional.ofNullable(definitions.get(FunctionCatalogContracts.referenceIdentity(identity)));
    }

    /** A reference carrying an unknown field or detached hash never acquires the captured authority. */
    public SubstraitFunctionDefinition definition(FunctionReferenceContract reference) {
        var definition = findDefinition(Objects.requireNonNull(reference, "reference"));
        if (!definition.getRef().equals(reference)) {
            throw new IllegalArgumentException("Function reference differs from the actual captured definition");
        }
        return definition;
    }

    /** Preserves unexposed and opaque actual entries for diagnostics, not as SQL execution authorization. */
    public Optional<RegisteredFunctionContract> registration(FunctionKind kind, String implementationName) {
        return Optional.ofNullable(registrations.get(FunctionContractHashes.identity(validKind(kind),
                Objects.requireNonNull(implementationName, "implementationName"))));
    }

    public boolean isAvailable(FunctionReferenceContract reference) {
        definition(reference);
        return available.contains(FunctionCatalogContracts.referenceIdentity(reference));
    }

    /** Declared targets affect a selected special programme, never ordinary overload selection. */
    public FunctionImplementationSelector.Selection bindScalar(FunctionReferenceContract reference,
            NamedStruct arguments, Set<Integer> constants, Optional<Type> declaredTarget) {
        var registration = direct(reference, FunctionKind.FUNCTION_KIND_SCALAR);
        if (registration.getResolutionPolicy()
                != FunctionResolutionPolicy.FUNCTION_RESOLUTION_POLICY_SCALAR_RUNTIME_PRIORITY) {
            throw new IllegalArgumentException("Scalar registration has no actual runtime selection policy");
        }
        var positions = call(arguments, constants);
        return selector.select(registration.getImplementationsList(), arguments, positions,
                Objects.requireNonNull(declaredTarget, "declaredTarget").orElse(null))
                .orElseThrow(() -> new IllegalArgumentException(
                        "No actual scalar signature matches the concrete call"));
    }

    /** Original raw input types are mandatory; this method cannot reconstruct final-state provenance. */
    public OrderedBinding bindRawAggregate(FunctionReferenceContract reference, NamedStruct arguments,
            Set<Integer> constants, boolean intermediateOutput) {
        return bindOrdered(reference, FunctionKind.FUNCTION_KIND_AGGREGATE, arguments, constants, intermediateOutput);
    }

    public OrderedBinding bindWindow(FunctionReferenceContract reference,
                                     NamedStruct arguments, Set<Integer> constants) {
        return bindOrdered(reference, FunctionKind.FUNCTION_KIND_WINDOW, arguments, constants, false);
    }

    private OrderedBinding bindOrdered(FunctionReferenceContract reference, FunctionKind kind,
            NamedStruct arguments, Set<Integer> constants, boolean intermediateOutput) {
        var registration = direct(reference, kind);
        var positions = call(arguments, constants);
        if (registration.getResolutionPolicy()
                != FunctionResolutionPolicy.FUNCTION_RESOLUTION_POLICY_ORDERED_FIRST_MATCH) {
            throw new IllegalArgumentException("Ordered registration has no actual first-match policy");
        }
        for (int ordinal = 0; ordinal < registration.getSignaturesCount(); ordinal++) {
            var signature = registration.getSignatures(ordinal);
            // Resolve both actual result and state before choosing which is emitted.
            // A selected output failure propagates rather than searching later signatures.
            var bound = signatures.bind(signature, arguments);
            if (bound.isEmpty()) continue;
            FunctionSignatureBinder.requireConstants(signature, arguments.getStruct().getTypesCount(), positions);
            var binding = bound.orElseThrow();
            if (intermediateOutput) {
                var state = binding.intermediate().orElseThrow(() ->
                        new IllegalArgumentException("Raw aggregate has no actual intermediate contract"));
                binding = new FunctionSignatureBinder.Binding(state, binding.intermediate());
            }
            return new OrderedBinding(registration, ordinal, signature, binding);
        }
        throw new IllegalArgumentException("No actual ordered signature matches the concrete call");
    }

    private RegisteredFunctionContract direct(FunctionReferenceContract reference, FunctionKind expectedKind) {
        var definition = definition(reference);
        if (reference.getKind() != expectedKind) {
            throw new IllegalArgumentException("Function call has the wrong exact kind");
        }
        if (definition.hasRewrite() || !definition.getStructuralLowering().isEmpty()) {
            throw new IllegalArgumentException(
                    "Declared rewrite or structural lowering is not a direct registered call");
        }
        if (!available.contains(FunctionCatalogContracts.referenceIdentity(reference))) {
            throw new IllegalArgumentException("Actual function is unavailable in this captured catalogue");
        }
        return registration(expectedKind, definition.getImplementationName()).orElseThrow(() ->
                new IllegalArgumentException("Actual direct function registration is absent"));
    }

    private SubstraitFunctionDefinition findDefinition(FunctionReferenceContract reference) {
        validKind(reference.getKind());
        var definition = definitions.get(FunctionCatalogContracts.referenceIdentity(reference));
        if (definition == null) {
            throw new IllegalArgumentException("Exact function kind/URN/name is absent from the captured catalogue");
        }
        return definition;
    }

    private static Set<Integer> call(NamedStruct arguments, Set<Integer> constants) {
        SubstraitRelationLayout.topLevelNames(Objects.requireNonNull(arguments, "arguments"));
        var positions = Set.copyOf(constants);
        FunctionSignatureBinder.validateConstantOrdinals(arguments.getStruct().getTypesCount(), positions);
        return positions;
    }

    private static FunctionKind validKind(FunctionKind kind) {
        if (kind == null || kind == FunctionKind.UNRECOGNIZED || kind == FunctionKind.FUNCTION_KIND_UNSPECIFIED) {
            throw new IllegalArgumentException("Unknown actual function kind");
        }
        return kind;
    }
}
