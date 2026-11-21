package org.pangrova.protocol.function.catalog;

import org.pangrova.protocol.v2.FunctionKind;
import org.pangrova.protocol.v2.FunctionSignatureContract;
import org.pangrova.protocol.v2.FunctionTypeTemplate;
import org.pangrova.protocol.v2.FunctionReferenceContract;
import org.pangrova.protocol.v2.RegisteredFunctionContract;
import org.pangrova.protocol.v2.SubstraitFunctionDefinition;

import java.util.Map;

/** Exact native dependencies of one declared binding; never a second function catalogue. */
final class FunctionDefinitionDependencies {
    private static final int MAX_REWRITE_INPUT_ORDINAL = 65535;

    private FunctionDefinitionDependencies() {}

    static void validate(SubstraitFunctionDefinition definition, Map<String, SubstraitFunctionDefinition> definitions) {
        int paths = (definition.getImplementationName().isEmpty() ? 0 : 1) + (definition.hasRewrite() ? 1 : 0)
                + (definition.getStructuralLowering().isEmpty() ? 0 : 1);
        FunctionCatalogContracts.require(paths != 1, "SQL definition must own exactly one execution path");
        if (!definition.hasRewrite()) return;
        var rewrite = definition.getRewrite();
        FunctionCatalogContracts.require(rewrite.getNodesCount() == 0 || rewrite.getNodesCount() > 64
                || Integer.toUnsignedLong(rewrite.getResultNodeOrdinal()) >= rewrite.getNodesCount()
                || !rewrite.getUnknownFields().asMap().isEmpty(), "Rewrite graph is invalid");
        for (int ordinal = 0; ordinal < rewrite.getNodesCount(); ordinal++) {
            var node = rewrite.getNodes(ordinal);
            FunctionCatalogContracts.require(!node.hasTarget() || !node.getImplementationName().isEmpty()
                    || !node.getUnknownFields().asMap().isEmpty() || node.getArgumentsCount() > 256,
                    "Rewrite node has an ambiguous actual dependency");
            target(node.getTarget(), definitions);
            for (var argument : node.getArgumentsList()) {
                FunctionCatalogContracts.require(!argument.getUnknownFields().asMap().isEmpty(),
                        "Rewrite argument has unknown fields");
                switch (argument.getSourceCase()) {
                    case INPUT_ORDINAL -> FunctionCatalogContracts.require(
                            Integer.toUnsignedLong(argument.getInputOrdinal()) > MAX_REWRITE_INPUT_ORDINAL,
                            "Rewrite input is unbounded");
                    case NODE_ORDINAL -> FunctionCatalogContracts.require(
                            Integer.toUnsignedLong(argument.getNodeOrdinal()) >= ordinal,
                            "Rewrite dependency is forward or cyclic");
                    default -> throw new IllegalArgumentException("Rewrite argument has no declared actual source");
                }
            }
        }
    }

    static boolean executable(SubstraitFunctionDefinition definition,
                              Map<String, RegisteredFunctionContract> registrations,
                              Map<String, SubstraitFunctionDefinition> definitions) {
        if (!definition.getStructuralLowering().isEmpty()) return false;
        if (definition.hasRewrite()) {
            return definition.getRewrite().getNodesList().stream().allMatch(node -> executable(registrations.get(
                    FunctionContractHashes.identity(FunctionKind.FUNCTION_KIND_SCALAR,
                            target(node.getTarget(), definitions).getImplementationName()))));
        }
        return executable(registrations.get(FunctionContractHashes.identity(definition.getRef().getKind(),
                definition.getImplementationName())));
    }

    static SubstraitFunctionDefinition target(FunctionReferenceContract reference,
                                             Map<String, SubstraitFunctionDefinition> definitions) {
        FunctionCatalogContracts.require(reference.getKind() != FunctionKind.FUNCTION_KIND_SCALAR
                || !reference.getUnknownFields().asMap().isEmpty() || !reference.getContractSha256().isEmpty(),
                "Rewrite requires an exact direct scalar declaration identity");
        var target = definitions.get(FunctionCatalogContracts.referenceIdentity(reference));
        FunctionCatalogContracts.require(target == null || target.getImplementationName().isBlank()
                || target.hasRewrite() || !target.getStructuralLowering().isEmpty(),
                "Rewrite target has no direct sole declaration");
        return target;
    }

    private static boolean executable(RegisteredFunctionContract registration) {
        if (registration == null) return false;
        if (registration.getKind() != FunctionKind.FUNCTION_KIND_SCALAR) {
            return registration.getSignaturesList().stream().anyMatch(FunctionDefinitionDependencies::canonical);
        }
        return registration.getImplementationsList().stream().anyMatch(implementation ->
                (implementation.hasSignature() && canonical(implementation.getSignature()))
                        || !implementation.getSpecialTypeProgram().isEmpty());
    }

    private static boolean canonical(FunctionSignatureContract signature) {
        return signature.hasOutput() && canonical(signature.getOutput())
                && (!signature.hasIntermediate() || canonical(signature.getIntermediate()))
                && signature.getArgumentsList().stream().allMatch(FunctionDefinitionDependencies::canonical);
    }

    private static boolean canonical(FunctionTypeTemplate type) {
        return !type.hasOpaqueRegisteredType() && (type.hasSkeleton() || type.hasTypeVariable() || type.hasAnyType())
                && type.getSlotsList().stream().allMatch(slot -> !slot.hasType() || canonical(slot.getType()));
    }
}
