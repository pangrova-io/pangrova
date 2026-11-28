package org.pangrova.protocol.function.catalog;

import org.pangrova.common.crypto.Sha256Digests;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.pangrova.protocol.v2.FunctionCatalogManifest;
import org.pangrova.protocol.v2.FunctionKind;
import org.pangrova.protocol.v2.RegisteredFunctionContract;
import org.pangrova.protocol.v2.SubstraitFunctionDefinition;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Deterministic identities of complete contracts; never authentication or execution evidence. */
public final class FunctionContractHashes {
    private FunctionContractHashes() {}

    /** SHA-256 of the actual canonical deterministic protobuf encoding. */
    public static ByteString digest(Message message) {
        byte[] bytes = new byte[message.getSerializedSize()];
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            message.writeTo(output);
            output.checkNoSpaceLeft();
            return ByteString.copyFrom(Sha256Digests.sha256().digest(bytes));
        } catch (IOException failure) {
            throw new IllegalStateException("Canonical function contract digest failed", failure);
        }
    }

    /** A public reference includes its declaration and actual matched implementation dependencies. */
    public static ByteString definition(SubstraitFunctionDefinition definition,
                                         List<RegisteredFunctionContract> registrations,
                                         Map<String, SubstraitFunctionDefinition> definitions) {
        var cleared = definition.toBuilder().setRef(definition.getRef().toBuilder().clearContractSha256()).build();
        var contract = FunctionCatalogManifest.newBuilder().addDefinitions(cleared);
        Set<String> dependencies = new HashSet<>();
        if (definition.hasRewrite()) {
            definition.getRewrite().getNodesList().forEach(node -> dependencies.add(
                    identity(FunctionKind.FUNCTION_KIND_SCALAR,
                            FunctionDefinitionDependencies.target(node.getTarget(), definitions)
                                    .getImplementationName())));
        } else {
            dependencies.add(identity(definition.getRef().getKind(), definition.getImplementationName()));
        }
        registrations.stream().filter(actual -> dependencies.contains(
                identity(actual.getKind(), actual.getImplementationName())))
                .sorted(Comparator.comparingInt((RegisteredFunctionContract value) -> value.getKindValue())
                        .thenComparing(RegisteredFunctionContract::getImplementationName))
                .forEach(contract::addRegisteredFunctions);
        return digest(contract.build());
    }

    static String identity(FunctionKind kind, String name) {
        return kind.getNumber() + ":" + name;
    }
}
