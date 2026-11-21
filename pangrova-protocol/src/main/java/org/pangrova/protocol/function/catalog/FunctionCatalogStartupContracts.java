package org.pangrova.protocol.function.catalog;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.function.generated.GeneratedExecutionSelection;
import org.pangrova.protocol.v2.FunctionCatalogFinalizeRequest;
import org.pangrova.protocol.v2.FunctionCatalogFinalizeResult;

import java.util.HexFormat;
import java.util.Objects;

/** One explicit, bounded transition from actual startup registration to task admission. */
public final class FunctionCatalogStartupContracts {
    private static final long MAX_ENCODED_INVENTORY_BYTES = 4L * 1024 * 1024;

    private FunctionCatalogStartupContracts() {}

    /** Builds the exact declaration and protocol identity; this is not a capability claim. */
    public static FunctionCatalogFinalizeRequest request() {
        return FunctionCatalogFinalizeRequest.newBuilder()
                .setProtocol(CanonicalProtocolIdentity.current())
                .setBindingDeclarationSha256(FunctionBindingDeclarations.declarationSha256())
                .setExecutionSelectionSha256(ByteString.copyFrom(
                        HexFormat.of().parseHex(GeneratedExecutionSelection.CONTRACT_SHA256)))
                .build();
    }

    /** Checks the actual Native completion receipt, including the existing encoded transport bound. */
    public static void validate(FunctionCatalogFinalizeResult result) {
        Objects.requireNonNull(result, "result");
        long bytes = result.getInventoryBytes();
        long encoded = result.getEncodedInventoryBytes();
        if (!result.getUnknownFields().asMap().isEmpty() || result.getManifestSha256().size() != 32
                || bytes <= 0 || bytes > MAX_ENCODED_INVENTORY_BYTES
                || encoded != 4L * ((bytes + 2L) / 3L) || encoded > MAX_ENCODED_INVENTORY_BYTES) {
            throw new IllegalArgumentException("Invalid actual function catalogue completion receipt");
        }
    }
}
