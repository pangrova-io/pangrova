package org.pangrova.coordinator.query.execution;

import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.spi.annotation.Internal;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.coordination.WriteAuthorizationReceipt;

import java.util.Objects;

/** Shared pre-allocation boundary for both query and direct statement writes. */
@Internal
public final class GeneratedWriteAuthorityAdmission {
    private GeneratedWriteAuthorityAdmission() {}

    /** Requires the installed producer to bind every declared generation intent. */
    public static WriteAuthorizationReceipt resolve(GeneratedWriteAuthoritySupplier.WriteAuthorityRequest request,
            GeneratedColumnSet definitions, GeneratedWriteAuthoritySupplier supplier) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(definitions, "definitions");
        org.pangrova.protocol.GeneratedColumnMetadataCodec.validate(definitions);
        if (supplier == null) {
            throw new AccessDeniedException(
                    "Writes require durable current authority before connector allocation");
        }
        WriteAuthorizationReceipt receipt = Objects.requireNonNull(
                supplier.resolve(request), "write authorization receipt");
        if (!receipt.declaration().equals(request.compiledDeclaration())
                || receipt.declaration().hasGeneratedColumns() != (definitions.getDefinitionsCount() != 0)
                || !receipt.declaration().generatedColumnsSha256().equals(
                        org.pangrova.spi.coordination.WriteDeclarationSnapshot.generatedColumnsDigest(
                                definitions.toByteArray()))) {
            throw new AccessDeniedException("Write authority differs from the current table declaration");
        }
        return receipt;
    }
}
