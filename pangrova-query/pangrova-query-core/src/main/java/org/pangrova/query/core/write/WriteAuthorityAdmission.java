package org.pangrova.query.core.write;

import org.pangrova.query.spi.write.WriteAuthorityResolver;

import org.pangrova.query.api.write.WriteAuthorityRequest;

import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.security.api.error.AccessDeniedException;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;

import java.util.Objects;

/** Shared pre-allocation boundary for both query and direct statement writes. */
@Internal
public final class WriteAuthorityAdmission {
    private WriteAuthorityAdmission() {}

    /** Requires the installed producer to bind every declared generation intent. */
    public static WriteAuthorizationReceipt resolve(WriteAuthorityRequest request,
            GeneratedColumnSet definitions, WriteAuthorityResolver supplier) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(definitions, "definitions");
        org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec.validate(definitions);
        if (supplier == null) {
            throw new AccessDeniedException(
                    "Writes require durable current authority before connector allocation");
        }
        WriteAuthorizationReceipt receipt = Objects.requireNonNull(
                supplier.resolve(request), "write authorization receipt");
        if (!receipt.declaration().equals(request.compiledDeclaration())
                || receipt.declaration().hasGeneratedColumns() != (definitions.getDefinitionsCount() != 0)
                || !receipt.declaration().generatedColumnsSha256().equals(
                        org.pangrova.connector.api.commit.WriteDeclarationSnapshot.generatedColumnsDigest(
                                definitions.toByteArray()))) {
            throw new AccessDeniedException("Write authority differs from the current table declaration");
        }
        return receipt;
    }
}
