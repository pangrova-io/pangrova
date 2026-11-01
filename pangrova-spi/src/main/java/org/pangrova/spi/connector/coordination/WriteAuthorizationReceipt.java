package org.pangrova.spi.connector.coordination;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;
import java.util.Arrays;

/** One admitted scope and its same-observation declaration, carried unchanged into the redo journal. */
@Experimental
public record WriteAuthorizationReceipt(byte[] authorizationScope, WriteDeclarationSnapshot declaration) {
    /** Copies mandatory authority bytes; ordinary writes also require an explicit declaration receipt. */
    public WriteAuthorizationReceipt {
        authorizationScope = Objects.requireNonNull(authorizationScope, "authorizationScope").clone();
        if (authorizationScope.length == 0) {
            throw new IllegalArgumentException("Every write requires a nonempty authorization receipt");
        }
        Objects.requireNonNull(declaration, "declaration");
    }

    @Override
    public byte[] authorizationScope() { return authorizationScope.clone(); }

    @Override
    public boolean equals(Object other) {
        return other instanceof WriteAuthorizationReceipt receipt
                && declaration.equals(receipt.declaration)
                && Arrays.equals(authorizationScope, receipt.authorizationScope);
    }

    @Override
    public int hashCode() { return 31 * declaration.hashCode() + Arrays.hashCode(authorizationScope); }
}
