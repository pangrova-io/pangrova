package org.pangrova.connector.lance;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.coordination.WriteDeclarationSnapshot;

/** Immutable receipt for serialization contracts only, not authorization or actual SDK declaration evidence. */
public final class LanceWriteReceiptFixtures {
    private LanceWriteReceiptFixtures() {}

    /** Provides opaque authority bytes for a pure replay contract that does not execute authorization. */
    public static WriteAuthorizationReceipt receipt() {
        return new WriteAuthorizationReceipt(new byte[]{1}, new WriteDeclarationSnapshot(
                new QualifiedTableName("fixture", "schema", "table"), "0".repeat(64), "0".repeat(64), false));
    }
}

