package org.pangrova.connector.spi.coordination;

import org.pangrova.connector.spi.metadata.table.QualifiedTableName;

/** Immutable receipt for serialization contracts only, not authorization or actual SDK declaration evidence. */
public final class SpiWriteReceiptFixtures {
    private SpiWriteReceiptFixtures() {}

    /** Provides opaque authority bytes for a pure replay contract that does not execute authorization. */
    public static WriteAuthorizationReceipt receipt() {
        return new WriteAuthorizationReceipt(new byte[]{1}, new WriteDeclarationSnapshot(
                new QualifiedTableName("fixture", "schema", "table"), "0".repeat(64), "0".repeat(64), false));
    }
}

