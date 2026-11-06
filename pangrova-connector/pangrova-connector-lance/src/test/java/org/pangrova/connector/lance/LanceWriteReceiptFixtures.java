package org.pangrova.connector.lance;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

/** Immutable receipt for serialization contracts only, not authorization or actual SDK declaration evidence. */
public final class LanceWriteReceiptFixtures {
    private LanceWriteReceiptFixtures() {}

    /** Provides opaque authority bytes for a pure replay contract that does not execute authorization. */
    public static WriteAuthorizationReceipt receipt() {
        return new WriteAuthorizationReceipt(new byte[]{1}, new WriteDeclarationSnapshot(
                new QualifiedTableName("fixture", "schema", "table"), "0".repeat(64), "0".repeat(64), false));
    }
}

