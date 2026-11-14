package org.pangrova.coordinator.testinfra;

import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Explicit immutable receipt for coordinator redo contracts, not authorization or execution proof. */
public final class CoordinatorWriteReceiptFixtures {
    private CoordinatorWriteReceiptFixtures() {}

    /** Creates a canonical ordinary receipt without provisioning any role, grant or tenant. */
    public static WriteAuthorizationReceipt receipt() {
        var table = new QualifiedTableName("fixture", "schema", "table");
        var declaration = new WriteDeclarationSnapshot(table, "0".repeat(64), "0".repeat(64), false);
        var permission = new DeferredAuthorizationScope.TablePermission(table, Set.of(), Privilege.INSERT,
                DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty());
        return DeferredAuthorizationScopeCodec.encodeWriteReceipt(new DeferredAuthorizationScope(List.of(permission),
                Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, List.of()))));
    }
}
