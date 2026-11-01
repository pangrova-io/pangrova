package org.pangrova.coordinator.testinfra;

import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.connector.coordination.WriteDeclarationSnapshot;

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
