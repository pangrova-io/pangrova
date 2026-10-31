package org.pangrova.metadata.testinfra;

import org.pangrova.security.api.authz.DeferredAuthorizationScope;
import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.coordination.WriteAuthorizationReceipt;
import org.pangrova.connector.spi.coordination.WriteDeclarationSnapshot;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Explicit immutable receipt for journal serialization contracts, not an authorization or SDK proof. */
public final class MetadataWriteReceiptFixtures {
    private MetadataWriteReceiptFixtures() {}

    /** Creates a canonical ordinary-write receipt; no authority is provisioned by this fixture. */
    public static WriteAuthorizationReceipt receipt() {
        var table = new QualifiedTableName("fixture", "schema", "table");
        var declaration = new WriteDeclarationSnapshot(table, "0".repeat(64), "0".repeat(64), false);
        var permission = new DeferredAuthorizationScope.TablePermission(table, Set.of(), Privilege.INSERT,
                DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty());
        return DeferredAuthorizationScopeCodec.encodeWriteReceipt(new DeferredAuthorizationScope(List.of(permission),
                Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, List.of()))));
    }
}
