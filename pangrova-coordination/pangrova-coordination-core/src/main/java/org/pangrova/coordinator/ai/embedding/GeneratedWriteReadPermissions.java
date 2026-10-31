package org.pangrova.coordinator.ai.embedding;

import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Combines same-invocation observations without accepting policy changes between compiler invocations. */
final class GeneratedWriteReadPermissions {
    private GeneratedWriteReadPermissions() {}

    static List<DeferredAuthorizationScope.TablePermission> merge(
            List<DeferredAuthorizationScope.TablePermission> reads) {
        Map<QualifiedTableName, DeferredAuthorizationScope.TablePermission> merged = new HashMap<>();
        for (var read : reads) {
            merged.merge(read.table(), read, GeneratedWriteReadPermissions::mergeTable);
        }
        return List.copyOf(merged.values());
    }

    private static DeferredAuthorizationScope.TablePermission mergeTable(
            DeferredAuthorizationScope.TablePermission first, DeferredAuthorizationScope.TablePermission next) {
        var before = first.observedPolicy();
        var after = next.observedPolicy();
        if (!before.rowFilter().equals(after.rowFilter())) {
            throw new AuthorizationDeniedException("Row policy changed between compilations; recompile write");
        }
        var masks = new HashMap<>(before.columnMasks());
        after.columnMasks().forEach((column, expression) -> {
            if (masks.containsKey(column) && !masks.get(column).equals(expression)) {
                throw new AuthorizationDeniedException("Column policy changed between compilations; recompile write");
            }
            masks.put(column, expression);
        });
        return new DeferredAuthorizationScope.TablePermission(first.table(), masks.keySet(), Privilege.SELECT,
                DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                new DeferredAuthorizationScope.PolicySnapshot(before.rowFilter(), masks));
    }
}
