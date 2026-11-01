package org.pangrova.security.api.authz;

import org.pangrova.spi.security.authz.DeferredAuthorizationScope;

import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.coordination.WriteDeclarationSnapshot;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.spi.security.policy.Privilege;

class GeneratedWriteScopeTest {
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "sales", "documents");
    private static final DeferredAuthorizationScope.ModelSnapshot MODEL = new DeferredAuthorizationScope.ModelSnapshot(
            "embed", "incarnation-1", 1, "deployment-1", "a".repeat(64));

    @Test
    void generatedScopeRequiresUniqueExactModelsAndOrdinaryAbsenceForbidsThem() {
        assertThatThrownBy(() -> new DeferredAuthorizationScope.DefinitionSnapshot(declaration(true), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exact model contracts");
        assertThatThrownBy(() -> new DeferredAuthorizationScope.DefinitionSnapshot(declaration(true), List.of(MODEL, MODEL)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unique logical names");
        assertThatThrownBy(() -> new DeferredAuthorizationScope.DefinitionSnapshot(declaration(false), List.of(MODEL)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalWriteReceiptRoundTripsByValueAndRejectsCorruptedModelReceipt() {
        var scope = new DeferredAuthorizationScope(List.of(
                new DeferredAuthorizationScope.TablePermission(TABLE, Set.of("body"), Privilege.SELECT,
                        DeferredAuthorizationScope.ReadMode.RAW_READ, DeferredAuthorizationScope.PolicySnapshot.empty()),
                new DeferredAuthorizationScope.TablePermission(TABLE, Set.of(), Privilege.INSERT,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())),
                Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration(true), List.of(MODEL))));
        var receipt = DeferredAuthorizationScopeCodec.encodeWriteReceipt(scope);
        assertThat(DeferredAuthorizationScopeCodec.decodeWriteReceipt(receipt.authorizationScope())).isEqualTo(receipt);
        String json = new String(receipt.authorizationScope(), StandardCharsets.UTF_8);
        int models = json.indexOf("\"models\":[");
        int end = json.indexOf(']', models);
        String corrupt = json.substring(0, models) + "\"models\":[]" + json.substring(end + 1);
        assertThatThrownBy(() -> DeferredAuthorizationScopeCodec.decodeWriteReceipt(corrupt.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static WriteDeclarationSnapshot declaration(boolean generated) {
        return new WriteDeclarationSnapshot(TABLE, "a".repeat(64), "b".repeat(64), generated);
    }
}
