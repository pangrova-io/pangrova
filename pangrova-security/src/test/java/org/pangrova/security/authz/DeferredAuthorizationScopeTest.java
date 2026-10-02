package org.pangrova.security.authz;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.StringLength;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.coordination.WriteDeclarationSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeferredAuthorizationScopeTest {
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "sales", "facts");

    @Test
    void roundTripsExactRawAndObservedScopesWithoutCredentialContext() {
        var source = new DeferredAuthorizationScope.TablePermission(TABLE, Set.of("amount"), Privilege.SELECT,
                DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                new DeferredAuthorizationScope.PolicySnapshot(Optional.of("amount > 10"),
                        Map.of("amount", Optional.empty())));
        var target = new DeferredAuthorizationScope.TablePermission(TABLE, Set.of(), Privilege.ALTER,
                DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty());
        var scope = new DeferredAuthorizationScope(List.of(source, target), Optional.empty());
        assertThat(DeferredAuthorizationScopeCodec.decode(DeferredAuthorizationScopeCodec.encode(scope))).isEqualTo(scope);
        assertThat(scope.sha256()).hasSize(64);
        assertThat(new String(DeferredAuthorizationScopeCodec.encode(scope), java.nio.charset.StandardCharsets.UTF_8))
                .doesNotContain("token", "groups", "issuer", "policyEpoch");
    }

    @Test
    void requiresSourceSelectTargetAlterAndCompleteObservation() {
        assertThatThrownBy(() -> new DeferredAuthorizationScope(List.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeferredAuthorizationScope.TablePermission(TABLE, Set.of("amount"), Privilege.SELECT,
                DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED, DeferredAuthorizationScope.PolicySnapshot.empty()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("every read column");
        assertThatThrownBy(() -> DeferredAuthorizationScope.raw(TABLE, Set.of(), TABLE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deterministicHashIgnoresSetAndMaskMapInsertionOrderButNotPolicyContent() {
        var first = DeferredAuthorizationScope.raw(TABLE, new java.util.LinkedHashSet<>(List.of("a", "b")), TABLE);
        var second = DeferredAuthorizationScope.raw(TABLE, new java.util.LinkedHashSet<>(List.of("b", "a")), TABLE);
        assertThat(first.sha256()).isEqualTo(second.sha256());
        var transformed = new DeferredAuthorizationScope(List.of(
                new DeferredAuthorizationScope.TablePermission(TABLE, Set.of("a", "b"), Privilege.SELECT,
                        DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                        new DeferredAuthorizationScope.PolicySnapshot(Optional.of("a > 1"),
                                Map.of("a", Optional.empty(), "b", Optional.of("NULL")))),
                first.permissions().get(1)), Optional.empty());
        assertThat(transformed.sha256()).isNotEqualTo(first.sha256());
    }

    @Test
    void rejectsMissingCorruptAndUnboundedMetadataScopes() {
        assertThatThrownBy(() -> DeferredAuthorizationScopeCodec.decode(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeferredAuthorizationScopeCodec.decode(new byte[] {1, 2}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeferredAuthorizationScopeCodec.decode("[]".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DeferredAuthorizationScopeCodec.decode(new byte[1024 * 1024 + 1]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void modelDefinitionSnapshotsAreSortedCopiedAndImmutable() {
        var declaration = new WriteDeclarationSnapshot(TABLE, "0".repeat(64), "1".repeat(64), true);
        var first = new DeferredAuthorizationScope.ModelSnapshot("a", "incarnation-a", 1, "deployment-a",
                "2".repeat(64));
        var second = new DeferredAuthorizationScope.ModelSnapshot("b", "incarnation-b", 2, "deployment-b",
                "3".repeat(64));
        var callerModels = new ArrayList<>(List.of(second, first));
        var snapshot = new DeferredAuthorizationScope.DefinitionSnapshot(declaration, callerModels);
        callerModels.clear();
        assertThat(snapshot.models()).containsExactly(first, second);
        assertThatThrownBy(() -> snapshot.models().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Property(tries = 100)
    void codecRoundTripsExactObservedContent(@ForAll @StringLength(max = 256) String expression) {
        var scope = new DeferredAuthorizationScope(List.of(
                new DeferredAuthorizationScope.TablePermission(TABLE, Set.of("amount"), Privilege.SELECT,
                        DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED,
                        new DeferredAuthorizationScope.PolicySnapshot(Optional.of(expression),
                                Map.of("amount", Optional.of(expression)))),
                new DeferredAuthorizationScope.TablePermission(TABLE, Set.of(), Privilege.ALTER,
                        DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty())), Optional.empty());
        byte[] bytes = DeferredAuthorizationScopeCodec.encode(scope);
        assertThat(DeferredAuthorizationScopeCodec.decode(bytes)).isEqualTo(scope);
        assertThat(DeferredAuthorizationScopeCodec.encode(DeferredAuthorizationScopeCodec.decode(bytes))).isEqualTo(bytes);
    }
}
