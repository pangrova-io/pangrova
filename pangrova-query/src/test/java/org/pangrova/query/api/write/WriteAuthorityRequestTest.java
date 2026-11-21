package org.pangrova.query.api.write;

import org.pangrova.security.api.enums.AuthorizedReadMode;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.enums.Privilege;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteAuthorityRequestTest {
    private static final QualifiedTableName TABLE = new QualifiedTableName("catalog", "schema", "table");
    private static final WriteDeclarationSnapshot DECLARATION = new WriteDeclarationSnapshot(
            TABLE, "a".repeat(64), "b".repeat(64), false);
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "writer"; }
        @Override public Optional<String> getCatalog() { return Optional.of("catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("schema"); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "tenant"); }
        @Override public Optional<ConnectorQueryAuthority> getQueryAuthority() { return Optional.empty(); }
    };

    @Test
    void capturesReadObservationsAndWriteActionsWithoutOwningTheBorrowedSession() {
        var observation = read(AuthorizedReadMode.SECURITY_TRANSFORMED);
        var reads = new ArrayList<>(List.of(observation));
        var actions = new HashSet<>(Set.of(Privilege.INSERT));
        var request = request(reads, actions);
        reads.clear();
        actions.clear();
        assertThat(request.observedReadPermissions()).containsExactly(observation);
        assertThat(request.requiredWritePrivileges()).containsExactly(Privilege.INSERT);
        assertThat(request.session()).isSameAs(SESSION);
        assertThatThrownBy(() -> request.observedReadPermissions().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> request.requiredWritePrivileges().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void refusesUnprotectedReadEvidenceAndNonWriteActions() {
        assertThatThrownBy(() -> request(List.of(read(AuthorizedReadMode.RAW_READ)), Set.of(Privilege.INSERT)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("protected compilation");
        assertThatThrownBy(() -> request(List.of(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("write actions");
        assertThatThrownBy(() -> request(List.of(), Set.of(Privilege.SELECT)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("write actions");
    }

    private static WriteAuthorityRequest request(List<TablePermission> reads, Set<Privilege> actions) {
        return new WriteAuthorityRequest(TABLE, SESSION, "INSERT", reads, actions, DECLARATION, false);
    }

    private static TablePermission read(AuthorizedReadMode mode) {
        return new TablePermission(TABLE, Set.of("value"), Privilege.SELECT, mode,
                mode == AuthorizedReadMode.SECURITY_TRANSFORMED
                        ? new DeferredAuthorizationScope.PolicySnapshot(Optional.empty(), Map.of("value", Optional.empty()))
                        : DeferredAuthorizationScope.PolicySnapshot.empty());
    }
}
