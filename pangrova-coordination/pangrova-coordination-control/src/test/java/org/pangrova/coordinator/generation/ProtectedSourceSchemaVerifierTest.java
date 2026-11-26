package org.pangrova.coordinator.generation;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtectedSourceSchemaVerifierTest {
    private static final QualifiedTableName SOURCE = new QualifiedTableName("lake", "sales", "documents");

    @Test
    void opaqueSnapshotTokenIsPassedToTheConnectorUnchanged() {
        String token = "snapshot:opaque/42";
        var connector = new GeneratedBuildTestConnectors.SnapshotConnector(Map.of(), token, () -> {});
        var catalog = new ConnectorCatalogHandle(new CatalogDescriptor(
                "lake", CatalogType.CONNECTOR, "test", CatalogState.ACTIVE, Map.of(), "", Set.of()), connector);
        var verifier = new ProtectedSourceSchemaVerifier(
                new GeneratedBuildTestConnectors.FixedCatalogRegistry(catalog));

        assertThatCode(() -> verifier.verify(SOURCE, token, session(), GeneratedProtectedInputFixtures.compiled()))
                .doesNotThrowAnyException();
    }

    @Test
    void blankSnapshotTokenIsRejectedBeforeConnectorAccess() {
        var connector = new GeneratedBuildTestConnectors.SnapshotConnector(Map.of(), "41", () -> {});
        var catalog = new ConnectorCatalogHandle(new CatalogDescriptor(
                "lake", CatalogType.CONNECTOR, "test", CatalogState.ACTIVE, Map.of(), "", Set.of()), connector);
        var verifier = new ProtectedSourceSchemaVerifier(
                new GeneratedBuildTestConnectors.FixedCatalogRegistry(catalog));

        assertThatThrownBy(() -> verifier.verify(SOURCE, " ", session(), GeneratedProtectedInputFixtures.compiled()))
                .hasMessageContaining("exact source snapshot token");
    }

    private static ConnectorSession session() {
        return new ConnectorSession() {
            @Override
            public String getUser() {
                return "builder";
            }

            @Override
            public Optional<String> getCatalog() {
                return Optional.of("lake");
            }

            @Override
            public Optional<String> getSchema() {
                return Optional.of("sales");
            }

            @Override
            public Map<String, String> getProperties() {
                return Map.of();
            }

            @Override
            public Optional<ConnectorQueryAuthority> getQueryAuthority() {
                return Optional.empty();
            }
        };
    }
}
