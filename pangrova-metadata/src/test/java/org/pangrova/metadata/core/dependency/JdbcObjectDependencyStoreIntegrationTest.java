package org.pangrova.metadata.core.dependency;

import java.sql.SQLException;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.api.error.MetastoreException;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcObjectDependencyStoreIntegrationTest extends ContainerTestBase {
    private static final ObjectRef PARENT = new ObjectRef("catalog", "schema", "parent", ObjectKind.TABLE);
    private static final ObjectRef CHILD = new ObjectRef("catalog", "schema", "child", ObjectKind.VIEW);
    private DataSource dataSource;
    private JdbcObjectDependencyStore store;

    @BeforeEach
    void createIsolatedGraph() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("dependency_graph");
        store = new JdbcObjectDependencyStore(dataSource);
        store.recordDependencies(CHILD, Set.of(PARENT));
    }

    @Test
    void canonicalProtectedAndExplicitExternalKindsRoundTripWithoutChangingTheirMeaning() {
        assertThat(store.getParentsOf(CHILD)).containsExactly(PARENT);
        assertThat(store.getParentsOf(CHILD).get(0).kind().isDropProtected()).isTrue();
        var external = new ObjectRef("remote", "schema", "source", ObjectKind.EXTERNAL);
        store.replaceDependenciesForChild(CHILD, Set.of(external));
        assertThat(store.getParentsOf(CHILD)).containsExactly(external);
        assertThat(store.getParentsOf(CHILD).get(0).kind().isDropProtected()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNKNOWN", "table", " TABLE", "TABLE\r\nevent=forged"})
    void corruptParentKindCannotDowngradeDropProtectionToExternal(String kind) throws SQLException {
        corruptKind("parent_kind", kind);
        assertCanonicalFailure(() -> store.getParentsOf(CHILD));
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("SELECT parent_kind FROM pangrova_object_dependencies");
                var rows = statement.executeQuery()) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo(kind);
            assertThat(rows.next()).isFalse();
        }
    }

    @Test
    void corruptChildKindCannotReturnASeeminglyValidCascadeGraph() throws SQLException {
        corruptKind("child_kind", "UNKNOWN");
        assertCanonicalFailure(() -> store.getDependentsOf(PARENT));
        assertCanonicalFailure(() -> store.getTransitiveDependentsOf(PARENT));
    }

    private void corruptKind(String column, String kind) throws SQLException {
        String sql = switch (column) {
            case "parent_kind" -> "UPDATE pangrova_object_dependencies SET parent_kind=?";
            case "child_kind" -> "UPDATE pangrova_object_dependencies SET child_kind=?";
            default -> throw new IllegalArgumentException("Not a dependency kind column");
        };
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(sql)) {
            statement.setString(1, kind);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    private static void assertCanonicalFailure(org.assertj.core.api.ThrowableAssert.ThrowingCallable read) {
        assertThatThrownBy(read).isInstanceOfSatisfying(MetastoreException.class, failure ->
                assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR))
                .hasMessageContaining("not canonical");
    }
}
