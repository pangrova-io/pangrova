package org.pangrova.metadata.store.jdbc.object;

import java.sql.SQLException;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import java.util.stream.Stream;
import static org.assertj.core.api.Assertions.catchThrowable;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.testing.testkit.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.metadata.api.relation.ObjectKind;
import org.pangrova.metadata.api.relation.ObjectRef;

class JdbcObjectDependencyStoreIntegrationTest extends ContainerTestBase
        implements org.pangrova.metadata.testinfra.ObjectDependencyStoreContract {
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

    @Override
    public org.pangrova.metadata.api.relation.ObjectDependencyStore dependencyStore() { return store; }

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

    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedRecordPreservesOriginalGraph(Throwable failure, SQLException rollbackFailure) {
        assertFailedMutation(failure, rollbackFailure, false);
    }

    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedReplacementRestoresDeletedEdges(Throwable failure, SQLException rollbackFailure) {
        assertFailedMutation(failure, rollbackFailure, true);
    }

    private void assertFailedMutation(Throwable failure, SQLException rollbackFailure, boolean replace) {
        var added = new ObjectRef("catalog", "schema", "added", ObjectKind.TABLE);
        var failingStore = new JdbcObjectDependencyStore(JdbcMutationFault.afterMutation(dataSource,
                "INSERT INTO pangrova_object_dependencies ", failure, rollbackFailure));
        Throwable caught = catchThrowable(() -> {
            if (replace) failingStore.replaceDependenciesForChild(CHILD, Set.of(added));
            else failingStore.recordDependencies(CHILD, Set.of(added));
        });
        assertThat(store.getParentsOf(CHILD)).containsExactly(PARENT);
        assertThat(store.getDependentsOf(PARENT)).containsExactly(CHILD);
        assertThat(store.getDependentsOf(added)).isEmpty();
        if (failure instanceof SQLException) {
            assertThat(caught).isInstanceOf(MetastoreException.class).hasCause(failure);
        } else {
            assertThat(caught).isSameAs(failure);
        }
        assertThat(failure.getSuppressed()).containsExactly(
                rollbackFailure == null ? new Throwable[0] : new Throwable[]{rollbackFailure});
    }

    private static Stream<Arguments> transactionFailures() {
        return Stream.of(false, true).flatMap(failRollback -> Stream.of(
                new SQLException("after mutation"), new IllegalStateException("after mutation"),
                new AssertionError("after mutation")).map(failure -> Arguments.of(failure,
                        failRollback ? new SQLException("rollback unavailable") : null)));
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void snapshotFailureRollsBackMutationAndCannotNotify(boolean replace) throws Exception {
        var failure = new SQLException("parent snapshot unavailable");
        DataSource failingSource = (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    Object result = invokeJdbc(dataSource, method, args);
                    if (!(result instanceof java.sql.Connection connection)) return result;
                    return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class<?>[]{java.sql.Connection.class}, (connectionProxy, operation, parameters) -> {
                                if (operation.getName().equals("prepareStatement")
                                        && ((String) parameters[0]).startsWith("SELECT parent_catalog")) {
                                    throw failure;
                                }
                                return invokeJdbc(connection, operation, parameters);
                            });
                });
        var service = new org.pangrova.metadata.core.relation.ObjectDependencyService(
                new JdbcObjectDependencyStore(failingSource));
        var notifications = new java.util.ArrayList<Set<ObjectRef>>();
        service.addMaintenanceHook(new org.pangrova.metadata.api.relation.MetadataMaintenanceHook() {
            @Override
            public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> parents) { notifications.add(parents); }
        });
        var added = new ObjectRef("catalog", "schema", "added", ObjectKind.TABLE);
        assertThatThrownBy(() -> {
            if (replace) service.replaceDependencies(CHILD, Set.of(added));
            else service.recordDependencies(CHILD, Set.of(added));
        }).isInstanceOf(MetastoreException.class).hasCause(failure);
        assertThat(notifications).isEmpty();
        assertThat(store.getParentsOf(CHILD)).containsExactly(PARENT);
        assertThat(store.getDependentsOf(PARENT)).containsExactly(CHILD);
        assertThat(store.getDependentsOf(added)).isEmpty();
    }

    private static Object invokeJdbc(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (java.lang.reflect.InvocationTargetException failure) {
            throw failure.getCause();
        }
    }


    @ParameterizedTest
    @ValueSource(strings = {"record", "replace", "independent"})
    void concurrentWritesPreserveWholeOperationsWithoutBlockingOtherChildren(String mode) throws Exception {
        boolean replace = mode.equals("replace");
        boolean independent = mode.equals("independent");
        var child = new ObjectRef("race", "contract", "child", ObjectKind.VIEW);
        var secondChild = new ObjectRef("race", "contract", independent ? "other" : "child", ObjectKind.TABLE);
        var firstParent = new ObjectRef("race", "contract", "first", ObjectKind.TABLE);
        var secondParent = replace ? new ObjectRef("race", "contract", "second", ObjectKind.TABLE) : firstParent;
        var deleted = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var firstPid = new java.util.concurrent.atomic.AtomicInteger();
        var secondPid = new java.util.concurrent.atomic.AtomicInteger();
        var firstStore = new JdbcObjectDependencyStore(observeWriter(firstPid, deleted, release, true));
        var secondStore = new JdbcObjectDependencyStore(observeWriter(secondPid, deleted, release, false));
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> replace
                    ? firstStore.replaceDependenciesForChild(child, Set.of(firstParent))
                    : firstStore.recordDependencies(child, Set.of(firstParent)));
            assertThat(deleted.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var second = pool.submit(() -> replace
                    ? secondStore.replaceDependenciesForChild(secondChild, Set.of(secondParent))
                    : secondStore.recordDependencies(secondChild, Set.of(secondParent)));
            // Correct serialization may block the second writer. The old implementation
            // instead finishes it now. Observe either outcome without a timing sleep.
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10))
                    .until(() -> second.isDone() || isBlockedBy(secondPid.get(), firstPid.get()));
            if (independent) assertThat(second.isDone()).as("different child must remain writable").isTrue();
            release.countDown();
            assertThat(first.get(10, java.util.concurrent.TimeUnit.SECONDS)).isNotNull();
            assertThat(second.get(10, java.util.concurrent.TimeUnit.SECONDS)).isNotNull();
            var finalParents = store.getParentsOf(child);
            if (replace) {
                assertThat(finalParents).hasSize(1);
                assertThat(finalParents.get(0)).isIn(firstParent, secondParent);
            } else {
                assertThat(finalParents).containsExactly(firstParent);
            }
            if (independent) assertThat(store.getParentsOf(secondChild)).containsExactly(secondParent);
        } finally {
            release.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private DataSource observeWriter(java.util.concurrent.atomic.AtomicInteger backendPid,
            java.util.concurrent.CountDownLatch deleted, java.util.concurrent.CountDownLatch release,
            boolean pauseAfterDelete) {
        return (DataSource) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    Object result = invokeJdbc(dataSource, method, args);
                    if (!(result instanceof java.sql.Connection connection)) return result;
                    try (var statement = connection.createStatement();
                            var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
                        rows.next();
                        backendPid.set(rows.getInt(1));
                    }
                    if (!pauseAfterDelete) return connection;
                    var firstDelete = new java.util.concurrent.atomic.AtomicBoolean(true);
                    return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class<?>[]{java.sql.Connection.class}, (connectionProxy, operation, parameters) -> {
                                Object statement = invokeJdbc(connection, operation, parameters);
                                if (!operation.getName().equals("prepareStatement")
                                        || !((String) parameters[0]).startsWith("DELETE FROM pangrova_object_dependencies")) {
                                    return statement;
                                }
                                return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                                        new Class<?>[]{java.sql.PreparedStatement.class}, (sp, call, values) -> {
                                            Object value = invokeJdbc(statement, call, values);
                                            if (call.getName().equals("executeUpdate") && firstDelete.getAndSet(false)) {
                                                deleted.countDown();
                                                if (!release.await(15, java.util.concurrent.TimeUnit.SECONDS)) {
                                                    throw new SQLException("test writer gate timed out");
                                                }
                                            }
                                            return value;
                                        });
                            });
                });
    }

    private boolean isBlockedBy(int waitingPid, int blockingPid) throws SQLException {
        if (waitingPid == 0 || blockingPid == 0) return false;
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("SELECT ? = ANY(pg_blocking_pids(?))")) {
            statement.setInt(1, blockingPid);
            statement.setInt(2, waitingPid);
            try (var rows = statement.executeQuery()) { rows.next(); return rows.getBoolean(1); }
        }
    }

}
