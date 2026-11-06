/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.control.store.jdbc.embedding;

import org.pangrova.ai.control.embedding.DimensionMismatchException;
import org.pangrova.ai.control.embedding.EmbeddingModelAlterTarget;
import org.pangrova.ai.control.embedding.EmbeddingRegistryAccessDeniedException;
import org.pangrova.ai.control.embedding.EmbeddingModelRevision;
import org.pangrova.ai.control.embedding.ModelAlreadyExistsException;
import org.pangrova.ai.control.embedding.ModelDropUnavailableException;
import org.pangrova.ai.control.embedding.ModelNotFoundException;
import org.pangrova.ai.control.embedding.ModelUpdateRequest;
import org.pangrova.ai.control.embedding.VersionConflictException;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.testing.testkit.ContainerTestBase;
import javax.sql.DataSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcEmbeddingModelRegistryIntegrationTest extends ContainerTestBase {

    private static final String ADMIN_GROUP = "pangrova_admin";
    private static final String USAGE_GROUP = "pangrova_usage";
    private static final UserIdentity ADMIN =
            new UserIdentity("admin", Set.of("idp-admin-group"), Map.of(), Instant.now());
    private static final UserIdentity USER =
            new UserIdentity("user", Set.of("idp-user-group"), Map.of(), Instant.now());
    private static final UserIdentity ANON =
            new UserIdentity("anon", Set.of(), Map.of(), Instant.now());

    private JdbcEmbeddingModelRegistry registry;
    private DataSource dataSource;
    private JdbcRoleStore roles;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of(ADMIN_GROUP));
        roles.createRole(Role.of(USAGE_GROUP));
        roles.grantRoleToUser(UserRoleAssignment.of(ADMIN.principal(), ADMIN_GROUP));
        roles.grantRoleToUser(UserRoleAssignment.of(USER.principal(), USAGE_GROUP));
        registry = new JdbcEmbeddingModelRegistry(dataSource, ADMIN_GROUP, USAGE_GROUP);
        registry.initSchema();
    }

    @Test
    void createAndFindModel() {
        EmbeddingModelDescriptor descriptor = boundModel("text-embed-v1", 768);
        registry.create(descriptor, ADMIN);

        Optional<EmbeddingModelDescriptor> found = registry.find("text-embed-v1", USER);
        assertTrue(found.isPresent());
        assertEquals("text-embed-v1", found.get().name());
        assertEquals(768, found.get().dimension());
        assertEquals(0, found.get().version());
        assertEquals("model-v7", found.get().deploymentVersion());
        assertEquals("04" + "00".repeat(31), found.get().deploymentSignatureSha256());
    }

    @Test
    void canonicalMigrationRetainsHistoricalRowsWithoutInventingRoleAuthority() throws Exception {
        dataSource = org.pangrova.testing.testkit.postgres.SharedPostgres.isolatedDataSource();
        org.flywaydb.core.Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target("10").load().migrate();
        var unmigrated = new JdbcEmbeddingModelRegistry(dataSource, ADMIN_GROUP, USAGE_GROUP);
        assertThrows(IllegalStateException.class, unmigrated::initSchema);
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement()) {
            statement.execute("INSERT INTO pangrova_embedding_models VALUES ("
                    + "'existing-model', 'openai-compatible', 'https://model.example/v1', "
                    + "768, 'existing-secret-ref', 8, 32, '', '', 3, FALSE)");
        }

        org.flywaydb.core.Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration").load().migrate();
        roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of(ADMIN_GROUP));
        roles.grantRoleToUser(UserRoleAssignment.of(ADMIN.principal(), ADMIN_GROUP));
        registry = new JdbcEmbeddingModelRegistry(dataSource, ADMIN_GROUP, USAGE_GROUP);
        registry.initSchema();
        assertThrows(EmbeddingRegistryAccessDeniedException.class, () -> registry.find("existing-model", ADMIN));
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement();
             var rows = statement.executeQuery(
                     "SELECT deployment_version,deployment_signature_sha256,incarnation_id,admin_role,usage_role,version "
                             + "FROM pangrova_embedding_models WHERE model_name='existing-model'")) {
            assertTrue(rows.next());
            assertEquals("", rows.getString(1));
            assertEquals("", rows.getString(2));
            assertNotNull(rows.getString(3));
            assertNull(rows.getString(4));
            assertNull(rows.getString(5));
            assertEquals(3, rows.getInt(6));
        }
    }

    @Test
    void schemaValidationDoesNotCreateOrUpgradeAnUnmigratedTable() throws Exception {
        var empty = org.pangrova.testing.testkit.postgres.SharedPostgres.isolatedDataSource();
        var uninitialized = new JdbcEmbeddingModelRegistry(empty, ADMIN_GROUP, USAGE_GROUP);
        assertThrows(IllegalStateException.class, uninitialized::initSchema);
        try (var connection = empty.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT to_regclass('pangrova_embedding_models')")) {
            assertTrue(rows.next());
            assertNull(rows.getString(1));
        }
    }

    @Test
    void findReturnsEmptyForUnknownModel() {
        Optional<EmbeddingModelDescriptor> found = registry.find("unknown-model", USER);
        assertTrue(found.isEmpty());
    }

    @Test
    void roleStoreDeletionAndRecreationCannotRestoreModelAdministration() throws Exception {
        registry.create(boundModel("m", 128), ADMIN);
        assertTrue(roles.dropRole(ADMIN_GROUP));
        assertTrue(roles.createRole(Role.of(ADMIN_GROUP)));
        roles.grantRoleToUser(UserRoleAssignment.of(ADMIN.principal(), ADMIN_GROUP));
        assertThrows(EmbeddingRegistryAccessDeniedException.class, () -> registry.prepareAlter("m", ADMIN));
        assertThrows(EmbeddingRegistryAccessDeniedException.class, () -> registry.find("m", ADMIN));
        assertTrue(registry.find("m", USER).isPresent());
        assertModelRoles(null, USAGE_GROUP);
    }

    private void assertModelRoles(String adminRole, String usageRole) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery(
                     "SELECT admin_role,usage_role FROM pangrova_embedding_models WHERE model_name='m'")) {
            assertTrue(rows.next());
            assertEquals(adminRole, rows.getString(1));
            assertEquals(usageRole, rows.getString(2));
        }
    }

    @Test
    void createRejectsNonZeroVersion() {
        EmbeddingModelDescriptor descriptor = model("m", 128, 1);
        assertThrows(IllegalArgumentException.class, () -> registry.create(descriptor, ADMIN));
    }

    @Test
    void createRejectsDuplicateModel() {
        registry.create(model("m", 128), ADMIN);
        assertThrows(ModelAlreadyExistsException.class,
                () -> registry.create(model("m", 128), ADMIN));
    }

    @Test
    void concurrentCreateHasOneWinnerAndStableConflict() throws Exception {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> first = executor.submit(
                    () -> createAfterBarrier(ready, start, "shared-model"));
            Future<Throwable> second = executor.submit(
                    () -> createAfterBarrier(ready, start, "shared-model"));

            ready.await();
            start.countDown();

            assertCreateRaceOutcome(first.get(), second.get());
        } finally {
            executor.shutdownNow();
        }
        assertEquals(1, registry.listAll(ADMIN).size());
    }

    @Test
    void createRequiresAdminGroup() {
        assertThrows(EmbeddingRegistryAccessDeniedException.class,
                () -> registry.create(model("m", 128), USER));
        assertThrows(EmbeddingRegistryAccessDeniedException.class,
                () -> registry.create(model("m", 128), ANON));
    }

    @Test
    void alterUpdatesEndpointWithVersionBump() {
        registry.create(boundModel("m", 128), ADMIN);
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new-endpoint.api/v2"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        EmbeddingModelDescriptor current = registry.find("m", ADMIN).orElseThrow();
        EmbeddingModelDescriptor updated = registry.alter(
                registry.prepareAlter("m", ADMIN),
                replacement(current, update, "model-v8"), ADMIN);

        assertEquals(1, updated.version());
        assertEquals("https://new-endpoint.api/v2", updated.endpoint());
        assertTrue(updated.isDeploymentBound());
        assertEquals("model-v8", updated.deploymentVersion());

        Optional<EmbeddingModelDescriptor> persisted = registry.find("m", USER);
        assertTrue(persisted.isPresent());
        assertEquals(1, persisted.get().version());
        assertTrue(persisted.get().isDeploymentBound());
        assertEquals("model-v8", persisted.get().deploymentVersion());
    }

    @Test
    void alterThrowsOnVersionConflict() {
        registry.create(boundModel("m", 128), ADMIN);
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new.api"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());

        // First alter succeeds with version 0
        EmbeddingModelDescriptor current = registry.find("m", ADMIN).orElseThrow();
        EmbeddingModelDescriptor replacement = replacement(current, update, "model-v8");
        EmbeddingModelAlterTarget target = registry.prepareAlter("m", ADMIN);
        registry.alter(target, replacement, ADMIN);

        // Second alter with stale version 0 should fail
        assertThrows(VersionConflictException.class,
                () -> registry.alter(target, replacement, ADMIN));
    }

    @Test
    void failedDropPreservesModelIncarnationForAlter() {
        registry.create(boundModel("m", 128), ADMIN);
        EmbeddingModelAlterTarget target = registry.prepareAlter("m", ADMIN);
        assertThrows(ModelDropUnavailableException.class,
                () -> registry.drop("m", false, ADMIN));
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://stale.api"), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty());

        registry.alter(target, replacement(target.descriptor(), update, "model-v8"), ADMIN);
        assertEquals("https://stale.api", registry.find("m", ADMIN).orElseThrow().endpoint());
    }

    @Test
    void alterThrowsForUnknownModel() {
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new.api"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        assertThrows(ModelNotFoundException.class,
                () -> registry.alter(
                        new EmbeddingModelAlterTarget(
                                boundModel("unknown", 128), "missing-incarnation"),
                        replacement(boundModel("unknown", 128), update, "model-v8"), ADMIN));
    }

    @Test
    void dropWithoutDependencyFlagFailsClosedAndSurvivesRegistryRestart() {
        registry.create(model("m", 128), ADMIN);
        ModelDropUnavailableException failure = assertThrows(
                ModelDropUnavailableException.class,
                () -> registry.drop("m", false, ADMIN));
        assertEquals("AI-0007", failure.getErrorCode().getCode());

        JdbcEmbeddingModelRegistry restarted = new JdbcEmbeddingModelRegistry(
                dataSource, ADMIN_GROUP, USAGE_GROUP);
        restarted.initSchema();
        assertTrue(restarted.find("m", USER).isPresent());
    }

    @Test
    void dropWithDependentsFails() {
        registry.create(model("m", 128), ADMIN);
        registry.registerDependency("m");

        assertThrows(ModelDropUnavailableException.class,
                () -> registry.drop("m", false, ADMIN));
        assertTrue(registry.find("m", ADMIN).isPresent());
    }

    @Test
    void dropWithCascadeRetainsModelAndDependencyFlag() throws Exception {
        registry.create(model("m", 128), ADMIN);
        registry.registerDependency("m");

        assertThrows(ModelDropUnavailableException.class,
                () -> registry.drop("m", true, ADMIN));
        assertTrue(registry.find("m", ADMIN).isPresent());
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery(
                        "SELECT has_dependents FROM pangrova_embedding_models "
                                + "WHERE model_name='m'")) {
            assertTrue(rows.next());
            assertTrue(rows.getBoolean(1));
        }
    }

    @Test
    void listAllReturnsAllModels() {
        registry.create(model("m1", 128), ADMIN);
        registry.create(model("m2", 256), ADMIN);
        registry.create(model("m3", 512), ADMIN);

        List<EmbeddingModelDescriptor> all = registry.listAll(USER);
        assertEquals(3, all.size());
    }

    @Test
    void listAllReturnsEmptyForAnon() {
        registry.create(model("m1", 128), ADMIN);
        List<EmbeddingModelDescriptor> all = registry.listAll(ANON);
        assertTrue(all.isEmpty());
    }

    @Test
    void validateDimensionPassesForMatchingDimension() {
        registry.create(model("m", 768), ADMIN);
        assertDoesNotThrow(() -> registry.validateDimension("m", 768));
    }

    @Test
    void validateDimensionThrowsForMismatch() {
        registry.create(model("m", 768), ADMIN);
        assertThrows(DimensionMismatchException.class,
                () -> registry.validateDimension("m", 384));
    }

    @Test
    void validateDimensionThrowsForUnknownModel() {
        assertThrows(ModelNotFoundException.class,
                () -> registry.validateDimension("unknown", 128));
    }

    private static EmbeddingModelDescriptor model(String name, int dimension) {
        return model(name, dimension, 0);
    }

    private static EmbeddingModelDescriptor model(String name, int dimension, int version) {
        return new EmbeddingModelDescriptor(
                name,
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://api.example.com/v1/embeddings",
                        "secret-ref-key"),
                new EmbeddingModelDescriptor.EmbeddingContract(dimension, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(32, 100),
                version, EmbeddingModelDescriptor.DeploymentFence.unbound());
    }

    private static EmbeddingModelDescriptor boundModel(String name, int dimension) {
        EmbeddingModelDescriptor descriptor = model(name, dimension);
        return new EmbeddingModelDescriptor(
                descriptor.name(), descriptor.route(), descriptor.contract(), descriptor.limits(),
                descriptor.version(), new EmbeddingModelDescriptor.DeploymentFence(
                        "model-v7", "04" + "00".repeat(31)));
    }

    private static EmbeddingModelDescriptor replacement(
            EmbeddingModelDescriptor current,
            ModelUpdateRequest update,
            String deploymentVersion) {
        EmbeddingModelDescriptor requested = EmbeddingModelRevision.applyUpdate(current, update);
        return new EmbeddingModelDescriptor(
                requested.name(), requested.route(), requested.contract(), requested.limits(),
                requested.version(), new EmbeddingModelDescriptor.DeploymentFence(
                        deploymentVersion, "05" + "00".repeat(31)));
    }

    private Throwable createAfterBarrier(
            CountDownLatch ready, CountDownLatch start, String modelName) {
        ready.countDown();
        try {
            start.await();
            registry.create(model(modelName, 128), ADMIN);
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    private static void assertCreateRaceOutcome(Throwable first, Throwable second) {
        List<Throwable> failures = java.util.stream.Stream.of(first, second)
                .filter(java.util.Objects::nonNull)
                .toList();
        assertEquals(1, failures.size());
        assertInstanceOf(ModelAlreadyExistsException.class, failures.get(0));
    }
}
