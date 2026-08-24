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
package org.pangrova.coordinator.ai.embedding;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class JdbcEmbeddingModelRegistryIntegrationTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() { PG.start(); }

    @AfterAll
    static void stopContainer() { PG.stop(); }

    private static final String ADMIN_GROUP = "pangrova_admin";
    private static final String USAGE_GROUP = "pangrova_usage";
    private static final UserIdentity ADMIN =
            new UserIdentity("admin", Set.of(ADMIN_GROUP), Map.of(), Instant.now());
    private static final UserIdentity USER =
            new UserIdentity("user", Set.of(USAGE_GROUP), Map.of(), Instant.now());
    private static final UserIdentity ANON =
            new UserIdentity("anon", Set.of(), Map.of(), Instant.now());

    private JdbcEmbeddingModelRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        // Drop schema so each test starts clean
        try (var conn = ds.getConnection()) {
            conn.createStatement().execute("DROP TABLE IF EXISTS pangrova_embedding_models");
        }
        registry = new JdbcEmbeddingModelRegistry(ds, ADMIN_GROUP, USAGE_GROUP);
        registry.initSchema();
    }

    @Test
    void createAndFindModel() {
        EmbeddingModelDescriptor descriptor = model("text-embed-v1", 768);
        registry.create(descriptor, ADMIN);

        Optional<EmbeddingModelDescriptor> found = registry.find("text-embed-v1", USER);
        assertTrue(found.isPresent());
        assertEquals("text-embed-v1", found.get().name());
        assertEquals(768, found.get().dimension());
        assertEquals(0, found.get().version());
    }

    @Test
    void findReturnsEmptyForUnknownModel() {
        Optional<EmbeddingModelDescriptor> found = registry.find("unknown-model", USER);
        assertTrue(found.isEmpty());
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
    void createRequiresAdminGroup() {
        assertThrows(EmbeddingRegistryAccessDeniedException.class,
                () -> registry.create(model("m", 128), USER));
        assertThrows(EmbeddingRegistryAccessDeniedException.class,
                () -> registry.create(model("m", 128), ANON));
    }

    @Test
    void alterUpdatesEndpointWithVersionBump() {
        registry.create(model("m", 128), ADMIN);
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new-endpoint.api/v2"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        EmbeddingModelDescriptor updated = registry.alter("m", update, 0, ADMIN);

        assertEquals(1, updated.version());
        assertEquals("https://new-endpoint.api/v2", updated.endpoint());

        Optional<EmbeddingModelDescriptor> persisted = registry.find("m", USER);
        assertTrue(persisted.isPresent());
        assertEquals(1, persisted.get().version());
    }

    @Test
    void alterThrowsOnVersionConflict() {
        registry.create(model("m", 128), ADMIN);
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new.api"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());

        // First alter succeeds with version 0
        registry.alter("m", update, 0, ADMIN);

        // Second alter with stale version 0 should fail
        assertThrows(VersionConflictException.class,
                () -> registry.alter("m", update, 0, ADMIN));
    }

    @Test
    void alterThrowsForUnknownModel() {
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new.api"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        assertThrows(ModelNotFoundException.class,
                () -> registry.alter("unknown", update, 0, ADMIN));
    }

    @Test
    void dropRemovesModel() {
        registry.create(model("m", 128), ADMIN);
        registry.drop("m", false, ADMIN);

        Optional<EmbeddingModelDescriptor> found = registry.find("m", USER);
        assertTrue(found.isEmpty());
    }

    @Test
    void dropWithDependentsFails() {
        registry.create(model("m", 128), ADMIN);
        registry.registerDependency("m");

        assertThrows(ModelInUseException.class,
                () -> registry.drop("m", false, ADMIN));
    }

    @Test
    void dropWithCascadeIgnoresDependents() {
        registry.create(model("m", 128), ADMIN);
        registry.registerDependency("m");

        assertDoesNotThrow(() -> registry.drop("m", true, ADMIN));
        assertTrue(registry.find("m", ADMIN).isEmpty());
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
                name, "openai-compatible",
                "https://api.example.com/v1/embeddings",
                dimension, "secret-ref-key",
                32, 100, "", "", version);
    }
}
