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
package org.pangrova.testing.e2e.ai;

import org.pangrova.coordinator.ai.embedding.ConcurrentMapEmbeddingModelRegistry;
import org.pangrova.coordinator.ai.embedding.DimensionMismatchException;
import org.pangrova.coordinator.ai.embedding.EmbeddingRegistryAccessDeniedException;
import org.pangrova.coordinator.ai.embedding.ModelAlreadyExistsException;
import org.pangrova.coordinator.ai.embedding.ModelInUseException;
import org.pangrova.coordinator.ai.embedding.ModelNotFoundException;
import org.pangrova.coordinator.ai.embedding.ModelUpdateRequest;
import org.pangrova.coordinator.ai.embedding.VersionConflictException;
import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E tests for the embedding model DDL lifecycle through
 * {@link ConcurrentMapEmbeddingModelRegistry}.
 *
 * <p>Exercises CREATE, DROP, ALTER, LIST operations with RBAC enforcement,
 * version tracking, and error handling using real implementations.</p>
 */
@DisplayName("Embedding Model DDL E2E")
class EmbeddingModelDdlE2ETest {

    private static final String ADMIN_GROUP = "pangrova-admins";
    private static final String USAGE_GROUP = "pangrova-users";

    private ConcurrentMapEmbeddingModelRegistry registry;
    private UserIdentity adminUser;
    private UserIdentity regularUser;
    private UserIdentity unprivilegedUser;

    @BeforeEach
    void setUp() {
        registry = new ConcurrentMapEmbeddingModelRegistry(ADMIN_GROUP, USAGE_GROUP);
        adminUser = new UserIdentity("admin@pangrova.io",
                Set.of(ADMIN_GROUP, USAGE_GROUP), Map.of(), Instant.now());
        regularUser = new UserIdentity("user@pangrova.io",
                Set.of(USAGE_GROUP), Map.of(), Instant.now());
        unprivilegedUser = new UserIdentity("guest@pangrova.io",
                Set.of(), Map.of(), Instant.now());
    }

    private EmbeddingModelDescriptor testDescriptor(String name, int dimension) {
        return new EmbeddingModelDescriptor(
                name,
                "openai-compatible",
                "http://localhost:11434/api/embed",
                dimension,
                "secret-ref-" + name,
                32,
                10,
                "search_query: ",
                "search_document: ",
                0);
    }

    @Nested
    @DisplayName("Create Model")
    class CreateModel {

        @Test
        @DisplayName("Admin can create a model and retrieve it")
        void createAndRetrieve() {
            EmbeddingModelDescriptor descriptor = testDescriptor("text-embed-3", 1536);

            registry.create(descriptor, adminUser);

            Optional<EmbeddingModelDescriptor> found = registry.find("text-embed-3", adminUser);
            assertThat(found).isPresent();
            assertThat(found.get().name()).isEqualTo("text-embed-3");
            assertThat(found.get().provider()).isEqualTo("openai-compatible");
            assertThat(found.get().dimension()).isEqualTo(1536);
            assertThat(found.get().version()).isEqualTo(0);
        }

        @Test
        @DisplayName("Create with non-zero version is rejected")
        void rejectsNonZeroVersion() {
            EmbeddingModelDescriptor descriptor = new EmbeddingModelDescriptor(
                    "bad-version", "openai-compatible",
                    "http://localhost/embed", 768, "secret",
                    32, 10, "", "", 5);

            assertThatThrownBy(() -> registry.create(descriptor, adminUser))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("version");
        }
    }

    @Nested
    @DisplayName("Drop Model")
    class DropModel {

        @Test
        @DisplayName("Drop removes model from registry")
        void dropRemovesModel() {
            registry.create(testDescriptor("to-drop", 384), adminUser);

            assertThat(registry.find("to-drop", adminUser)).isPresent();

            registry.drop("to-drop", false, adminUser);

            // Admin user has usage privilege, so find returns empty (not exception)
            assertThat(registry.find("to-drop", adminUser)).isEmpty();
        }

        @Test
        @DisplayName("Drop non-existent model throws ModelNotFoundException")
        void dropNonExistent() {
            assertThatThrownBy(() -> registry.drop("ghost-model", false, adminUser))
                    .isInstanceOf(ModelNotFoundException.class)
                    .hasMessageContaining("ghost-model");
        }

        @Test
        @DisplayName("Drop model in use without cascade throws ModelInUseException")
        void dropInUseWithoutCascade() {
            registry.create(testDescriptor("in-use-model", 768), adminUser);
            registry.registerDependency("in-use-model");

            assertThatThrownBy(() -> registry.drop("in-use-model", false, adminUser))
                    .isInstanceOf(ModelInUseException.class)
                    .hasMessageContaining("in-use-model");
        }

        @Test
        @DisplayName("Drop model in use with cascade succeeds")
        void dropInUseWithCascade() {
            registry.create(testDescriptor("cascade-model", 768), adminUser);
            registry.registerDependency("cascade-model");

            registry.drop("cascade-model", true, adminUser);

            assertThat(registry.find("cascade-model", adminUser)).isEmpty();
        }
    }

    @Nested
    @DisplayName("List Models")
    class ListModels {

        @Test
        @DisplayName("List returns all created models")
        void listAllModels() {
            registry.create(testDescriptor("model-a", 256), adminUser);
            registry.create(testDescriptor("model-b", 512), adminUser);
            registry.create(testDescriptor("model-c", 1024), adminUser);

            List<EmbeddingModelDescriptor> all = registry.listAll(adminUser);

            assertThat(all).hasSize(3);
            assertThat(all.stream().map(EmbeddingModelDescriptor::name).toList())
                    .containsExactlyInAnyOrder("model-a", "model-b", "model-c");
        }

        @Test
        @DisplayName("List returns empty for unprivileged user")
        void listEmptyForUnprivileged() {
            registry.create(testDescriptor("hidden-model", 128), adminUser);

            List<EmbeddingModelDescriptor> result = registry.listAll(unprivilegedUser);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("List returns models for user with USAGE privilege")
        void listForUsageUser() {
            registry.create(testDescriptor("visible-model", 768), adminUser);

            List<EmbeddingModelDescriptor> result = registry.listAll(regularUser);

            assertThat(result).hasSize(1);
            assertThat(result.get(0).name()).isEqualTo("visible-model");
        }
    }

    @Nested
    @DisplayName("Duplicate Model")
    class DuplicateModel {

        @Test
        @DisplayName("Creating duplicate model throws ModelAlreadyExistsException")
        void duplicateThrows() {
            registry.create(testDescriptor("dup-model", 384), adminUser);

            assertThatThrownBy(() -> registry.create(testDescriptor("dup-model", 512), adminUser))
                    .isInstanceOf(ModelAlreadyExistsException.class)
                    .hasMessageContaining("dup-model");
        }

        @Test
        @DisplayName("After dropping, same name can be re-created")
        void reCreateAfterDrop() {
            registry.create(testDescriptor("recycled", 256), adminUser);
            registry.drop("recycled", false, adminUser);

            registry.create(testDescriptor("recycled", 512), adminUser);

            Optional<EmbeddingModelDescriptor> found = registry.find("recycled", adminUser);
            assertThat(found).isPresent();
            assertThat(found.get().dimension()).isEqualTo(512);
        }
    }

    @Nested
    @DisplayName("Alter Model")
    class AlterModel {

        @Test
        @DisplayName("Alter updates endpoint and increments version")
        void alterEndpoint() {
            registry.create(testDescriptor("alter-me", 768), adminUser);

            ModelUpdateRequest update = new ModelUpdateRequest(
                    Optional.of("http://new-endpoint:9090/embed"),
                    Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty());

            EmbeddingModelDescriptor updated = registry.alter("alter-me", update, 0, adminUser);

            assertThat(updated.endpoint()).isEqualTo("http://new-endpoint:9090/embed");
            assertThat(updated.version()).isEqualTo(1);
            assertThat(updated.dimension()).isEqualTo(768);
        }

        @Test
        @DisplayName("Alter with wrong version throws VersionConflictException")
        void alterVersionConflict() {
            registry.create(testDescriptor("versioned", 384), adminUser);

            ModelUpdateRequest update = new ModelUpdateRequest(
                    Optional.of("http://changed/embed"),
                    Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty());

            assertThatThrownBy(() -> registry.alter("versioned", update, 99, adminUser))
                    .isInstanceOf(VersionConflictException.class)
                    .hasMessageContaining("expected version 99");
        }

        @Test
        @DisplayName("Alter non-existent model throws ModelNotFoundException")
        void alterNonExistent() {
            ModelUpdateRequest update = new ModelUpdateRequest(
                    Optional.of("http://endpoint/embed"),
                    Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty());

            assertThatThrownBy(() -> registry.alter("nonexistent", update, 0, adminUser))
                    .isInstanceOf(ModelNotFoundException.class);
        }

        @Test
        @DisplayName("Alter with no updates throws IllegalArgumentException")
        void alterNoUpdates() {
            registry.create(testDescriptor("no-op", 256), adminUser);

            ModelUpdateRequest emptyUpdate = new ModelUpdateRequest(
                    Optional.empty(), Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty());

            assertThatThrownBy(() -> registry.alter("no-op", emptyUpdate, 0, adminUser))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("No fields to update");
        }

        @Test
        @DisplayName("Multiple sequential alters increment version correctly")
        void sequentialAlters() {
            registry.create(testDescriptor("multi-alter", 768), adminUser);

            ModelUpdateRequest update1 = new ModelUpdateRequest(
                    Optional.of("http://v1/embed"),
                    Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty());
            EmbeddingModelDescriptor v1 = registry.alter("multi-alter", update1, 0, adminUser);
            assertThat(v1.version()).isEqualTo(1);

            ModelUpdateRequest update2 = new ModelUpdateRequest(
                    Optional.empty(), Optional.empty(),
                    Optional.of(64),
                    Optional.empty(), Optional.empty(), Optional.empty());
            EmbeddingModelDescriptor v2 = registry.alter("multi-alter", update2, 1, adminUser);
            assertThat(v2.version()).isEqualTo(2);
            assertThat(v2.maxBatchSize()).isEqualTo(64);
            assertThat(v2.endpoint()).isEqualTo("http://v1/embed");
        }
    }

    @Nested
    @DisplayName("RBAC Enforcement")
    class RbacEnforcement {

        @Test
        @DisplayName("Non-admin cannot create model")
        void regularUserCannotCreate() {
            assertThatThrownBy(
                    () -> registry.create(testDescriptor("forbidden", 256), regularUser))
                    .isInstanceOf(EmbeddingRegistryAccessDeniedException.class)
                    .hasMessageContaining("user@pangrova.io")
                    .hasMessageContaining("CREATE");
        }

        @Test
        @DisplayName("Non-admin cannot drop model")
        void regularUserCannotDrop() {
            registry.create(testDescriptor("admin-only", 384), adminUser);

            assertThatThrownBy(() -> registry.drop("admin-only", false, regularUser))
                    .isInstanceOf(EmbeddingRegistryAccessDeniedException.class)
                    .hasMessageContaining("DROP");
        }

        @Test
        @DisplayName("Non-admin cannot alter model")
        void regularUserCannotAlter() {
            registry.create(testDescriptor("alter-denied", 512), adminUser);

            ModelUpdateRequest update = new ModelUpdateRequest(
                    Optional.of("http://hacker/embed"),
                    Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty());

            assertThatThrownBy(() -> registry.alter("alter-denied", update, 0, regularUser))
                    .isInstanceOf(EmbeddingRegistryAccessDeniedException.class)
                    .hasMessageContaining("ALTER");
        }

        @Test
        @DisplayName("User with USAGE group can find model")
        void usageUserCanFind() {
            registry.create(testDescriptor("readable", 768), adminUser);

            Optional<EmbeddingModelDescriptor> found = registry.find("readable", regularUser);

            assertThat(found).isPresent();
            assertThat(found.get().name()).isEqualTo("readable");
        }

        @Test
        @DisplayName("Unprivileged user cannot find model")
        void unprivilegedCannotFind() {
            registry.create(testDescriptor("secret-model", 384), adminUser);

            assertThatThrownBy(() -> registry.find("secret-model", unprivilegedUser))
                    .isInstanceOf(EmbeddingRegistryAccessDeniedException.class)
                    .hasMessageContaining("FIND");
        }

        @Test
        @DisplayName("Admin user has implicit usage privilege")
        void adminHasUsagePrivilege() {
            registry.create(testDescriptor("admin-readable", 256), adminUser);

            Optional<EmbeddingModelDescriptor> found = registry.find("admin-readable", adminUser);
            assertThat(found).isPresent();

            List<EmbeddingModelDescriptor> all = registry.listAll(adminUser);
            assertThat(all).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Dimension Validation")
    class DimensionValidation {

        @Test
        @DisplayName("Matching dimension passes validation")
        void matchingDimension() {
            registry.create(testDescriptor("dim-check", 768), adminUser);

            registry.validateDimension("dim-check", 768);
            // No exception means success
        }

        @Test
        @DisplayName("Mismatching dimension throws DimensionMismatchException")
        void mismatchingDimension() {
            registry.create(testDescriptor("dim-mismatch", 768), adminUser);

            assertThatThrownBy(() -> registry.validateDimension("dim-mismatch", 384))
                    .isInstanceOf(DimensionMismatchException.class)
                    .hasMessageContaining("768")
                    .hasMessageContaining("384");
        }

        @Test
        @DisplayName("Validating non-existent model throws ModelNotFoundException")
        void validateNonExistent() {
            assertThatThrownBy(() -> registry.validateDimension("no-such-model", 768))
                    .isInstanceOf(ModelNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Registry Construction")
    class RegistryConstruction {

        @Test
        @DisplayName("Null admin group is rejected")
        void nullAdminGroup() {
            assertThatThrownBy(() -> new ConcurrentMapEmbeddingModelRegistry(null, USAGE_GROUP))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("adminGroup");
        }

        @Test
        @DisplayName("Blank usage group is rejected")
        void blankUsageGroup() {
            assertThatThrownBy(() -> new ConcurrentMapEmbeddingModelRegistry(ADMIN_GROUP, "  "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("usageGroup");
        }
    }
}
