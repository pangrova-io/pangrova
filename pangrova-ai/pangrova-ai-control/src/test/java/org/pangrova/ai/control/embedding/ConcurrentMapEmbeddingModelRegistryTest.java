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
package org.pangrova.ai.control.embedding;

import org.pangrova.ai.control.exception.DimensionMismatchException;
import org.pangrova.ai.control.exception.EmbeddingRegistryAccessDeniedException;
import org.pangrova.ai.control.exception.ModelAlreadyExistsException;
import org.pangrova.ai.control.exception.ModelDropUnavailableException;
import org.pangrova.ai.control.exception.ModelNotFoundException;
import org.pangrova.ai.control.exception.VersionConflictException;

import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConcurrentMapEmbeddingModelRegistryTest {

    private static final String ADMIN_GROUP = "embedding_admin";
    private static final String USAGE_GROUP = "embedding_user";

    private ConcurrentMapEmbeddingModelRegistry registry;
    private UserIdentity adminUser;
    private UserIdentity usageUser;
    private UserIdentity unprivilegedUser;

    @BeforeEach
    void setUp() {
        registry = new ConcurrentMapEmbeddingModelRegistry(ADMIN_GROUP, USAGE_GROUP);
        adminUser = new UserIdentity("admin", Set.of(ADMIN_GROUP), null, Instant.now());
        usageUser = new UserIdentity("reader", Set.of(USAGE_GROUP), null, Instant.now());
        unprivilegedUser = new UserIdentity("nobody", Set.of("other_group"), null, Instant.now());
    }

    @Test
    void createStoresModelAndFindReturnsIt() {
        EmbeddingModelDescriptor descriptor = testDescriptor("bge_m3", 1024);
        registry.create(descriptor, adminUser);

        Optional<EmbeddingModelDescriptor> found = registry.find("bge_m3", adminUser);
        assertThat(found).isPresent();
        assertThat(found.get().name()).isEqualTo("bge_m3");
        assertThat(found.get().dimension()).isEqualTo(1024);
        assertThat(found.get().version()).isEqualTo(0);
    }

    @Test
    void createDuplicateThrowsModelAlreadyExists() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);

        assertThatThrownBy(() -> registry.create(testDescriptor("bge_m3", 1024), adminUser))
                .isInstanceOf(ModelAlreadyExistsException.class)
                .hasMessageContaining("bge_m3");
    }

    @Test
    void createWithNonZeroVersionThrows() {
        EmbeddingModelDescriptor badVersion = new EmbeddingModelDescriptor(
                "model",
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai", "https://endpoint", "vault://key"),
                new EmbeddingModelDescriptor.EmbeddingContract(768, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(64, 100),
                5, EmbeddingModelDescriptor.DeploymentFence.unbound());

        assertThatThrownBy(() -> registry.create(badVersion, adminUser))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version must be 0");
    }

    @Test
    void createWithoutAdminThrowsAccessDenied() {
        assertThatThrownBy(() -> registry.create(testDescriptor("model", 768), usageUser))
                .isInstanceOf(EmbeddingRegistryAccessDeniedException.class)
                .hasMessageContaining("reader")
                .hasMessageContaining("CREATE");
    }

    @Test
    void alterIncrementsVersionAndUpdatesFields() {
        registry.create(boundDescriptor("bge_m3", 1024), adminUser);

        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new-endpoint/v2"),
                Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());

        EmbeddingModelDescriptor current = registry.find("bge_m3", adminUser).orElseThrow();
        EmbeddingModelDescriptor updated = registry.alter(
                registry.prepareAlter("bge_m3", adminUser),
                replacement(current, update), adminUser);
        assertThat(updated.version()).isEqualTo(1);
        assertThat(updated.endpoint()).isEqualTo("https://new-endpoint/v2");
        assertThat(updated.dimension()).isEqualTo(1024);
        assertThat(updated.isDeploymentBound()).isTrue();
    }

    @Test
    void alterWithWrongVersionThrowsConflict() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);

        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new"), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        EmbeddingModelDescriptor current = registry.find("bge_m3", adminUser).orElseThrow();
        assertThatThrownBy(() -> registry.alter(
                new EmbeddingModelAlterTarget(current, "stale-incarnation"),
                replacement(current, update), adminUser))
                .isInstanceOf(VersionConflictException.class)
                .hasMessageContaining("expected version 0")
                .hasMessageContaining("current is 0");
    }

    @Test
    void failedDropPreservesOriginalAlterTarget() {
        registry.create(boundDescriptor("bge_m3", 1024), adminUser);
        EmbeddingModelAlterTarget target = registry.prepareAlter("bge_m3", adminUser);
        assertThatThrownBy(() -> registry.drop("bge_m3", false, adminUser))
                .isInstanceOf(ModelDropUnavailableException.class);
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://stale-endpoint"), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        registry.alter(target, replacement(target.descriptor(), update), adminUser);
        assertThat(registry.find("bge_m3", adminUser).orElseThrow().endpoint())
                .isEqualTo("https://stale-endpoint");
    }

    @Test
    void alterNonExistentModelThrowsNotFound() {
        ModelUpdateRequest update = new ModelUpdateRequest(
                Optional.of("https://new"), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> registry.alter(
                new EmbeddingModelAlterTarget(
                        boundDescriptor("missing", 1024), "missing-incarnation"),
                replacement(boundDescriptor("missing", 1024), update), adminUser))
                .isInstanceOf(ModelNotFoundException.class);
    }

    @Test
    void dropWithoutDependencyFlagFailsClosed() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);
        assertThatThrownBy(() -> registry.drop("bge_m3", false, adminUser))
                .isInstanceOf(ModelDropUnavailableException.class);

        Optional<EmbeddingModelDescriptor> found = registry.find("bge_m3", adminUser);
        assertThat(found).isPresent();
    }

    @Test
    void dropWithDependentsWithoutCascadeStillFailsClosed() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);
        registry.registerDependency("bge_m3");

        assertThatThrownBy(() -> registry.drop("bge_m3", false, adminUser))
                .isInstanceOf(ModelDropUnavailableException.class)
                .hasMessageContaining("durable catalog authority");
    }

    @Test
    void dropWithCascadeRetainsModelAndDependency() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);
        registry.registerDependency("bge_m3");

        assertThatThrownBy(() -> registry.drop("bge_m3", true, adminUser))
                .isInstanceOf(ModelDropUnavailableException.class);
        assertThat(registry.find("bge_m3", adminUser)).isPresent();
    }

    @Test
    void findWithUsagePrivilegeSucceeds() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);

        Optional<EmbeddingModelDescriptor> found = registry.find("bge_m3", usageUser);
        assertThat(found).isPresent();
    }

    @Test
    void findWithoutPrivilegeThrowsAccessDenied() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);

        assertThatThrownBy(() -> registry.find("bge_m3", unprivilegedUser))
                .isInstanceOf(EmbeddingRegistryAccessDeniedException.class);
    }

    @Test
    void listAllReturnsAllModelsForPrivilegedUser() {
        registry.create(testDescriptor("model_a", 768), adminUser);
        registry.create(testDescriptor("model_b", 1024), adminUser);

        List<EmbeddingModelDescriptor> all = registry.listAll(usageUser);
        assertThat(all).hasSize(2);
    }

    @Test
    void listAllReturnsEmptyForUnprivilegedUser() {
        registry.create(testDescriptor("model_a", 768), adminUser);

        List<EmbeddingModelDescriptor> all = registry.listAll(unprivilegedUser);
        assertThat(all).isEmpty();
    }

    @Test
    void validateDimensionPassesWhenMatching() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);
        registry.validateDimension("bge_m3", 1024);
    }

    @Test
    void validateDimensionThrowsMismatch() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);

        assertThatThrownBy(() -> registry.validateDimension("bge_m3", 768))
                .isInstanceOf(DimensionMismatchException.class)
                .hasMessageContaining("1024")
                .hasMessageContaining("768");
    }

    @Test
    void validateDimensionThrowsNotFoundForMissingModel() {
        assertThatThrownBy(() -> registry.validateDimension("missing", 768))
                .isInstanceOf(ModelNotFoundException.class);
    }

    @Test
    void adminGroupImpliesUsagePrivilege() {
        registry.create(testDescriptor("bge_m3", 1024), adminUser);

        Optional<EmbeddingModelDescriptor> found = registry.find("bge_m3", adminUser);
        assertThat(found).isPresent();

        List<EmbeddingModelDescriptor> all = registry.listAll(adminUser);
        assertThat(all).hasSize(1);
    }

    private static EmbeddingModelDescriptor testDescriptor(String name, int dimension) {
        return new EmbeddingModelDescriptor(
                name,
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://api.example.com/v1/embeddings",
                        "vault://secrets/api-key"),
                new EmbeddingModelDescriptor.EmbeddingContract(
                        dimension, "query: ", "passage: "),
                new EmbeddingModelDescriptor.ServingLimits(64, 100),
                0, EmbeddingModelDescriptor.DeploymentFence.unbound());
    }

    private static EmbeddingModelDescriptor boundDescriptor(String name, int dimension) {
        EmbeddingModelDescriptor descriptor = testDescriptor(name, dimension);
        return new EmbeddingModelDescriptor(
                descriptor.name(), descriptor.route(), descriptor.contract(), descriptor.limits(),
                descriptor.version(), new EmbeddingModelDescriptor.DeploymentFence(
                        "model-v1", "01" + "00".repeat(31)));
    }

    private static EmbeddingModelDescriptor replacement(
            EmbeddingModelDescriptor current, ModelUpdateRequest update) {
        EmbeddingModelDescriptor requested = EmbeddingModelRevision.applyUpdate(current, update);
        return new EmbeddingModelDescriptor(
                requested.name(), requested.route(), requested.contract(), requested.limits(),
                requested.version(), new EmbeddingModelDescriptor.DeploymentFence(
                        "model-v2", "02" + "00".repeat(31)));
    }
}
