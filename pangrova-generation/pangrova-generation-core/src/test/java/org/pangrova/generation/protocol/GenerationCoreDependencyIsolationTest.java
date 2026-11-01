package org.pangrova.generation.protocol;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.api.generation.job.GenerationJobStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Codecs and routing must not pull backend or authorization implementations into consumers. */
class GenerationCoreDependencyIsolationTest {
    @Test
    void realCoreClasspathContainsPortsButNoProvidersOrSecurityRuntime() throws Exception {
        var loader = getClass().getClassLoader();
        assertThat(Class.forName(GenerationJobStore.class.getName(), false, loader)).isSameAs(GenerationJobStore.class);
        for (String name : List.of("org.pangrova.persistence.jdbc.generation.job.PostgresGenerationJobStore",
                "org.pangrova.security.authz.deferred.DeferredAuthorization",
                "org.pangrova.metadata.adapter.generation.MetadataGenerationTransactionAuthority",
                "org.pangrova.coordinator.bootstrap.CoordinatorBootstrap", "org.postgresql.Driver")) {
            assertThatThrownBy(() -> Class.forName(name, false, loader)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
