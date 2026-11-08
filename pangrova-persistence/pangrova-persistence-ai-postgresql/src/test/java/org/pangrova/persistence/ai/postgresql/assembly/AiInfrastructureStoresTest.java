package org.pangrova.persistence.ai.postgresql.assembly;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiInfrastructureStoresTest {
    @Test
    void ownerClasspathDoesNotContainMetadataRuntimeOrFixtures() {
        var loader = getClass().getClassLoader();
        for (String implementation : new String[]{
                "org.pangrova.metadata.assembly.generation.GenerationStores",
                "org.pangrova.metadata.testinfra.GenerationAuthorityFixtures"}) {
            assertThatThrownBy(() -> loader.loadClass(implementation)).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test
    void compositionCannotInventMissingJobSubmissionProvider() {
        assertThatThrownBy(() -> AiInfrastructureStores.jdbc(new PGSimpleDataSource(), null))
                .isInstanceOf(NullPointerException.class).hasMessage("jobSubmissions must not be null");
    }
}
