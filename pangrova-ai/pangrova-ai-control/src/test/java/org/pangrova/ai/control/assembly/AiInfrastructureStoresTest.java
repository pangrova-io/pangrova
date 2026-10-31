package org.pangrova.ai.control.assembly;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiInfrastructureStoresTest {
    @Test
    void ownerClasspathDoesNotContainMetadataRuntimeOrFixtures() {
        var loader = getClass().getClassLoader();
        for (String implementation : new String[]{
                "org.pangrova.metadata.assembly.databuild.DataBuildStores",
                "org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner",
                "org.pangrova.metadata.testinfra.DataBuildAuthorityFixtures"}) {
            assertThatThrownBy(() -> loader.loadClass(implementation)).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test
    void compositionCannotInventMissingCrossDomainAuthority() {
        assertThatThrownBy(() -> AiInfrastructureStores.jdbc(new PGSimpleDataSource(), null))
                .isInstanceOf(NullPointerException.class).hasMessage("authority must not be null");
    }
}
