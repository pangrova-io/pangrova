package org.pangrova.spi.generation;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.generation.execution.GenerationStageGraph;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.generation.job.GenerationJobKey;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the real SPI dependency closure, without filtering the application classloader. */
class GenerationValueIsolationTest {
    @Test
    void valuesLinkWithoutStoragePortsCodecsOrDomainRuntime() {
        var loader = getClass().getClassLoader();
        for (Class<?> type : List.of(GenerationStageGraph.class, GenerationDispatchSpec.class,
                GenerationJobKey.class)) {
            assertThat(type.getConstructors()).isNotEmpty();
            assertThat(type.getRecordComponents()).isNotEmpty();
            assertThat(type.getMethods()).isNotEmpty();
        }
        assertThat(new GenerationJobKey("tenant", "job").executionId()).isNotBlank();
        for (String name : List.of(
                "org.pangrova.persistence.api.generation.job.GenerationJobStore",
                "org.pangrova.persistence.api.generation.transaction.jdbc.GenerationTransactionAuthority",
                "org.pangrova.persistence.jdbc.generation.job.PostgresGenerationJobStore",
                "org.pangrova.generation.protocol.GenerationStageGraphCodec",
                "org.pangrova.generation.assembly.GenerationStageGraphFactory",
                "org.pangrova.security.authz.deferred.DeferredAuthorization")) {
            assertThatThrownBy(() -> Class.forName(name, false, loader))
                    .as("SPI must not load %s", name).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
