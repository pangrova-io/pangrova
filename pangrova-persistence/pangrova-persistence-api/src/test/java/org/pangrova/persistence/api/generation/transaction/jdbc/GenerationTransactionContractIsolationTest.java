package org.pangrova.persistence.api.generation.transaction.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Loads borrowed-connection contracts on an explicitly minimal, isolated classpath. */
class GenerationTransactionContractIsolationTest {
    @Test
    void jdbcAuthorityPortLinksWithoutDomainImplementationsOrDatabaseDrivers() throws Exception {
        URL[] locations = {
                GenerationTransactionAuthority.class.getProtectionDomain().getCodeSource().getLocation(),
                GenerationDispatchSpec.class.getProtectionDomain().getCodeSource().getLocation(),
                DigestUtils.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of("GenerationTransactionAuthority", "GenerationPublicationCandidates",
                    "GenerationJobSubmissionTransactions", "GenerationExecutionTransactionFence",
                    "GenerationTransactionInputs")) {
                var port = loader.loadClass("org.pangrova.persistence.api.generation.transaction.jdbc." + name);
                assertThat(port.isInterface()).isTrue();
                assertThat(port.getClassLoader()).isSameAs(loader);
                assertThat(port.getMethods()).isNotEmpty();
            }
            for (String missing : List.of("org.postgresql.Driver", "com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.persistence.jdbc.generation.job.PostgresGenerationJobStore",
                    "org.pangrova.generation.protocol.GenerationStageGraphCodec",
                    "org.pangrova.security.core.authz.deferred.DeferredAuthorization",
                    "org.pangrova.metadata.adapter.generation.MetadataGenerationTransactionAuthority")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
