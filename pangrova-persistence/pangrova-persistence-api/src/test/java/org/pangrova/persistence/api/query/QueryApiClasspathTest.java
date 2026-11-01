package org.pangrova.persistence.api.query;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Stable;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;
import org.slf4j.LoggerFactory;
import org.pangrova.spi.observability.event.QueryState;
import org.pangrova.persistence.api.query.audit.QueryAuditStore;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Links the entire API artifact without Query services, JDBC stores or provider libraries. */
class QueryApiClasspathTest {
    @Test
    void everyExportedTypeLinksAndAnAnchorConstructsOnTheContractClasspath() throws Exception {
        URL api = QueryAuditStore.class.getProtectionDomain().getCodeSource().getLocation();
        URL[] locations = {api,
                Stable.class.getProtectionDomain().getCodeSource().getLocation(),
                QueryState.class.getProtectionDomain().getCodeSource().getLocation(),
                ObjectStoreAccessor.class.getProtectionDomain().getCodeSource().getLocation(),
                LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation()};
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (String absent : List.of("org.pangrova.query.audit.QueryAuditService",
                    "org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionAnchorStore",
                    "org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionTransactionFence",
                    "org.postgresql.Driver",
                    "org.pangrova.persistence.jdbc.security.transaction.AuthorizationTransactionGate", "com.fasterxml.jackson.databind.ObjectMapper",
                    "com.github.benmanes.caffeine.cache.Caffeine")) {
                assertThatThrownBy(() -> loader.loadClass(absent)).isInstanceOf(ClassNotFoundException.class);
            }
            Path classes = Path.of(api.toURI());
            var linked = new ArrayList<String>();
            try (var paths = Files.walk(classes.resolve("org/pangrova/persistence/api"))) {
                for (Path path : paths.filter(p -> p.toString().endsWith(".class"))
                        .filter(p -> !p.getFileName().toString().equals("package-info.class")).toList()) {
                    String name = classes.relativize(path).toString().replace(java.io.File.separatorChar, '.');
                    name = name.substring(0, name.length() - ".class".length());
                    Class<?> type = Class.forName(name, true, loader);
                    type.getMethods();
                    type.getConstructors();
                    type.getFields();
                    type.getRecordComponents();
                    linked.add(name);
                }
            }
            assertThat(linked).contains("org.pangrova.persistence.api.query.audit.QueryAuditStore",
                    "org.pangrova.persistence.api.query.transaction.ExecutionTransactionFence",
                    "org.pangrova.persistence.api.query.state.QueryStateJournal",
                    "org.pangrova.persistence.api.query.transaction.WriteCommitQuarantineStore");
            Class<?> anchor = loader.loadClass("org.pangrova.spi.query.coordination.ExecutionAnchor");
            Instant start = Instant.parse("2026-10-31T00:00:00Z");
            Object value = anchor.getConstructor(String.class, String.class, String.class,
                    long.class, Instant.class, Instant.class)
                    .newInstance("execution", "owner", "incarnation", 7L, start, start.plusSeconds(30));
            assertThat(anchor.getMethod("fencingToken").invoke(value)).isEqualTo(7L);
            assertThat(anchor.getMethod("expiresAt").invoke(value)).isEqualTo(start.plusSeconds(30));
        }
    }
}
