package org.pangrova.query.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Stable;
import org.pangrova.observability.api.event.QueryState;
import org.pangrova.protocol.execution.QueryId;
import org.pangrova.query.api.audit.QueryAuditStore;

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
                QueryId.class.getProtectionDomain().getCodeSource().getLocation()};
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (String absent : List.of("org.pangrova.query.core.audit.QueryAuditService",
                    "org.pangrova.query.store.jdbc.anchor.JdbcExecutionAnchorStore",
                    "org.pangrova.query.store.jdbc.anchor.JdbcExecutionTransactionFence",
                    "org.postgresql.Driver",
                    "org.pangrova.security.api.UserIdentity", "com.fasterxml.jackson.databind.ObjectMapper",
                    "com.github.benmanes.caffeine.cache.Caffeine")) {
                assertThatThrownBy(() -> loader.loadClass(absent)).isInstanceOf(ClassNotFoundException.class);
            }
            Path classes = Path.of(api.toURI());
            var linked = new ArrayList<String>();
            try (var paths = Files.walk(classes.resolve("org/pangrova/query/api"))) {
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
            assertThat(linked).contains("org.pangrova.query.api.audit.QueryAuditStore",
                    "org.pangrova.query.api.coordination.ExecutionAnchor",
                    "org.pangrova.query.api.coordination.ExecutionOwnerLostException",
                    "org.pangrova.query.api.coordination.jdbc.ExecutionTransactionFence",
                    "org.pangrova.query.api.state.QueryLifecycle",
                    "org.pangrova.query.api.state.QueryStateJournal",
                    "org.pangrova.query.api.transaction.WriteCommitQuarantineStore");
            Class<?> anchor = loader.loadClass("org.pangrova.query.api.coordination.ExecutionAnchor");
            Instant start = Instant.parse("2026-10-31T00:00:00Z");
            Object value = anchor.getConstructor(String.class, String.class, String.class,
                    long.class, Instant.class, Instant.class)
                    .newInstance("execution", "owner", "incarnation", 7L, start, start.plusSeconds(30));
            assertThat(anchor.getMethod("fencingToken").invoke(value)).isEqualTo(7L);
            assertThat(anchor.getMethod("expiresAt").invoke(value)).isEqualTo(start.plusSeconds(30));
        }
    }
}
