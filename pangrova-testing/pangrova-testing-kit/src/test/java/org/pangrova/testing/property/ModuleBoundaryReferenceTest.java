package org.pangrova.testing.property;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the real module policy with equivalent reference spellings and fresh source/POM facts. */
class ModuleBoundaryReferenceTest {
    @TempDir Path root;

    @BeforeEach
    void setUp() throws Exception {
        write(".pangrova/modules.yaml", """
                artifacts:
                  consumer:
                    path: consumer
                    packageRoots: [org.pangrova.consumer]
                    exports: [org.pangrova.consumer]
                    allowedImports: [producer]
                  producer:
                    path: producer
                    packageRoots: [org.pangrova.producer]
                    exports: [org.pangrova.producer.api]
                """);
        // The gate must use actual source/POM state, even when its fact snapshot is stale.
        write(".pangrova/module-baseline.yaml", "artifacts: {}\nreactor: []\n");
        pom("producer", "");
        pom("consumer", "test");
        write("producer/src/main/java/org/pangrova/producer/internal/Hidden.java", """
                package org.pangrova.producer.internal;
                public class Hidden { public static Object value; public static class Nested {} }
                """);
        write("producer/src/main/java/org/pangrova/producer/api/Visible.java", """
                package org.pangrova.producer.api;
                public class Visible {}
                """);
    }

    @Test
    void qualifiedWildcardAndStaticReferencesCannotBypassExportsOrMainDependencyScope() throws Exception {
        for (String body : List.of(
                "public class Use { Object value = new org.pangrova.producer.internal.Hidden(); }",
                "import org.pangrova.producer.internal.*; public class Use { Hidden value; }",
                "import static org.pangrova.producer.internal.Hidden.*; public class Use { Object field = value; }",
                "import org.pangrova.producer.internal.Hidden.Nested; public class Use { Nested value; }")) {
            write("consumer/src/main/java/org/pangrova/consumer/Use.java", "package org.pangrova.consumer;\n" + body);
            var model = ModuleDependencyPropertyTest.Model.load(root);
            var policy = model.policies().get("consumer");
            assertThat(model.imports("consumer").get("main")).containsExactly("producer");
            assertThatThrownBy(() -> ModuleDependencyPropertyTest.assertExportedPackages(model, policy, "producer"))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("not exported")
                    .hasMessageContaining("consumer/src/main/java/org/pangrova/consumer/Use.java:2");
            assertThat(ModuleDependencyPropertyTest.pomScope(model, "consumer", Set.of("producer")))
                    .containsExactly("producer(test)");
        }
    }

    @Test
    void qualifiedTestReferencesRequireAnActualDeclaredDependency() throws Exception {
        pom("consumer", "");
        write("consumer/src/test/java/org/pangrova/consumer/UseTest.java", """
                package org.pangrova.consumer;
                public class UseTest { Object value = new org.pangrova.producer.api.Visible(); }
                """);
        var missing = ModuleDependencyPropertyTest.Model.load(root);
        assertThat(missing.imports("consumer").get("main")).isEmpty();
        assertThat(missing.imports("consumer").get("test")).containsExactly("producer");
        assertThatThrownBy(() -> ModuleDependencyPropertyTest.assertTestDependencies(missing, "consumer"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("without a POM dependency");
        pom("consumer", "test");
        ModuleDependencyPropertyTest.assertTestDependencies(ModuleDependencyPropertyTest.Model.load(root), "consumer");
    }

    @Test
    void qualifiedReferencesStillHonorNarrowFriendExports() throws Exception {
        String policy = Files.readString(root.resolve(".pangrova/modules.yaml"));
        write(".pangrova/modules.yaml", policy.replace("allowedImports: [producer]", """
                allowedImports: [producer]
                    friendExports:
                      producer: [org.pangrova.producer.api.allowed]"""));
        write("consumer/src/main/java/org/pangrova/consumer/Use.java", """
                package org.pangrova.consumer;
                public class Use { Object value = new org.pangrova.producer.api.Visible(); }
                """);
        var model = ModuleDependencyPropertyTest.Model.load(root);
        var consumer = model.policies().get("consumer");
        ModuleDependencyPropertyTest.assertExportedPackages(model, consumer, "producer");
        assertThatThrownBy(() -> ModuleDependencyPropertyTest.assertFriendExport(model, consumer, "producer"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("may only import");
    }

    private void pom(String module, String scope) throws Exception {
        String dependency = scope.isEmpty() ? "" : "<dependencies><dependency><groupId>org.pangrova</groupId>"
                + "<artifactId>producer</artifactId><scope>" + scope + "</scope></dependency></dependencies>";
        write(module + "/pom.xml", "<project><artifactId>" + module + "</artifactId>" + dependency + "</project>");
    }

    private void write(String file, String content) throws Exception {
        Path path = root.resolve(file);
        Files.createDirectories(path.getParent());
        Files.writeString(path, content);
    }
}
