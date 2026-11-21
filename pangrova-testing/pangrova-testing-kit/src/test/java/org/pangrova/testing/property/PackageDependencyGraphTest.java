package org.pangrova.testing.property;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.architecture.JavaSourceReferences;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PackageDependencyGraphTest {
    @TempDir
    Path root;

    @Test
    void discoversNewLeavesAtAnyReactorDepth() throws Exception {
        pom(root, "<modules><module>family</module></modules>");
        pom(root.resolve("family"), "<modules><module>nested/new-leaf</module></modules>");
        Path leaf = root.resolve("family/nested/new-leaf");
        pom(leaf, "");
        source(leaf, "org.pangrova.newleaf.A", "public class A {}");
        assertThat(PackageDependencyGraph.sourceRoots(root))
                .containsExactly(leaf.resolve("src/main/java").toRealPath());
    }

    @Test
    void missingDeclaredModuleFailsInsteadOfSilentlyReducingCoverage() throws Exception {
        pom(root, "<modules><module>missing</module></modules>");
        assertThatThrownBy(() -> PackageDependencyGraph.scan(root)).isInstanceOf(java.io.IOException.class);
    }

    @Test
    void duplicateReactorEntryFails() throws Exception {
        pom(root, "<modules><module>child</module><module>child</module></modules>");
        pom(root.resolve("child"), "");
        assertThatThrownBy(() -> PackageDependencyGraph.sourceRoots(root))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Repeated reactor module");
    }

    @Test
    void malformedJavaFailsInsteadOfSilentlyDroppingDependencies() throws Exception {
        pom(root, "");
        source(root, "org.pangrova.a.A", "public class A { invalid syntax !!! }");
        assertThatThrownBy(() -> PackageDependencyGraph.scan(root))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Cannot parse architecture sources");
    }

    @Test
    void qualifiedReferencesAreDetectedButTextAndCommentsAreNot() throws Exception {
        pom(root, "");
        source(root, "org.pangrova.a.A", """
                public class A {
                    String literal = "org.pangrova.text.C.value";
                    // org.pangrova.text.C.value
                    /** {@link org.pangrova.text.C} */
                    Object value = org.pangrova.real.B.value;
                }
                """);
        source(root, "org.pangrova.real.B", "public class B { public static Object value; }");
        source(root, "org.pangrova.text.C", "public class C { public static Object value; }");
        assertThat(PackageDependencyGraph.scan(root).dependencies("org.pangrova.a"))
                .containsExactly("org.pangrova.real");
    }

    @Test
    void moduleImportsIgnoreTextBlockExamples() throws Exception {
        source(root, "org.pangrova.a.A", "import org.pangrova.real.B;\n"
                + "/* import org.pangrova.fake.Comment; */\n"
                + "public class A { String example = \"\"\"\n"
                + "import org.pangrova.fake.Text;\n\"\"\"; B value; }");
        assertThat(PackageDependencyGraph.imports(root.resolve("src/main/java")))
                .containsExactly("org.pangrova.real.B");
    }

    @Test
    void moduleReferencesIncludeQualifiedConstructorsCallsAndNestedTypes() throws Exception {
        source(root, "org.pangrova.a.A", """
                public class A {
                    Object created = new org.pangrova.internal.Hidden();
                    Object value = org.pangrova.internal.Hidden.open().toString();
                    Class<?> nested = org.pangrova.internal.Hidden.Nested.class;
                    java.util.function.Supplier<?> callback = org.pangrova.internal.Hidden::open;
                    String example = "org.pangrova.fake.Example";
                }
                """);
        assertThat(PackageDependencyGraph.imports(root.resolve("src/main/java")))
                .containsExactlyInAnyOrder("org.pangrova.internal.Hidden",
                        "org.pangrova.internal.Hidden.open", "org.pangrova.internal.Hidden.Nested.class");
    }

    @Test
    void moduleReferencesRetainWildcardAndStaticImports() throws Exception {
        source(root, "org.pangrova.a.A", """
                import org.pangrova.internal.*;
                import static org.pangrova.other.Host.*;
                import static org.pangrova.third.Host.value;
                public class A {}
                """);
        assertThat(PackageDependencyGraph.imports(root.resolve("src/main/java")))
                .containsExactlyInAnyOrder("org.pangrova.internal.*",
                        "org.pangrova.other.Host.*", "org.pangrova.third.Host.value");
    }

    @Test
    void packageAnnotationsAreReferencesButOwnershipAndCommentsAreNot() throws Exception {
        Path source = root.resolve("src/main/java");
        Path owner = Files.createDirectories(source.resolve("org/pangrova/owner"));
        Files.writeString(owner.resolve("package-info.java"), """
                /* package org.pangrova.fake; */
                @org.pangrova.policy.Marker
                package org.pangrova.owner;
                """);
        var facts = JavaSourceReferences.scan(source);
        assertThat(facts.packages()).containsExactly("org.pangrova.owner");
        assertThat(facts.references()).containsOnlyKeys("org.pangrova.policy.Marker");
        assertThat(facts.references().get("org.pangrova.policy.Marker")).isEqualTo(Path.of("org/pangrova/owner/package-info.java") + ":2");
    }

    @Test
    void inventoriesNestedEnumsCustomFailuresAndConstantDeclarations() throws Exception {
        source(root, "org.pangrova.owner.Port", """
                public interface Port {
                    enum Phase { OPEN }
                    final class Failure extends RuntimeException {}
                }
                """);
        source(root, "org.pangrova.owner.Keys", """
                final class Keys {
                    static final String VERSION = "1";
                    private Keys() {}
                }
                """);
        source(root, "org.pangrova.owner.enums.Action", "public enum Action { OPEN }");
        var facts = JavaSourceReferences.scan(root.resolve("src/main/java"));
        assertThat(facts.enumDeclarations()).containsExactlyInAnyOrder(
                "org.pangrova.owner.Port.Phase", "org.pangrova.owner.enums.Action");
        assertThat(facts.misplacedDeclarations().values()).containsExactlyInAnyOrder("enums", "exception", "constants");
    }

    @Test
    void misplacedClassReportsItsDeclaredPackageAndExpectedPath() throws Exception {
        Path source = Files.createDirectories(root.resolve("src/main/java"));
        Files.writeString(source.resolve("Named.java"), "package org.pangrova.example; class Named {}");

        assertThatThrownBy(() -> JavaSourceReferences.scan(source))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Named.java declares package org.pangrova.example")
                .hasMessageContaining(Path.of("org/pangrova/example/Named.java").toString());
    }

    @Test
    void nestedTypesAndStaticMembersResolveToTheirOwningPackage() throws Exception {
        pom(root, "");
        source(root, "org.pangrova.a.A", """
                import org.pangrova.b.Outer.Inner;
                import static org.pangrova.c.Host.value;
                public class A { Inner inner; Object field = value; }
                """);
        source(root, "org.pangrova.b.Outer", "public class Outer { public static class Inner {} }");
        source(root, "org.pangrova.c.Host", "public class Host { public static Object value; }");
        assertThat(PackageDependencyGraph.scan(root).dependencies("org.pangrova.a"))
                .containsExactlyInAnyOrder("org.pangrova.b", "org.pangrova.c");
    }

    @Test
    void wildcardImportsAreExplicitSourceDependencies() throws Exception {
        pom(root, "");
        source(root, "org.pangrova.a.A", """
                import org.pangrova.b.*;
                import static org.pangrova.c.Host.*;
                public class A { B b; Object field = value; }
                """);
        source(root, "org.pangrova.b.B", "public class B {}");
        source(root, "org.pangrova.c.Host", "public class Host { public static Object value; }");
        assertThat(PackageDependencyGraph.scan(root).dependencies("org.pangrova.a"))
                .containsExactlyInAnyOrder("org.pangrova.b", "org.pangrova.c");
    }

    @Test
    void findsLongCrossModuleCyclesAndNonSiblingCyclesDeterministically() throws Exception {
        pom(root, "<modules><module>one</module><module>two</module></modules>");
        Path one = root.resolve("one");
        Path two = root.resolve("two");
        pom(one, "");
        pom(two, "");
        source(one, "org.pangrova.alpha.A", "public class A { org.pangrova.beta.inner.B b; }");
        source(two, "org.pangrova.beta.inner.B", "public class B { org.pangrova.gamma.C c; }");
        source(two, "org.pangrova.gamma.C", "public class C { org.pangrova.alpha.A a; }");
        source(one, "org.pangrova.left.D", "public class D { org.pangrova.other.right.E e; }");
        source(one, "org.pangrova.other.right.E", "public class E { org.pangrova.left.D d; }");
        var graph = PackageDependencyGraph.scan(root);
        assertThat(graph.cycles()).containsExactly(
                List.of("org.pangrova.alpha", "org.pangrova.beta.inner", "org.pangrova.gamma"),
                List.of("org.pangrova.left", "org.pangrova.other.right"));
        Path report = root.resolve("target/cycles.txt");
        graph.writeReport(report);
        String first = Files.readString(report);
        graph.writeReport(report);
        assertThat(Files.readString(report)).isEqualTo(first)
                .doesNotContain("../")
                .contains("Cyclic components: 2\n")
                .contains("A.java:2 -> org.pangrova.beta.inner.B", "B.java:2 -> org.pangrova.gamma.C");
    }

    @Test
    void reportShowsAnActualClosedPathWhenComponentOrderIsNotAnEdgeOrder() throws Exception {
        pom(root, "");
        source(root, "org.pangrova.a.A", "public class A { org.pangrova.c.C c; }");
        source(root, "org.pangrova.b.B", "public class B { org.pangrova.a.A a; }");
        source(root, "org.pangrova.c.C", "public class C { org.pangrova.b.B b; }");

        Path report = root.resolve("target/cycles.txt");
        PackageDependencyGraph.scan(root).writeReport(report);
        assertThat(Files.readString(report))
                .contains("Component: [org.pangrova.a, org.pangrova.b, org.pangrova.c]")
                .contains("Cycle: org.pangrova.a -> org.pangrova.c -> org.pangrova.b -> org.pangrova.a")
                .contains("A.java:2 -> org.pangrova.c.C", "B.java:2 -> org.pangrova.a.A",
                        "C.java:2 -> org.pangrova.b.B");
    }

    @Test
    void acyclicDisconnectedAndSamePackageReferencesDoNotFormCycles() throws Exception {
        pom(root, "");
        source(root, "org.pangrova.a.A", "import org.pangrova.b.B; public class A { B b; }");
        source(root, "org.pangrova.b.B", "public class B { org.pangrova.b.Other other; }");
        source(root, "org.pangrova.b.Other", "public class Other {}");
        source(root, "org.pangrova.c.C", "public class C {}");
        var graph = PackageDependencyGraph.scan(root);
        assertThat(graph.cycles()).isEmpty();
        assertThat(graph.dependencies("org.pangrova.b")).isEmpty();
        Path report = root.resolve("target/cycles.txt");
        graph.writeReport(report);
        assertThat(Files.readString(report)).contains("Packages: 3\nCyclic components: 0\n");
    }

    @Test
    void duplicateProductionTypesCannotOverwriteOwnership() throws Exception {
        pom(root, "<modules><module>one</module><module>two</module></modules>");
        for (String module : List.of("one", "two")) {
            Path directory = root.resolve(module);
            pom(directory, "");
            source(directory, "org.pangrova.a.A", "public class A {}");
        }
        assertThatThrownBy(() -> PackageDependencyGraph.scan(root))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Duplicate production type");
    }

    private static void pom(Path directory, String content) throws Exception {
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("pom.xml"), "<project>" + content + "</project>");
    }

    private static void source(Path module, String type, String declaration) throws Exception {
        Path source = module.resolve("src/main/java/" + type.replace('.', '/') + ".java");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "package " + type.substring(0, type.lastIndexOf('.')) + ";\n" + declaration);
    }
}
