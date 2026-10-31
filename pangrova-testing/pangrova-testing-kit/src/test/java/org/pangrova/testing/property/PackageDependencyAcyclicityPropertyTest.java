package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks every strongly connected component of the default reactor's main-source graph. */
class PackageDependencyAcyclicityPropertyTest {
    @Test
    void noPackageDependencyCycles() throws Exception {
        var graph = PackageDependencyGraph.scan(TestProjectPaths.findProjectRoot());
        Path report = Path.of("target/package-dependency-cycles.txt");
        graph.writeReport(report);
        assertThat(graph.cycles())
                .as("Package cycles; source witnesses: %s", report.toAbsolutePath())
                .isEmpty();
    }
}
