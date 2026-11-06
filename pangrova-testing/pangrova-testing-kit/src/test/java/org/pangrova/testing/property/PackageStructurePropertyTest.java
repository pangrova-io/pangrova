package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.GenerationMode;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies capability-oriented package names; file counts do not establish cohesion. */
class PackageStructurePropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();
    private static final Pattern PACKAGE_DECLARATION = Pattern.compile("(?m)^\\s*package\\s+([\\w.]+)\\s*;");

    /** Retired resource/resilience owners; SPI is the current shared-contract owner. */
    private static final Pattern LEGACY_PACKAGE_REFERENCE = Pattern.compile(
            "(?m)^\\s*(?:package|import(?:\\s+static)?)\\s+"
                    + "(org\\.pangrova\\.(?:resource|resilience)(?:\\.[\\w.]+)?)\\s*;");

    private static final Set<String> FORBIDDEN_TECH_LAYER_NAMES = Set.of(
            "dto", "service", "dao", "repository", "controller"
    );

    /** Existing naming debt; additions require review, not a blanket module exemption. */
    private static final Set<String> TECH_LAYER_EXEMPT_PACKAGES = Set.of(
            "org.pangrova.metadata.service"
    );

    private static final List<String> PACKAGE_NAMES = scanPackageNames();

    @Property(generation = GenerationMode.EXHAUSTIVE)
    void noHorizontalTechLayerPackages(@ForAll("packageNames") String packageName) {
        if (TECH_LAYER_EXEMPT_PACKAGES.contains(packageName)) {
            return;
        }
        String lastSegment = packageName.substring(packageName.lastIndexOf('.') + 1);
        assertThat(FORBIDDEN_TECH_LAYER_NAMES)
                .as("Package %s uses horizontal tech-layer name '%s'. Use business-capability naming.",
                        packageName, lastSegment)
                .doesNotContain(lastSegment);
    }

    @Example
    void legacyModulePackagesAreGone() {
        List<String> offenders = new ArrayList<>();
        for (Path sourceRoot : findJavaSourceRoots(true)) {
            try (Stream<Path> files = Files.walk(sourceRoot)) {
                for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    var matcher = LEGACY_PACKAGE_REFERENCE.matcher(Files.readString(file));
                    while (matcher.find()) {
                        offenders.add(PROJECT_ROOT.relativize(file) + " -> " + matcher.group(1));
                    }
                }
            } catch (IOException error) {
                throw new UncheckedIOException("Cannot inspect legacy packages in " + sourceRoot, error);
            }
        }
        assertThat(offenders)
                .as("pangrova-resource/resilience were deleted in M15; import the owning "
                        + "domain contract instead")
                .isEmpty();
    }

    @Example
    void domainContractsAreAllowedWhileRetiredOwnersRemainRejected() {
        for (String declaration : List.of("package org.pangrova.table.api.ownership;",
                "import org.pangrova.table.api.ownership.TableControlRecord;",
                "import static org.pangrova.table.api.ownership.TableControlState.ACTIVE;")) {
            assertThat(LEGACY_PACKAGE_REFERENCE.matcher(declaration).find()).isFalse();
        }
        for (String declaration : List.of("package org.pangrova.resource;",
                "import org.pangrova.resilience.RetryPolicy;",
                "import static org.pangrova.resource.MemoryPool.LIMIT;")) {
            assertThat(LEGACY_PACKAGE_REFERENCE.matcher(declaration).find()).isTrue();
        }
    }

    @Provide
    Arbitrary<String> packageNames() {
        assertThat(PACKAGE_NAMES).isNotEmpty();
        return Arbitraries.of(PACKAGE_NAMES);
    }

    private static List<String> scanPackageNames() {
        Set<String> packages = new HashSet<>();
        for (Path sourceRoot : findJavaSourceRoots()) {
            try (Stream<Path> files = Files.walk(sourceRoot)) {
                for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    var matcher = PACKAGE_DECLARATION.matcher(Files.readString(file));
                    if (matcher.find() && matcher.group(1).startsWith("org.pangrova.")) {
                        packages.add(matcher.group(1));
                    }
                }
            } catch (IOException error) {
                throw new UncheckedIOException("Cannot inspect package names in " + sourceRoot, error);
            }
        }
        return packages.stream().sorted().toList();
    }

    private static List<Path> findJavaSourceRoots() {
        return findJavaSourceRoots(false);
    }

    private static List<Path> findJavaSourceRoots(boolean includeTests) {
        List<Path> roots = new ArrayList<>();
        try (Stream<Path> families = Files.list(PROJECT_ROOT)) {
            for (Path family : families.filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("pangrova-")).toList()) {
                try (Stream<Path> modules = Files.walk(family, 2)) {
                    modules.filter(p -> Files.isRegularFile(p.resolve("pom.xml")))
                            .forEach(p -> {
                                addIfDirectory(roots, p.resolve("src/main/java"));
                                if (includeTests) {
                                    addIfDirectory(roots, p.resolve("src/test/java"));
                                }
                            });
                }
            }
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot enumerate production source roots", error);
        }
        return List.copyOf(roots);
    }

    private static void addIfDirectory(List<Path> roots, Path candidate) {
        if (Files.isDirectory(candidate)) {
            roots.add(candidate);
        }
    }
}
