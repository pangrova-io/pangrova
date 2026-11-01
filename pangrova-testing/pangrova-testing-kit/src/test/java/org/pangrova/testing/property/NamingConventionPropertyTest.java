package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

import org.junit.jupiter.api.Test;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies enduring runtime type identity and naming boundary constraints. */
class NamingConventionPropertyTest {

    private static final Path PROJECT_ROOT = TestProjectPaths.findProjectRoot();

    // ── Value types ──

    record ClassInfo(String simpleName, String qualifiedName, String module, String packageName) {
        @Override
        public String toString() {
            return module + ":" + qualifiedName;
        }
    }

    // ── Module lists ──

    /** Stable-layer modules where framework names are banned in class names. */
    private static final List<String> STABLE_MODULES = List.of(
            "pangrova-common", "pangrova-protocol",
            "pangrova-spi", "pangrova-filesystem-api", "pangrova-persistence-api", "pangrova-streaming"
    );

    /** Framework substrings banned in stable-layer class names. */
    private static final List<String> BANNED_FRAMEWORK_NAMES = List.of(
            "Jetty", "Calcite", "Presto", "Rss"
    );

    /** C1 bans Flight in these stable owners, but permits observability adapters. */
    private static final Set<String> FLIGHT_RESTRICTED_STABLE_MODULES = Set.of(
            "pangrova-common", "pangrova-protocol"
    );

    /** Banned abbreviation prefixes in class names. */
    private static final Pattern BANNED_CLASS_PREFIX = Pattern.compile(
            "^(Rss|Dpp|Aqe|Sop|Ipc|Repl)[A-Z].*"
    );

    /** MV as prefix (not substring like MVN). */
    private static final Pattern MV_PREFIX = Pattern.compile("^MV[A-Z].*");

    /** Dt as prefix for DynamicTable. */
    private static final Pattern DT_PREFIX = Pattern.compile("^Dt[A-Z].*");

    /** HA as prefix. */
    private static final Pattern HA_PREFIX = Pattern.compile("^HA[A-Z].*");

    /** Banned package segments. */
    private static final Set<String> BANNED_PACKAGE_SEGMENTS = Set.of(
            "rss", "dpp", "aqe", "sop", "rls", "jaxrs"
    );

    /**
     * Package segment "dt" is banned (should be "dynamictable").
     * Package segment "ha" is allowed since classes inside are already
     * named HighAvailability*. Package compatibility is reviewed separately.
     */
    private static final String BANNED_DT_SEGMENT = "dt";

    // ── Precomputed data ──

    private static final List<ClassInfo> ALL_PRODUCTION_CLASSES = scanAllProductionClasses();
    private static final List<ClassInfo> STABLE_LAYER_CLASSES = filterByModules(ALL_PRODUCTION_CLASSES, STABLE_MODULES);


    /** Fully qualified names must be unique; simple names may repeat by owner. */
    @Test
    void productionQualifiedNamesMustBeUniqueAcrossModules() {
        assertThat(ALL_PRODUCTION_CLASSES).isNotEmpty();
        Set<String> qualifiedNames = new HashSet<>();
        for (ClassInfo classInfo : ALL_PRODUCTION_CLASSES) {
            assertThat(qualifiedNames.add(classInfo.qualifiedName()))
                    .as("Duplicate runtime type '%s' in module '%s'",
                            classInfo.qualifiedName(), classInfo.module())
                    .isTrue();
        }
    }

    /** Framework-neutral owner layers must not advertise implementation frameworks. */
    @Property(generation = GenerationMode.EXHAUSTIVE)
    void stableLayerClasses_doNotContainFrameworkNames(
            @ForAll("stableLayerClasses") ClassInfo classInfo) {
        verifyStableLayerClassName(classInfo);
    }

    @Test
    void frameworkRestrictionsFollowModuleBoundaries() {
        assertThatCode(() -> verifyStableLayerClassName(new ClassInfo(
                "FlightClientTracingInterceptor", "org.pangrova.observability.sdk.tracing.adapter.grpc.FlightClientTracingInterceptor",
                "pangrova-observability-sdk", "org.pangrova.observability.sdk.tracing.adapter.grpc")))
                .doesNotThrowAnyException();
        assertThatCode(() -> verifyStableLayerClassName(new ClassInfo(
                "ArrowBatchCodec", "org.pangrova.protocol.ArrowBatchCodec",
                "pangrova-protocol", "org.pangrova.protocol")))
                .doesNotThrowAnyException();
        for (String module : FLIGHT_RESTRICTED_STABLE_MODULES) {
            assertThatThrownBy(() -> verifyStableLayerClassName(new ClassInfo(
                    "FlightSession", "org.pangrova.FlightSession", module, "org.pangrova")))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("Flight");
        }
        assertThatThrownBy(() -> verifyStableLayerClassName(new ClassInfo(
                "ArrowBatchCodec", "org.pangrova.common.ArrowBatchCodec",
                "pangrova-common", "org.pangrova.common")))
                .isInstanceOf(AssertionError.class).hasMessageContaining("Arrow");
    }

    private static void verifyStableLayerClassName(ClassInfo classInfo) {
        String name = classInfo.simpleName();
        for (String banned : BANNED_FRAMEWORK_NAMES) {
            assertThat(name)
                    .as("Framework leak '%s' in stable class %s", banned, classInfo)
                    .doesNotContain(banned);
        }
        if (FLIGHT_RESTRICTED_STABLE_MODULES.contains(classInfo.module())) {
            assertThat(name).as("Flight leak in stable class %s", classInfo).doesNotContain("Flight");
        }
        // C1 allows Arrow in protocol, unlike the other stable owners.
        if (!classInfo.module().equals("pangrova-protocol")) {
            assertThat(name).as("Arrow leak in stable class %s", classInfo).doesNotContain("Arrow");
        }
    }

    /** Production names and packages retain the documented abbreviation constraints. */
    @Property(generation = GenerationMode.EXHAUSTIVE)
    void classNames_doNotContainBannedAbbreviations(
            @ForAll("allProductionClasses") ClassInfo classInfo) {
        String name = classInfo.simpleName();
        assertThat(BANNED_CLASS_PREFIX.matcher(name).matches())
                .as("Banned abbreviation prefix in class '%s' (%s)", name, classInfo)
                .isFalse();
        assertThat(MV_PREFIX.matcher(name).matches())
                .as("MV prefix in class '%s' (%s)", name, classInfo)
                .isFalse();
        assertThat(DT_PREFIX.matcher(name).matches())
                .as("Dt prefix in class '%s' (%s)", name, classInfo)
                .isFalse();
        assertThat(HA_PREFIX.matcher(name).matches())
                .as("HA prefix in class '%s' (%s)", name, classInfo)
                .isFalse();

        // Check banned package segments
        String pkg = classInfo.packageName();
        for (String segment : pkg.split("\\.")) {
            assertThat(BANNED_PACKAGE_SEGMENTS.contains(segment))
                    .as("Banned package segment '%s' in %s", segment, classInfo)
                    .isFalse();
        }
        // "dt" as standalone package segment is banned
        for (String segment : pkg.split("\\.")) {
            assertThat(segment.equals(BANNED_DT_SEGMENT))
                    .as("Banned 'dt' package segment in %s", classInfo)
                    .isFalse();
        }
    }


    // ── Providers ──

    @Provide
    Arbitrary<ClassInfo> stableLayerClasses() {
        assertThat(STABLE_LAYER_CLASSES).isNotEmpty();
        return Arbitraries.of(STABLE_LAYER_CLASSES);
    }

    @Provide
    Arbitrary<ClassInfo> allProductionClasses() {
        assertThat(ALL_PRODUCTION_CLASSES).isNotEmpty();
        return Arbitraries.of(ALL_PRODUCTION_CLASSES);
    }

    // ── Source scanning infrastructure ──

    private static List<ClassInfo> scanAllProductionClasses() {
        List<ClassInfo> result = new ArrayList<>();
        try (Stream<Path> families = Files.list(PROJECT_ROOT)) {
            families.filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("pangrova-"))
                    .forEach(family -> scanFamily(family, result));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot enumerate production modules", error);
        }
        return List.copyOf(result);
    }

    private static void scanFamily(Path family, List<ClassInfo> result) {
        try (Stream<Path> modules = Files.walk(family, 2)) {
            modules.filter(p -> Files.isRegularFile(p.resolve("pom.xml")))
                    .filter(p -> Files.isDirectory(p.resolve("src/main/java")))
                    .forEach(module -> scanDirectory(module.resolve("src/main/java"),
                            module.getFileName().toString(), result));
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot enumerate module family " + family, error);
        }
    }

    private static void scanDirectory(Path directory, String module, List<ClassInfo> result) {
        Pattern packageDeclaration = Pattern.compile("(?m)^\\s*package\\s+([\\w.]+)\\s*;");
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path javaFile : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                String fileName = javaFile.getFileName().toString();
                if (fileName.equals("package-info.java") || fileName.equals("module-info.java")) {
                    continue;
                }
                String simpleName = fileName.substring(0, fileName.length() - 5);
                var matcher = packageDeclaration.matcher(Files.readString(javaFile));
                String packageName = matcher.find() ? matcher.group(1) : "";
                String qualifiedName = packageName.isEmpty() ? simpleName : packageName + "." + simpleName;
                result.add(new ClassInfo(simpleName, qualifiedName, module, packageName));
            }
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot inspect production sources in " + directory, error);
        }
    }

    private static List<ClassInfo> filterByModules(List<ClassInfo> classes, List<String> moduleNames) {
        Set<String> shortNames = new HashSet<>();
        for (String m : moduleNames) {
            shortNames.add(extractModuleName(m));
        }
        return classes.stream()
                .filter(c -> shortNames.contains(c.module()))
                .toList();
    }

    private static String extractModuleName(String modulePath) {
        int slash = modulePath.lastIndexOf('/');
        return slash >= 0 ? modulePath.substring(slash + 1) : modulePath;
    }


}
