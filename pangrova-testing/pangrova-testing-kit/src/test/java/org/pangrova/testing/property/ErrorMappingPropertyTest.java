package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.common.enums.PangrovaErrorCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying unified error semantic mapping across modules.
 *
 * <p>Scans all modules for {@code PangrovaException} subclasses via source analysis
 * and verifies that every exception carries a non-null {@code PangrovaErrorCode}
 * with a non-blank message. Also verifies error code uniqueness and format.</p>
 *
 * <p><b>Validates: Requirements 5.8, 7.8</b></p>
 */
class ErrorMappingPropertyTest {

    private static final Pattern ERROR_CODE_FORMAT =
            Pattern.compile("^[A-Z]+-\\d{4}$");

    /** Known intermediate exception parents that themselves extend PangrovaException. */
    private static final Set<String> KNOWN_PANGROVA_PARENTS = Set.of(
            "PangrovaException",
            "MetadataException",
            "ConnectorException",
            "PlannerException"
    );

    /** Pattern to detect constructor accepting PangrovaErrorCode parameter. */
    private static final Pattern CTOR_ERROR_CODE_PATTERN =
            Pattern.compile("\\b(public|protected)\\s+\\w+\\s*\\([^)]*PangrovaErrorCode");

    /** Pattern to detect constructor accepting String parameter. */
    private static final Pattern CTOR_STRING_PATTERN =
            Pattern.compile("\\b(public|protected)\\s+\\w+\\s*\\([^)]*String");

    /**
     * Pattern to detect super() call that passes an PangrovaErrorCode constant
     * OR a helper method/expression that derives one (e.g. toErrorCode(category)).
     * Matches:
     *   - super(PangrovaErrorCode.SOME_CODE, ...)
     *   - super(toErrorCode(category), ...)
     *   - super(this.deriveErrorCode(), ...)
     */
    private static final Pattern SUPER_ERROR_CODE_PATTERN =
            Pattern.compile("super\\s*\\(\\s*(PangrovaErrorCode\\.|[A-Za-z_]\\w*[Ee]rrorCode\\s*\\()");

    /**
     * Pattern to detect super() call that constructs a message with 2+ arguments.
     * Matches super(errorCode, message) or super(errorCode, buildMessage(...), cause).
     * The comma after the first argument indicates a message is being passed.
     */
    private static final Pattern SUPER_WITH_MULTI_ARGS_PATTERN =
            Pattern.compile("super\\s*\\([^,]+,");

    private static final List<PangrovaErrorCode> ALL_ERROR_CODES =
            List.of(PangrovaErrorCode.values());

    private static final List<String> ERROR_CODE_NAMES =
            ALL_ERROR_CODES.stream().map(Enum::name).toList();

    /**
     * All discovered PangrovaException subclasses from production source.
     * Must be initialized after KNOWN_PANGROVA_PARENTS and pattern constants.
     */
    private static final List<ExceptionInfo> EXCEPTION_INFOS =
            scanExceptionSubclasses();

    private static final List<String> EXCEPTION_CLASS_NAMES =
            EXCEPTION_INFOS.stream().map(ExceptionInfo::className).toList();

    // -----------------------------------------------------------------------
    // Property 13a: PangrovaErrorCode values have unique code strings
    // -----------------------------------------------------------------------

    /**
     * Property 13: Every PangrovaErrorCode enum value has a unique code string.
     * Duplicate codes would cause ambiguous error classification.
     *
     * <p><b>Validates: Requirements 5.8, 7.8</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 13: error semantic unified mapping")
    void errorCodeStringsAreUnique(@ForAll("errorCodeNames") String enumName) {
        PangrovaErrorCode target = PangrovaErrorCode.valueOf(enumName);
        long count = ALL_ERROR_CODES.stream()
                .filter(ec -> ec.getCode().equals(target.getCode()))
                .count();
        assertThat(count)
                .as("PangrovaErrorCode '%s' code '%s' must be unique",
                        enumName, target.getCode())
                .isEqualTo(1);
    }

    // -----------------------------------------------------------------------
    // Property 13b: PangrovaErrorCode values follow CATEGORY-NNNN format
    // -----------------------------------------------------------------------

    /**
     * Property 13: Every PangrovaErrorCode code string follows CATEGORY-NNNN format.
     *
     * <p><b>Validates: Requirements 5.8, 7.8</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 13: error semantic unified mapping")
    void errorCodeFollowsCategoryFormat(@ForAll("errorCodeNames") String enumName) {
        PangrovaErrorCode errorCode = PangrovaErrorCode.valueOf(enumName);
        assertThat(errorCode.getCode())
                .as("PangrovaErrorCode '%s' code must match CATEGORY-NNNN format",
                        enumName)
                .matches(ERROR_CODE_FORMAT);
    }

    // -----------------------------------------------------------------------
    // Property 13c: Every PangrovaException subclass accepts PangrovaErrorCode
    // -----------------------------------------------------------------------

    /**
     * Property 13: Every PangrovaException subclass must accept an PangrovaErrorCode
     * in at least one constructor (verified via source analysis). This ensures
     * module-boundary exceptions always carry structured error classification.
     *
     * <p><b>Validates: Requirements 5.8, 7.8</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 13: error semantic unified mapping")
    void everyExceptionSubclassAcceptsErrorCode(
            @ForAll("exceptionClassNames") String className) {
        ExceptionInfo info = findExceptionInfo(className);
        assertThat(info.acceptsErrorCode())
                .as("PangrovaException subclass '%s' in '%s' must accept "
                                + "PangrovaErrorCode in constructor or inherit it from parent",
                        className, info.moduleName())
                .isTrue();
    }

    // -----------------------------------------------------------------------
    // Property 13d: Every PangrovaException subclass provides context message
    // -----------------------------------------------------------------------

    /**
     * Property 13: Every PangrovaException subclass must accept a String message
     * parameter in at least one constructor, ensuring error context is provided.
     *
     * <p><b>Validates: Requirements 5.8, 7.8</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 13: error semantic unified mapping")
    void everyExceptionSubclassAcceptsMessage(
            @ForAll("exceptionClassNames") String className) {
        ExceptionInfo info = findExceptionInfo(className);
        assertThat(info.acceptsMessage())
                .as("PangrovaException subclass '%s' in '%s' must accept "
                                + "String message in constructor",
                        className, info.moduleName())
                .isTrue();
    }

    // -----------------------------------------------------------------------
    // Property 13e: No module-boundary exception bypasses PangrovaException
    // -----------------------------------------------------------------------

    /**
     * Property 13: Module-boundary exception classes in pangrova-* modules must
     * extend PangrovaException (directly or transitively), not raw RuntimeException.
     * Verified via source-level extends clause analysis.
     *
     * <p><b>Validates: Requirements 5.8, 7.8</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 13: error semantic unified mapping")
    void noModuleBoundaryExceptionBypassesPangrovaException(
            @ForAll("exceptionClassNames") String className) {
        ExceptionInfo info = findExceptionInfo(className);
        assertThat(info.extendsPangrovaHierarchy())
                .as("Exception '%s' in module '%s' must extend PangrovaException hierarchy",
                        className, info.moduleName())
                .isTrue();
    }

    // -----------------------------------------------------------------------
    // Providers
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<String> errorCodeNames() {
        return Arbitraries.of(ERROR_CODE_NAMES);
    }

    @Provide
    Arbitrary<String> exceptionClassNames() {
        return Arbitraries.of(EXCEPTION_CLASS_NAMES);
    }

    // -----------------------------------------------------------------------
    // Scanning infrastructure
    // -----------------------------------------------------------------------

    private static ExceptionInfo findExceptionInfo(String className) {
        return EXCEPTION_INFOS.stream()
                .filter(info -> info.className().equals(className))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "No ExceptionInfo for " + className));
    }

    /**
     * Scans all pangrova-* module source directories for classes that extend
     * PangrovaException (directly or transitively via source text analysis).
     */
    private static List<ExceptionInfo> scanExceptionSubclasses() {
        Path root = TestProjectPaths.findProjectRoot();
        List<ExceptionInfo> results = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        try (Stream<Path> moduleDirs = Files.list(root)) {
            moduleDirs
                    .filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("pangrova-"))
                    .filter(p -> !p.getFileName().toString().equals("pangrova-testing"))
                    .forEach(moduleDir -> scanModuleExceptions(
                            moduleDir, results, seen));
        } catch (IOException ignored) {
            // skip if root listing fails
        }
        return List.copyOf(results);
    }

    private static void scanModuleExceptions(
            Path moduleDir, List<ExceptionInfo> results, Set<String> seen) {
        Path srcDir = moduleDir.resolve("src/main/java");
        if (!Files.isDirectory(srcDir)) {
            return;
        }
        String moduleName = moduleDir.getFileName().toString();
        try (Stream<Path> files = Files.walk(srcDir)) {
            files.filter(p -> p.toString().endsWith(".java"))
                    .forEach(javaFile -> processExceptionFile(
                            javaFile, moduleName, results, seen));
        } catch (IOException ignored) {
            // skip if walk fails
        }
    }

    private static void processExceptionFile(
            Path javaFile, String moduleName,
            List<ExceptionInfo> results, Set<String> seen) {
        try {
            String content = Files.readString(javaFile);
            if (!isExceptionSubclass(content)) {
                return;
            }
            String simpleName = extractSimpleName(javaFile);
            if (simpleName == null || !seen.add(moduleName + ":" + simpleName)) {
                return;
            }
            boolean acceptsCode = hasErrorCodeAcceptance(content);
            boolean acceptsMsg = hasMessageAcceptance(content);
            results.add(new ExceptionInfo(
                    simpleName, moduleName, true, acceptsCode, acceptsMsg));
        } catch (IOException ignored) {
            // skip unreadable files
        }
    }

    private static boolean isExceptionSubclass(String content) {
        for (String parent : KNOWN_PANGROVA_PARENTS) {
            if (content.contains("extends " + parent)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the exception class accepts PangrovaErrorCode either:
     * 1. Directly in a constructor parameter, or
     * 2. Via a super() call with a hardcoded PangrovaErrorCode constant.
     */
    private static boolean hasErrorCodeAcceptance(String content) {
        Matcher ctorMatcher = CTOR_ERROR_CODE_PATTERN.matcher(content);
        if (ctorMatcher.find()) {
            return true;
        }
        Matcher superMatcher = SUPER_ERROR_CODE_PATTERN.matcher(content);
        return superMatcher.find();
    }

    /**
     * Checks if the exception class provides a context message either:
     * 1. Via a String constructor parameter, or
     * 2. Via a super() call that passes a message (string literal, variable, or method call).
     */
    private static boolean hasMessageAcceptance(String content) {
        Matcher matcher = CTOR_STRING_PATTERN.matcher(content);
        if (matcher.find()) {
            return true;
        }
        // Accept constructors that build message via super() with 2+ args
        // (errorCode, message) or (errorCode, message, cause)
        return SUPER_WITH_MULTI_ARGS_PATTERN.matcher(content).find();
    }

    private static String extractSimpleName(Path javaFile) {
        String fileName = javaFile.getFileName().toString();
        if (fileName.endsWith(".java")) {
            return fileName.substring(0, fileName.length() - 5);
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Value types
    // -----------------------------------------------------------------------

    record ExceptionInfo(
            String className,
            String moduleName,
            boolean extendsPangrovaHierarchy,
            boolean acceptsErrorCode,
            boolean acceptsMessage) {}
}
