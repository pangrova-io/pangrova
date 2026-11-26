package org.pangrova.protocol.property.rest;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.rest.task.TaskReportEvidence;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.net.URL;
import java.net.JarURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exhaustive test verifying immutability of all REST DTO classes
 * in {@code org.pangrova.protocol.rest}, the shared execution identity package,
 * and the public client DTO packages
 * {@code org.pangrova.common.client.query} and {@code org.pangrova.common.client.metadata}
 * and shared result addresses, including their sub-packages.
 *
 * <p>Uses reflection to discover all classes at test time, ensuring
 * newly added DTOs are automatically covered without manual updates.</p>
 *
 * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
 */
class ProtocolImmutabilityPropertyTest {

    /** Packages scanned for DTO classes. */
    private static final List<String> DTO_PACKAGES = List.of(
            "org.pangrova.common.client.result",
            "org.pangrova.protocol.rest",
            "org.pangrova.protocol.execution.identity",
            "org.pangrova.common.client.query",
            "org.pangrova.common.client.metadata");

    /**
     * Packages whose record types count as immutable DTO components. The public
     * error package is scanned only by reference (its exception types are not DTOs).
     */
    private static final List<String> DTO_TYPE_ROOTS = List.of(
            "org.pangrova.common.client.result",
            "org.pangrova.protocol.rest",
            "org.pangrova.protocol.execution.identity",
            "org.pangrova.common.client.query",
            "org.pangrova.common.client.metadata",
            "org.pangrova.common.client.error");

    /**
     * Immutable types allowed as record component types.
     * Primitives are inherently immutable and handled separately.
     */
    private static final Set<Class<?>> ALLOWED_IMMUTABLE_TYPES = Set.of(
            String.class, UUID.class, Long.class, Integer.class, Boolean.class,
            Double.class, Float.class, Short.class, Byte.class,
            Character.class, ByteString.class
    );

    private static final List<Class<?>> ALL_DTO_CLASSES = discoverDtoClasses();

    @Test
    void resultAddressAndReportEvidenceAreIncludedInTheImmutableDtoAudit() {
        assertThat(ALL_DTO_CLASSES).contains(ResultEndpoint.class, TaskReportEvidence.class);
    }

    /**
     * Property 8: REST DTO classes use an immutable representation.
     *
     * <p>For any REST DTO class discovered via classpath scanning,
     * the class must be a Java record or a final envelope with immutable final fields.</p>
     *
     * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
     */
    @ParameterizedTest
    @MethodSource("dtoClasses")
    @Tag("protocol-immutability")
    void allDtosUseImmutableRepresentations(
            Class<?> dtoClass) {
        assertThat(dtoClass.isRecord() || java.lang.reflect.Modifier.isFinal(dtoClass.getModifiers()))
                .as("DTO %s must be a record or a final envelope", dtoClass.getSimpleName())
                .isTrue();
        for (var field : dtoClass.getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            assertThat(java.lang.reflect.Modifier.isFinal(field.getModifiers()))
                    .as("DTO %s field %s must be final", dtoClass.getSimpleName(), field.getName()).isTrue();
            assertThat(isImmutableComponentType(field.getType()))
                    .as("DTO %s field %s must have an immutable representation",
                            dtoClass.getSimpleName(), field.getName()).isTrue();
        }
    }

    /**
     * Property 8: No REST DTO has setter methods.
     *
     * <p>For any REST DTO class, no public method should follow the
     * setter naming convention (setXxx with a single parameter).</p>
     *
     * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
     */
    @ParameterizedTest
    @MethodSource("dtoClasses")
    @Tag("protocol-immutability")
    void noDtoHasSetters(Class<?> dtoClass) {
        for (Method method : dtoClass.getMethods()) {
            if (method.getDeclaringClass() == Object.class) {
                continue;
            }
            boolean isSetter = method.getName().startsWith("set")
                    && method.getParameterCount() == 1;
            assertThat(isSetter)
                    .as("DTO %s must not have setter: %s",
                            dtoClass.getSimpleName(), method.getName())
                    .isFalse();
        }
    }

    /**
     * Property 8: All record components use immutable types.
     *
     * <p>For any REST DTO record, every component type must be either
     * a primitive, enum, a known immutable type (String, UUID, boxed primitives),
     * a {@link List}, {@link Map}, or {@link Set} (defensively copied in compact
     * constructors), another protocol record, or the audited report evidence
     * envelope.</p>
     *
     * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
     */
    @ParameterizedTest
    @MethodSource("dtoClasses")
    @Tag("protocol-immutability")
    void allComponentsAreImmutableTypes(Class<?> dtoClass) {
        if (!dtoClass.isRecord()) {
            return; // final envelope fields are checked by allDtosUseImmutableRepresentations
        }
        for (RecordComponent component : dtoClass.getRecordComponents()) {
            Class<?> type = component.getType();
            assertThat(isImmutableComponentType(type))
                    .as("Record %s component '%s' of type %s must be an immutable type",
                            dtoClass.getSimpleName(), component.getName(),
                            type.getSimpleName())
                    .isTrue();
        }
    }

    static java.util.stream.Stream<Class<?>> dtoClasses() {
        assertThat(ALL_DTO_CLASSES)
                .as("Should discover at least 20 REST DTO classes")
                .hasSizeGreaterThanOrEqualTo(20);
        return ALL_DTO_CLASSES.stream();
    }

    private static boolean isImmutableComponentType(Class<?> type) {
        if (type.isPrimitive()) {
            return true;
        }
        if (type.isEnum()) {
            return true;
        }
        if (ALLOWED_IMMUTABLE_TYPES.contains(type)) {
            return true;
        }
        // Collections are allowed when defensively copied (verified by RestDtoImmutabilityTest).
        if (type == List.class || type == Map.class || type == Set.class) {
            return true;
        }
        // byte[] is allowed when defensively copied in compact constructor and accessor
        if (type == byte[].class) {
            return true;
        }
        // Generated protobuf messages are immutable after build; mutable builders are not allowed.
        if (com.google.protobuf.MessageLite.class.isAssignableFrom(type)
                && java.lang.reflect.Modifier.isFinal(type.getModifiers())) {
            return true;
        }
        // Another protocol record is immutable by induction
        if (type.isRecord() && DTO_TYPE_ROOTS.stream().anyMatch(root -> type.getPackageName().equals(root)
                || type.getPackageName().startsWith(root + "."))) {
            return true;
        }
        // The envelope's final fields are checked separately by the DTO audit.
        return type == TaskReportEvidence.class && ALL_DTO_CLASSES.contains(type);
    }

    /**
     * Discovers all concrete classes in {@link #DTO_PACKAGES} and sub-packages
     * by scanning the classpath directory structure.
     */
    private static List<Class<?>> discoverDtoClasses() {
        List<Class<?>> classes = new ArrayList<>();
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            for (String basePackage : DTO_PACKAGES) {
                String path = basePackage.replace('.', '/');
                var resources = cl.getResources(path);
                while (resources.hasMoreElements()) {
                    URL resource = resources.nextElement();
                    if ("file".equals(resource.getProtocol())) {
                        scanDirectory(new File(resource.toURI()), basePackage, classes);
                    } else if ("jar".equals(resource.getProtocol())) {
                        var connection = (JarURLConnection) resource.openConnection();
                        connection.setUseCaches(false);
                        try (var jar = connection.getJarFile()) {
                            var entries = jar.entries();
                            while (entries.hasMoreElements()) {
                                String name = entries.nextElement().getName();
                                if (name.startsWith(path + "/") && name.endsWith(".class")) {
                                    addDtoClass(name.substring(0, name.length() - 6).replace('/', '.'), classes);
                                }
                            }
                        }
                    } else {
                        throw new IllegalStateException("Unsupported DTO classpath resource: " + resource);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan REST DTO classes", e);
        }
        return classes.stream().distinct().toList();
    }

    private static boolean isTestClass(Class<?> clazz) {
        String name = clazz.getSimpleName();
        if (name.endsWith("Test") || name.endsWith("PropertyTest")) {
            return true;
        }
        // Exclude inner/nested classes of test classes
        Class<?> enclosing = clazz.getEnclosingClass();
        return enclosing != null && isTestClass(enclosing);
    }

    private static void scanDirectory(File dir, String packageName, List<Class<?>> result)
            throws ClassNotFoundException {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                scanDirectory(file, packageName + "." + file.getName(), result);
            } else if (file.getName().endsWith(".class")) {
                String className = packageName + "."
                        + file.getName().substring(0, file.getName().length() - 6);
                addDtoClass(className, result);
            }
        }
    }

    private static void addDtoClass(String className, List<Class<?>> result)
            throws ClassNotFoundException {
        Class<?> clazz = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        if (!clazz.isInterface() && !clazz.isEnum() && !clazz.isSynthetic()
                && !isTestClass(clazz) && !isConverterUtility(clazz) && !isConstantsUtility(clazz)) {
            result.add(clazz);
        }
    }

    /**
     * Excludes converter/utility classes that live in the {@code convert}
     * sub-package. These are stateless helpers, not protocol DTOs.
     */
    private static boolean isConverterUtility(Class<?> clazz) {
        return clazz.getPackageName().endsWith(".convert");
    }

    private static boolean isConstantsUtility(Class<?> clazz) {
        try {
            var ctor = clazz.getDeclaredConstructor();
            return java.lang.reflect.Modifier.isPrivate(ctor.getModifiers());
        } catch (NoSuchMethodException e) {
            return false;
        }
    }
}
