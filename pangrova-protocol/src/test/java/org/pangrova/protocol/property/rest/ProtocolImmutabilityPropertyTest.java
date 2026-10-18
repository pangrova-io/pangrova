package org.pangrova.protocol.property.rest;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Tag;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying immutability of all REST DTO classes
 * in {@code org.pangrova.protocol.rest} and its sub-packages.
 *
 * <p>Uses reflection to discover all classes at test time, ensuring
 * newly added DTOs are automatically covered without manual updates.</p>
 *
 * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
 */
class ProtocolImmutabilityPropertyTest {

    private static final String BASE_PACKAGE = "org.pangrova.protocol.rest";

    /**
     * Immutable types allowed as record component types.
     * Primitives are inherently immutable and handled separately.
     */
    private static final Set<Class<?>> ALLOWED_IMMUTABLE_TYPES = Set.of(
            String.class, Long.class, Integer.class, Boolean.class,
            Double.class, Float.class, Short.class, Byte.class,
            Character.class
    );

    private static final List<Class<?>> ALL_DTO_CLASSES = discoverDtoClasses();

    /**
     * Property 8: REST DTO classes use an immutable representation.
     *
     * <p>For any REST DTO class discovered via classpath scanning,
     * the class must be a Java record or the final canonical task envelope.</p>
     *
     * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
     */
    @Property(tries = 10)
    @Tag("protocol-immutability")
    void allDtosUseImmutableRepresentations(
            @ForAll("dtoClasses") Class<?> dtoClass) {
        assertThat(dtoClass.isRecord()
                || (dtoClass == SubmitTaskRequest.class
                && java.lang.reflect.Modifier.isFinal(dtoClass.getModifiers())))
                .as("DTO %s must be a record or the final canonical envelope",
                        dtoClass.getSimpleName())
                .isTrue();
    }

    /**
     * Property 8: No REST DTO has setter methods.
     *
     * <p>For any REST DTO class, no public method should follow the
     * setter naming convention (setXxx with a single parameter).</p>
     *
     * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
     */
    @Property(tries = 10)
    @Tag("protocol-immutability")
    void noDtoHasSetters(@ForAll("dtoClasses") Class<?> dtoClass) {
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
     * a primitive, enum, a known immutable type (String, boxed primitives),
     * a {@link List}, {@link Map}, or {@link Set} (defensively copied in compact
     * constructors), or another protocol record.</p>
     *
     * <p><b>Validates: Requirements 2.1, 2.7, 7.9</b></p>
     */
    @Property(tries = 10)
    @Tag("protocol-immutability")
    void allComponentsAreImmutableTypes(@ForAll("dtoClasses") Class<?> dtoClass) {
        if (!dtoClass.isRecord()) {
            return; // covered by allDtosAreRecords
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

    @Provide
    Arbitrary<Class<?>> dtoClasses() {
        assertThat(ALL_DTO_CLASSES)
                .as("Should discover at least 20 REST DTO classes")
                .hasSizeGreaterThanOrEqualTo(20);
        return Arbitraries.of(ALL_DTO_CLASSES);
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
        // (e.g., DynamicFilterPayload clones on construction and on access)
        if (type == byte[].class) {
            return true;
        }
        // Another protocol record is immutable by induction
        if (type.isRecord() && type.getPackageName().startsWith(BASE_PACKAGE)) {
            return true;
        }
        return false;
    }

    /**
     * Discovers all concrete classes in {@link #BASE_PACKAGE} and sub-packages
     * by scanning the classpath directory structure.
     */
    private static List<Class<?>> discoverDtoClasses() {
        List<Class<?>> classes = new ArrayList<>();
        try {
            String path = BASE_PACKAGE.replace('.', '/');
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            var resources = cl.getResources(path);
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                if ("file".equals(resource.getProtocol())) {
                    scanDirectory(new File(resource.toURI()), BASE_PACKAGE, classes);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan REST DTO classes", e);
        }
        return List.copyOf(classes);
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

    private static void scanDirectory(File dir, String packageName, List<Class<?>> result) {
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
                try {
                    Class<?> clazz = Class.forName(className);
                    if (!clazz.isInterface()
                            && !clazz.isEnum()
                            && !clazz.isSynthetic()
                            && !isTestClass(clazz)
                            && !isConverterUtility(clazz)
                            && !isConstantsUtility(clazz)) {
                        result.add(clazz);
                    }
                } catch (ClassNotFoundException ignored) {
                    // skip unloadable classes
                }
            }
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
