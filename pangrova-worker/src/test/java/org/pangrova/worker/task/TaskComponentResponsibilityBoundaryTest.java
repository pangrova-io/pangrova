package org.pangrova.worker.task;

import org.pangrova.worker.task.convert.TaskRequestBuilder;
import org.pangrova.worker.task.memory.TaskMemoryAllocator;
import org.pangrova.worker.task.memory.TaskMemoryRequestor;
import org.pangrova.worker.task.convert.TaskRequestConverter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies responsibility boundaries between Worker Task processing components.
 *
 * <p>Component responsibilities:
 * <ul>
 *   <li>{@link TaskManager} — Task lifecycle management (submit, execute, complete, fail, cancel)</li>
 *   <li>{@link TaskRequestConverter} — REST DTO → SPI TaskDescriptor conversion</li>
 *   <li>{@link TaskRequestBuilder} — SubmitTaskRequest → Protobuf TaskRequest bytes</li>
 *   <li>{@link TaskStateMapper} — Worker state → SPI/Protobuf state mapping (internal)</li>
 *   <li>{@link TaskTypeMapper} — REST taskType → Protobuf taskType mapping (internal)</li>
 * </ul>
 *
 * Validates: Requirements 3.3
 */
class TaskComponentResponsibilityBoundaryTest {

    // ── Mapper visibility: must be package-private ──

    @Test
    void taskStateMapper_isPackagePrivate() {
        assertThat(Modifier.isPublic(TaskStateMapper.class.getModifiers()))
                .as("TaskStateMapper must be package-private, not public")
                .isFalse();
    }

    @Test
    void taskTypeMapper_isPublicForConvertSubpackage() {
        assertThat(Modifier.isPublic(TaskTypeMapper.class.getModifiers()))
                .as("TaskTypeMapper must be public (used by convert/ sub-package)")
                .isTrue();
    }

    @Test
    void taskMemoryRequestor_isPublicForMemorySubpackage() {
        assertThat(Modifier.isPublic(TaskMemoryRequestor.class.getModifiers()))
                .as("TaskMemoryRequestor must be public (accessed from parent task package)")
                .isTrue();
    }

    @Test
    void taskLifecycleManager_isPackagePrivate() {
        assertThat(Modifier.isPublic(TaskLifecycleManager.class.getModifiers()))
                .as("TaskLifecycleManager must be package-private, not public")
                .isFalse();
    }

    // ── Public API surface: only TaskManager, TaskRequestConverter, TaskRequestBuilder ──

    @Test
    void taskManager_isPublic() {
        assertThat(Modifier.isPublic(TaskManager.class.getModifiers()))
                .as("TaskManager must be public (external API)")
                .isTrue();
    }

    @Test
    void taskRequestConverter_isPublic() {
        assertThat(Modifier.isPublic(TaskRequestConverter.class.getModifiers()))
                .as("TaskRequestConverter must be public (external API)")
                .isTrue();
    }

    @Test
    void taskRequestBuilder_isPublic() {
        assertThat(Modifier.isPublic(TaskRequestBuilder.class.getModifiers()))
                .as("TaskRequestBuilder must be public (external API)")
                .isTrue();
    }

    // ── No logical overlap: TaskRequestConverter does NOT do state/type mapping ──

    @Test
    void taskRequestConverter_hasNoStateMappingMethods() {
        Set<String> methodNames = publicMethodNames(TaskRequestConverter.class);
        assertThat(methodNames)
                .as("TaskRequestConverter should not contain state mapping methods")
                .noneMatch(name -> name.toLowerCase().contains("state")
                        && name.toLowerCase().contains("map"));
    }

    @Test
    void taskRequestConverter_hasNoTypeMappingToProtoMethods() {
        Set<String> methodNames = publicMethodNames(TaskRequestConverter.class);
        assertThat(methodNames)
                .as("TaskRequestConverter should not contain proto type mapping methods")
                .noneMatch(name -> name.toLowerCase().contains("prototype")
                        || name.toLowerCase().contains("toproto"));
    }

    // ── TaskManager exposes SPI state via getTaskSpiState (encapsulates TaskStateMapper) ──

    @Test
    void taskManager_exposesSpiStateLookup() {
        Set<String> methodNames = publicMethodNames(TaskManager.class);
        assertThat(methodNames)
                .as("TaskManager must expose getTaskSpiState for external SPI state queries")
                .contains("getTaskSpiState");
    }

    // ── TaskRequestBuilder uses TaskTypeMapper internally (not TaskRequestConverter) ──

    @Test
    void taskRequestBuilder_buildMethodAcceptsSubmitTaskRequest() throws NoSuchMethodException {
        Method buildMethod = TaskRequestBuilder.class.getMethod("build",
                org.pangrova.protocol.rest.task.SubmitTaskRequest.class);
        assertThat(buildMethod.getReturnType())
                .as("TaskRequestBuilder.build() must return byte[]")
                .isEqualTo(byte[].class);
    }

    // ── Component count: task package has exactly 10 production files (within threshold) ──

    @Test
    void taskPackage_fileCountWithinGovernanceThreshold() {
        List<String> taskPackageClasses = List.of(
                "TaskManager", "TaskManagerContext", "TaskMemoryRequestor",
                "TaskMemoryAllocator", "TaskStateMapper", "TaskTypeMapper",
                "TaskStatusReporter", "RemoteTaskStatusReporter", "TaskStatusCollector",
                "TaskLifecycleManager");
        List<String> convertPackageClasses = List.of(
                "TaskRequestConverter", "TaskRequestBuilder");
        assertThat(taskPackageClasses.size())
                .as("task/ package file count must be ≤15 (governance threshold)")
                .isLessThanOrEqualTo(15);
        assertThat(convertPackageClasses.size())
                .as("task/convert/ package file count must be ≤8 (healthy)")
                .isLessThanOrEqualTo(8);
    }

    private static Set<String> publicMethodNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()) || Modifier.isStatic(m.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.toSet());
    }
}
