package org.pangrova.testing.property;

import org.pangrova.testing.TestProjectPaths;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden tests for cross-language protobuf enum compatibility.
 * Prevents Java-side enum reorder from silently breaking the native protocol.
 *
 * Each enum's (name, number) mapping is pinned as a golden snapshot.
 * If a proto regeneration changes ordinals, these tests fail immediately
 * instead of causing silent runtime misinterpretation between Java and C++.
 *
 * <p><b>Validates: Requirements R45.1</b></p>
 */
class ProtoEnumGoldenPropertyTest {

    private static final Map<String, Integer> TASK_STATE_GOLDEN = new LinkedHashMap<>();
    static {
        TASK_STATE_GOLDEN.put("TASK_STATE_UNSPECIFIED", 0);
        TASK_STATE_GOLDEN.put("TASK_STATE_PENDING", 1);
        TASK_STATE_GOLDEN.put("TASK_STATE_RUNNING", 2);
        TASK_STATE_GOLDEN.put("TASK_STATE_FLUSHING", 3);
        TASK_STATE_GOLDEN.put("TASK_STATE_FINISHED", 4);
        TASK_STATE_GOLDEN.put("TASK_STATE_FAILED", 5);
        TASK_STATE_GOLDEN.put("TASK_STATE_CANCELLED", 6);
    }

    private static final Map<String, Integer> MEMORY_PRESSURE_GOLDEN = new LinkedHashMap<>();
    static {
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_NONE", 0);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_LOW", 1);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_MEDIUM", 2);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_HIGH", 3);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_CRITICAL", 4);
    }

    private static final Map<String, Integer> NATIVE_COMMAND_TYPE_GOLDEN = new LinkedHashMap<>();
    static {
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_UNKNOWN", 0);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_INJECT_DYNAMIC_FILTER", 1);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_UPDATE_MEMORY_LIMIT", 2);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_PREFETCH_SPLITS", 3);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_EVICT_CACHE", 4);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_GET_METRICS", 5);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_GET_CACHE_STATS", 6);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_GET_CAPABILITIES", 7);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_MEMORY_PRESSURE", 8);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_START_FLIGHT_SERVER", 10);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_CONNECT_EXCHANGE_SOURCE", 11);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_START_RESULT_SERVER", 12);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_STOP_RESULT_SERVER", 13);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_START_NATIVE_EXCHANGE", 14);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_STOP_SHUFFLE_SERVER", 15);
        NATIVE_COMMAND_TYPE_GOLDEN.put("NATIVE_COMMAND_NO_MORE_EXCHANGE_SOURCES", 16);
    }

    private static final Path NATIVE_PROTO_DIR = TestProjectPaths.findProjectRoot()
            .resolve("pangrova-worker-native/proto/pangrova/native/protocol/v1");
    private static final Path NATIVE_TASK_PROTO = NATIVE_PROTO_DIR.resolve("task.proto");
    private static final Path NATIVE_MEMORY_PROTO = NATIVE_PROTO_DIR.resolve("memory.proto");
    private static final Path NATIVE_COMMAND_PROTO = NATIVE_PROTO_DIR.resolve("command.proto");
    private static final Path JAVA_TASK_PROTO = TestProjectPaths.findProjectRoot()
            .resolve("pangrova-protocol/src/main/proto/pangrova/protocol/v1/task.proto");

    /**
     * Property: TaskState enum values in the native proto file match the golden snapshot.
     */
    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto enum golden compatibility")
    void taskStateEnumMatchesGolden(@ForAll("taskStateEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> parsed = parseEnumFromProto(NATIVE_TASK_PROTO, "TaskState");
        assertThat(parsed)
                .as("Native proto TaskState must contain golden entry %s=%d", entry.getKey(), entry.getValue())
                .containsEntry(entry.getKey(), entry.getValue());
    }

    /**
     * Property: MemoryPressureLevel enum values in the native proto match the golden snapshot.
     */
    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto enum golden compatibility")
    void memoryPressureLevelMatchesGolden(
            @ForAll("memoryPressureEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> parsed = parseEnumFromProto(NATIVE_MEMORY_PROTO, "MemoryPressureLevel");
        assertThat(parsed)
                .as("Native proto MemoryPressureLevel must contain golden entry %s=%d",
                        entry.getKey(), entry.getValue())
                .containsEntry(entry.getKey(), entry.getValue());
    }

    /**
     * Property: NativeCommandType enum values in the native proto match the golden snapshot.
     */
    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto enum golden compatibility")
    void nativeCommandTypeMatchesGolden(
            @ForAll("nativeCommandTypeEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> parsed = parseEnumFromProto(NATIVE_COMMAND_PROTO, "NativeCommandType");
        assertThat(parsed)
                .as("Native proto NativeCommandType must contain golden entry %s=%d",
                        entry.getKey(), entry.getValue())
                .containsEntry(entry.getKey(), entry.getValue());
    }

    /**
     * Property: Java-side TaskState proto values match native-side TaskState values exactly.
     */
    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto enum golden compatibility")
    void javaAndNativeTaskStateAligned(
            @ForAll("taskStateEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> javaSide = parseEnumFromProto(JAVA_TASK_PROTO, "TaskState");
        assertThat(javaSide)
                .as("Java proto TaskState must contain %s=%d matching native proto",
                        entry.getKey(), entry.getValue())
                .containsEntry(entry.getKey(), entry.getValue());
    }

    @Provide
    Arbitrary<Map.Entry<String, Integer>> taskStateEntries() {
        return Arbitraries.of(new ArrayList<>(TASK_STATE_GOLDEN.entrySet()));
    }

    @Provide
    Arbitrary<Map.Entry<String, Integer>> memoryPressureEntries() {
        return Arbitraries.of(new ArrayList<>(MEMORY_PRESSURE_GOLDEN.entrySet()));
    }

    @Provide
    Arbitrary<Map.Entry<String, Integer>> nativeCommandTypeEntries() {
        return Arbitraries.of(new ArrayList<>(NATIVE_COMMAND_TYPE_GOLDEN.entrySet()));
    }

    /**
     * Parse enum name-to-number mappings from a .proto file.
     * Handles proto3 enum syntax: {@code NAME = NUMBER;}
     */
    private static Map<String, Integer> parseEnumFromProto(Path protoFile, String enumName) {
        Map<String, Integer> result = new LinkedHashMap<>();
        try {
            List<String> lines = Files.readAllLines(protoFile);
            boolean inEnum = false;
            Pattern enumStart = Pattern.compile("^\\s*enum\\s+" + Pattern.quote(enumName) + "\\s*\\{");
            Pattern enumValue = Pattern.compile("^\\s*(\\w+)\\s*=\\s*(\\d+)\\s*;");
            for (String line : lines) {
                if (!inEnum && enumStart.matcher(line).find()) {
                    inEnum = true;
                    continue;
                }
                if (inEnum) {
                    if (line.trim().equals("}")) break;
                    Matcher m = enumValue.matcher(line);
                    if (m.find()) {
                        result.put(m.group(1), Integer.parseInt(m.group(2)));
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read proto file: " + protoFile, e);
        }
        return result;
    }
}
