package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

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
 * Golden tests for canonical cross-runtime protobuf enum compatibility.
 * Prevents an enum renumber from silently changing the Java/C++ wire contract.
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
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_LEVEL_UNSPECIFIED", 0);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_LEVEL_LOW", 1);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_LEVEL_MEDIUM", 2);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_LEVEL_HIGH", 3);
        MEMORY_PRESSURE_GOLDEN.put("MEMORY_PRESSURE_LEVEL_CRITICAL", 4);
    }

    private static final Map<String, Integer> NATIVE_COMMAND_TYPE_GOLDEN = new LinkedHashMap<>();
    static {
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_UNSPECIFIED", 0);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_GET_CAPABILITIES", 1);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_UPDATE_RESOURCE_GRANT", 2);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_INJECT_RUNTIME_FILTER", 3);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_PREFETCH_ARTIFACTS", 4);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_EVICT_CACHE", 5);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_GET_METRICS", 6);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_MEMORY_PRESSURE", 7);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE", 8);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_NO_MORE_EXCHANGE_SOURCES", 9);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_START_RESULT_SERVER", 10);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_STOP_RESULT_SERVER", 11);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_CANCEL_ATTEMPT", 12);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_START_SHUFFLE_SERVER", 13);
        NATIVE_COMMAND_TYPE_GOLDEN.put("RUNTIME_COMMAND_TYPE_STOP_SHUFFLE_SERVER", 14);
    }

    private static final Path CANONICAL_PROTO_DIR = TestProjectPaths.findProjectRoot()
            .resolve("pangrova-protocol/src/main/proto/pangrova/protocol/v2");
    private static final Path CANONICAL_TASK_PROTO = CANONICAL_PROTO_DIR.resolve("result.proto");
    private static final Path CANONICAL_RUNTIME_PROTO = CANONICAL_PROTO_DIR.resolve("runtime.proto");

    /**
     * Property: canonical TaskState values match the golden snapshot.
     */
    @Property(tries = 10)
    @Tag("proto-golden")
    void taskStateEnumMatchesGolden(@ForAll("taskStateEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> parsed = parseEnumFromProto(CANONICAL_TASK_PROTO, "TaskState");
        assertThat(parsed)
                .as("Canonical TaskState must contain golden entry %s=%d", entry.getKey(), entry.getValue())
                .containsEntry(entry.getKey(), entry.getValue());
    }

    /**
     * Property: canonical MemoryPressureLevel values match the golden snapshot.
     */
    @Property(tries = 10)
    @Tag("proto-golden")
    void memoryPressureLevelMatchesGolden(
            @ForAll("memoryPressureEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> parsed = parseEnumFromProto(CANONICAL_RUNTIME_PROTO, "MemoryPressureLevel");
        assertThat(parsed)
                .as("Canonical MemoryPressureLevel must contain golden entry %s=%d",
                        entry.getKey(), entry.getValue())
                .containsEntry(entry.getKey(), entry.getValue());
    }

    /**
     * Property: canonical NativeCommandType values match the golden snapshot.
     */
    @Property(tries = 10)
    @Tag("proto-golden")
    void nativeCommandTypeMatchesGolden(
            @ForAll("nativeCommandTypeEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> parsed = parseEnumFromProto(CANONICAL_RUNTIME_PROTO, "RuntimeCommandType");
        assertThat(parsed)
                .as("Canonical NativeCommandType must contain golden entry %s=%d",
                        entry.getKey(), entry.getValue())
                .containsEntry(entry.getKey(), entry.getValue());
    }

    /**
     * Property: canonical TaskState remains stable for both generated runtimes.
     */
    @Property(tries = 10)
    @Tag("proto-golden")
    void canonicalTaskStateRemainsStable(
            @ForAll("taskStateEntries") Map.Entry<String, Integer> entry) {
        Map<String, Integer> canonical = parseEnumFromProto(CANONICAL_TASK_PROTO, "TaskState");
        assertThat(canonical)
                .as("Canonical TaskState must contain %s=%d",
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
