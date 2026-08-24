package org.pangrova.testing.property;

import org.pangrova.testing.TestProjectPaths;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Tag;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden + cross-boundary alignment tests for protobuf <em>message field numbers and types</em>.
 *
 * <p>The sibling {@link ProtoEnumGoldenPropertyTest} pins enum (name, number) pairs. That guard alone
 * leaves message field numbers unprotected: a native↔Java field-number or wire-type drift on a
 * cross-boundary message (TaskRequest, WriteConfig, ResourceLimit, ShuffleConfig, SplitInfo) would
 * silently corrupt deserialization without any test failing (F-PROTO-01).
 *
 * <p>This test closes that gap two ways:
 * <ol>
 *   <li><b>Golden snapshot</b> — each cross-boundary message's (field-number → wire-type) map is pinned
 *       on the native side; a reorder/retype fails immediately.</li>
 *   <li><b>Cross-boundary alignment</b> — for every field number present on <em>both</em> the native and
 *       Java protos, the declared type must be identical. Field <em>names</em> are intentionally NOT
 *       compared because the contract is name-blind (e.g. field 3 is {@code substrait_plan} natively and
 *       {@code plan_fragment} on the Java side — same number, same {@code bytes} type).</li>
 * </ol>
 *
 * <p><b>Validates: Requirements R45.3 (T13.3) · Finding F-PROTO-01</b></p>
 */
class ProtoMessageFieldGoldenPropertyTest {

    private static final Path ROOT = TestProjectPaths.findProjectRoot();

    private static final Path NATIVE_TASK_PROTO = ROOT.resolve(
            "pangrova-worker-native/proto/pangrova/native/protocol/v1/task.proto");
    private static final Path NATIVE_COMMON_PROTO = ROOT.resolve(
            "pangrova-worker-native/proto/pangrova/native/protocol/v1/common.proto");
    private static final Path NATIVE_SHUFFLE_PROTO = ROOT.resolve(
            "pangrova-worker-native/proto/pangrova/native/protocol/v1/shuffle.proto");
    private static final Path NATIVE_SPLIT_PROTO = ROOT.resolve(
            "pangrova-worker-native/proto/pangrova/native/protocol/v1/split.proto");

    private static final Path JAVA_TASK_PROTO = ROOT.resolve(
            "pangrova-protocol/src/main/proto/pangrova/protocol/v1/task.proto");
    private static final Path JAVA_COMMON_PROTO = ROOT.resolve(
            "pangrova-protocol/src/main/proto/pangrova/protocol/v1/common.proto");

    // ── Golden (field-number → wire-type) snapshots, taken from the native proto ──

    private static final Map<Integer, String> TASK_REQUEST_GOLDEN = new LinkedHashMap<>();
    static {
        TASK_REQUEST_GOLDEN.put(1, "string");
        TASK_REQUEST_GOLDEN.put(2, "string");
        TASK_REQUEST_GOLDEN.put(3, "bytes");
        TASK_REQUEST_GOLDEN.put(4, "repeated SplitInfo");
        TASK_REQUEST_GOLDEN.put(5, "ResourceLimit");
        TASK_REQUEST_GOLDEN.put(6, "map<string,string>");
        TASK_REQUEST_GOLDEN.put(7, "TaskType");
        TASK_REQUEST_GOLDEN.put(8, "int32");
        // 9 is permanently reserved (was java_split_bytes) — absent on purpose.
        TASK_REQUEST_GOLDEN.put(10, "int32");
        TASK_REQUEST_GOLDEN.put(11, "WriteConfig");
        TASK_REQUEST_GOLDEN.put(12, "ShuffleConfig");
        TASK_REQUEST_GOLDEN.put(13, "string");
        TASK_REQUEST_GOLDEN.put(14, "string");
        TASK_REQUEST_GOLDEN.put(15, "int64");
    }

    private static final Map<Integer, String> WRITE_CONFIG_GOLDEN = new LinkedHashMap<>();
    static {
        WRITE_CONFIG_GOLDEN.put(1, "string");
        WRITE_CONFIG_GOLDEN.put(2, "string");
        WRITE_CONFIG_GOLDEN.put(3, "repeated string");
        WRITE_CONFIG_GOLDEN.put(4, "string");
        WRITE_CONFIG_GOLDEN.put(5, "FileFormat");
        WRITE_CONFIG_GOLDEN.put(6, "int64");
    }

    private static final Map<Integer, String> RESOURCE_LIMIT_GOLDEN = new LinkedHashMap<>();
    static {
        RESOURCE_LIMIT_GOLDEN.put(1, "int64");
        RESOURCE_LIMIT_GOLDEN.put(2, "int32");
        RESOURCE_LIMIT_GOLDEN.put(3, "int64");
        RESOURCE_LIMIT_GOLDEN.put(4, "string");
    }

    private static final Map<Integer, String> SHUFFLE_CONFIG_GOLDEN = new LinkedHashMap<>();
    static {
        SHUFFLE_CONFIG_GOLDEN.put(1, "ShuffleSerdeFormat");
        SHUFFLE_CONFIG_GOLDEN.put(2, "string");
        SHUFFLE_CONFIG_GOLDEN.put(3, "int32");
        SHUFFLE_CONFIG_GOLDEN.put(4, "ShufflePersistenceMode");
        SHUFFLE_CONFIG_GOLDEN.put(5, "int32");
        SHUFFLE_CONFIG_GOLDEN.put(6, "repeated int32");
        // Field 7 (max_buffered_batches_per_partition) is native-only; the Java side does not
        // declare it yet, so it is excluded from the cross-boundary alignment intersection.
        SHUFFLE_CONFIG_GOLDEN.put(7, "int32");
    }

    private static final Map<Integer, String> SPLIT_INFO_GOLDEN = new LinkedHashMap<>();
    static {
        SPLIT_INFO_GOLDEN.put(1, "string");
        SPLIT_INFO_GOLDEN.put(2, "string");
        SPLIT_INFO_GOLDEN.put(3, "string");
        SPLIT_INFO_GOLDEN.put(4, "FileFormat");
        SPLIT_INFO_GOLDEN.put(5, "int64");
        SPLIT_INFO_GOLDEN.put(6, "int64");
        SPLIT_INFO_GOLDEN.put(7, "repeated ColumnMapping");
        SPLIT_INFO_GOLDEN.put(8, "map<string,string>");
        SPLIT_INFO_GOLDEN.put(9, "IcebergSplitInfo");   // oneof connector_info
        SPLIT_INFO_GOLDEN.put(10, "PaimonSplitInfo");   // oneof connector_info
        SPLIT_INFO_GOLDEN.put(11, "SchemaEvolution");
        SPLIT_INFO_GOLDEN.put(12, "string");
        SPLIT_INFO_GOLDEN.put(13, "repeated IndexEvalHintInfo");
        SPLIT_INFO_GOLDEN.put(14, "SplitConstraintInfo");
    }

    // ── 1. Native golden snapshot checks ──

    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto message field golden compatibility")
    void nativeTaskRequestFieldsMatchGolden(@ForAll("taskRequestEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(NATIVE_TASK_PROTO, "TaskRequest");
        assertThat(parsed)
                .as("Native TaskRequest field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto message field golden compatibility")
    void nativeWriteConfigFieldsMatchGolden(@ForAll("writeConfigEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(NATIVE_TASK_PROTO, "WriteConfig");
        assertThat(parsed)
                .as("Native WriteConfig field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto message field golden compatibility")
    void nativeResourceLimitFieldsMatchGolden(@ForAll("resourceLimitEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(NATIVE_COMMON_PROTO, "ResourceLimit");
        assertThat(parsed)
                .as("Native ResourceLimit field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto message field golden compatibility")
    void nativeShuffleConfigFieldsMatchGolden(@ForAll("shuffleConfigEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(NATIVE_SHUFFLE_PROTO, "ShuffleConfig");
        assertThat(parsed)
                .as("Native ShuffleConfig field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Property(tries = 10)
    @Tag("Feature: full-review-fix, Property: Proto message field golden compatibility")
    void nativeSplitInfoFieldsMatchGolden(@ForAll("splitInfoEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(NATIVE_SPLIT_PROTO, "SplitInfo");
        assertThat(parsed)
                .as("Native SplitInfo field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    // ── 2. Native ↔ Java cross-boundary alignment (name-blind, by field number) ──

    @Test
    void taskRequestFieldNumbersAlignAcrossBoundary() {
        assertFieldNumbersAlign(NATIVE_TASK_PROTO, JAVA_TASK_PROTO, "TaskRequest");
    }

    @Test
    void writeConfigFieldNumbersAlignAcrossBoundary() {
        assertFieldNumbersAlign(NATIVE_TASK_PROTO, JAVA_TASK_PROTO, "WriteConfig");
    }

    @Test
    void resourceLimitFieldNumbersAlignAcrossBoundary() {
        assertFieldNumbersAlign(NATIVE_COMMON_PROTO, JAVA_COMMON_PROTO, "ResourceLimit");
    }

    @Test
    void shuffleConfigFieldNumbersAlignAcrossBoundary() {
        // Native ShuffleConfig lives in shuffle.proto; the Java mirror lives in task.proto.
        // Java is a prefix subset (no field 7), so only the intersection is asserted.
        assertFieldNumbersAlign(NATIVE_SHUFFLE_PROTO, JAVA_TASK_PROTO, "ShuffleConfig");
    }

    @Test
    void splitInfoFieldNumbersAlignAcrossBoundary() {
        assertFieldNumbersAlign(NATIVE_SPLIT_PROTO, JAVA_TASK_PROTO, "SplitInfo");
    }

    /**
     * For every field number present on BOTH sides, the declared wire type must be identical.
     * Field numbers unique to one side (e.g. a native-only trailing field) are allowed, since proto3
     * tolerates unknown fields — but a shared number with a different type is a silent corruption bug.
     */
    private static void assertFieldNumbersAlign(Path nativeProto, Path javaProto, String message) {
        Map<Integer, String> nativeFields = parseMessageFields(nativeProto, message);
        Map<Integer, String> javaFields = parseMessageFields(javaProto, message);

        assertThat(nativeFields)
                .as("Native proto %s must declare at least one field", message)
                .isNotEmpty();
        assertThat(javaFields)
                .as("Java proto %s must declare at least one field", message)
                .isNotEmpty();

        TreeSet<Integer> shared = new TreeSet<>(nativeFields.keySet());
        shared.retainAll(javaFields.keySet());
        assertThat(shared)
                .as("%s must share at least one field number across the boundary", message)
                .isNotEmpty();

        for (Integer number : shared) {
            assertThat(javaFields.get(number))
                    .as("%s field %d: Java type '%s' must equal native type '%s'",
                            message, number, javaFields.get(number), nativeFields.get(number))
                    .isEqualTo(nativeFields.get(number));
        }
    }

    // ── 3. Negative self-test: the comparator must actually catch a drift ──

    @Test
    void parserDetectsFieldNumberDrift() {
        // Same logical message, but field 3's type was changed bytes -> string. A name-blind,
        // number-keyed comparison MUST flag this; otherwise the guard above is toothless.
        Map<Integer, String> good = parseMessageBody(
                "message M {\n  string a = 1;\n  int32 b = 2;\n  bytes c = 3;\n}", "M");
        Map<Integer, String> drifted = parseMessageBody(
                "message M {\n  string a = 1;\n  int32 b = 2;\n  string c = 3;\n}", "M");
        assertThat(good.get(3)).isEqualTo("bytes");
        assertThat(drifted.get(3)).isEqualTo("string");
        assertThat(good).isNotEqualTo(drifted);
    }

    @Test
    void parserHandlesRepeatedMapAndOneof() {
        Map<Integer, String> fields = parseMessageBody(
                "message M {\n"
                        + "  repeated Foo a = 1;\n"
                        + "  map<string, int64> b = 2;\n"
                        + "  reserved 3;\n"
                        + "  oneof choice {\n"
                        + "    Bar x = 4;\n"
                        + "    Baz y = 5;\n"
                        + "  }\n"
                        + "  string z = 6;\n"
                        + "}", "M");
        assertThat(fields).containsEntry(1, "repeated Foo");
        assertThat(fields).containsEntry(2, "map<string,int64>");
        assertThat(fields).doesNotContainKey(3); // reserved, not a field
        assertThat(fields).containsEntry(4, "Bar");
        assertThat(fields).containsEntry(5, "Baz");
        assertThat(fields).containsEntry(6, "string");
    }

    // ── Providers ──

    @Provide
    Arbitrary<Map.Entry<Integer, String>> taskRequestEntries() {
        return Arbitraries.of(new ArrayList<>(TASK_REQUEST_GOLDEN.entrySet()));
    }

    @Provide
    Arbitrary<Map.Entry<Integer, String>> writeConfigEntries() {
        return Arbitraries.of(new ArrayList<>(WRITE_CONFIG_GOLDEN.entrySet()));
    }

    @Provide
    Arbitrary<Map.Entry<Integer, String>> resourceLimitEntries() {
        return Arbitraries.of(new ArrayList<>(RESOURCE_LIMIT_GOLDEN.entrySet()));
    }

    @Provide
    Arbitrary<Map.Entry<Integer, String>> shuffleConfigEntries() {
        return Arbitraries.of(new ArrayList<>(SHUFFLE_CONFIG_GOLDEN.entrySet()));
    }

    @Provide
    Arbitrary<Map.Entry<Integer, String>> splitInfoEntries() {
        return Arbitraries.of(new ArrayList<>(SPLIT_INFO_GOLDEN.entrySet()));
    }

    // ── Proto message field parser ──

    private static final Pattern FIELD = Pattern.compile(
            "^\\s*(repeated\\s+)?(map\\s*<[^>]+>|[A-Za-z_][\\w.]*)\\s+[A-Za-z_]\\w*\\s*=\\s*(\\d+)\\s*;");

    private static Map<Integer, String> parseMessageFields(Path protoFile, String messageName) {
        try {
            return parseMessageBody(String.join("\n", Files.readAllLines(protoFile)), messageName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read proto file: " + protoFile, e);
        }
    }

    /**
     * Parse {@code (field-number -> normalized-type)} for a single message out of proto source text.
     * Handles {@code repeated}, {@code map<...>}, and {@code oneof} fields; skips {@code reserved}.
     * The returned type is name-blind and whitespace-normalized so it captures the wire contract.
     */
    static Map<Integer, String> parseMessageBody(String protoText, String messageName) {
        Map<Integer, String> result = new LinkedHashMap<>();
        String[] lines = protoText.split("\n", -1);
        Pattern messageStart = Pattern.compile(
                "(^|\\s)message\\s+" + Pattern.quote(messageName) + "\\s*\\{");
        int depth = -1; // -1 = not yet inside the target message
        for (String line : lines) {
            if (depth < 0) {
                if (messageStart.matcher(line).find()) {
                    depth = countChar(line, '{') - countChar(line, '}');
                }
                continue;
            }
            // Inside the target message (depth counts nested oneof braces too).
            Matcher m = FIELD.matcher(line);
            if (m.find()) {
                String type = (m.group(1) != null ? "repeated " : "")
                        + normalizeType(m.group(2));
                result.put(Integer.parseInt(m.group(3)), type);
            }
            depth += countChar(line, '{') - countChar(line, '}');
            if (depth <= 0) {
                break;
            }
        }
        return result;
    }

    private static String normalizeType(String type) {
        // Collapse all whitespace inside map<...> so "map<string, string>" == "map<string,string>".
        return type.replaceAll("\\s+", "");
    }

    private static int countChar(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) {
                n++;
            }
        }
        return n;
    }
}
