package org.pangrova.testing.property;

import org.pangrova.testing.testkit.TestProjectPaths;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden tests for canonical protobuf <em>message field numbers and types</em>.
 *
 * <p>The sibling {@link ProtoEnumGoldenPropertyTest} pins enum (name, number) pairs. That guard alone
 * leaves message field numbers unprotected: a field-number or wire-type drift on a
 * cross-boundary message (TaskRequest, WriteConfig, ResourceLimit, ShuffleConfig, SplitInfo) would
 * silently corrupt deserialization without any test failing (F-PROTO-01).
 *
 * <p>Each canonical cross-runtime message's (field-number → wire-type) map is
 * pinned. Maven and CMake consume this same file; the descriptor handshake and
 * Native capability tests prove that the generated runtimes use the same
 * dependency-complete descriptor set.
 *
 * <p><b>Validates: Requirements R45.3 (T13.3) · Finding F-PROTO-01</b></p>
 */
class ProtoMessageFieldGoldenPropertyTest {

    private static final Path ROOT = TestProjectPaths.findProjectRoot();

    private static final Path CANONICAL_TASK_PROTO = ROOT.resolve(
            "pangrova-protocol/src/main/proto/pangrova/protocol/v2/task.proto");
    private static final Path CANONICAL_COMMON_PROTO = ROOT.resolve(
            "pangrova-protocol/src/main/proto/pangrova/protocol/v2/execution_binding.proto");

    // ── Golden (field-number → wire-type) snapshots of the canonical source ──

    private static final Map<Integer, String> TASK_REQUEST_GOLDEN = new LinkedHashMap<>();
    static {
        TASK_REQUEST_GOLDEN.put(1, "ProtocolVersion");
        TASK_REQUEST_GOLDEN.put(2, "TaskAttemptId");
        TASK_REQUEST_GOLDEN.put(3, "TaskKind");
        TASK_REQUEST_GOLDEN.put(4, "SecurityContext");
        TASK_REQUEST_GOLDEN.put(5, "bytes");
        TASK_REQUEST_GOLDEN.put(6, "Checksum");
        TASK_REQUEST_GOLDEN.put(7, "repeated DataSplit");
        TASK_REQUEST_GOLDEN.put(8, "repeated OperatorBinding");
        TASK_REQUEST_GOLDEN.put(9, "StageResourceGrant");
        TASK_REQUEST_GOLDEN.put(10, "uint64");
        TASK_REQUEST_GOLDEN.put(11, "TraceContext");
        TASK_REQUEST_GOLDEN.put(12, "string");
        TASK_REQUEST_GOLDEN.put(13, "TaskExecutionOptions");
    }

    private static final Map<Integer, String> WRITE_CONFIG_GOLDEN = new LinkedHashMap<>();
    static {
        WRITE_CONFIG_GOLDEN.put(1, "string");
        WRITE_CONFIG_GOLDEN.put(2, "string");
        WRITE_CONFIG_GOLDEN.put(3, "repeated string");
        WRITE_CONFIG_GOLDEN.put(4, "FileFormat");
        WRITE_CONFIG_GOLDEN.put(5, "int64");
    }

    private static final Map<Integer, String> RESOURCE_LIMIT_GOLDEN = new LinkedHashMap<>();
    static {
        RESOURCE_LIMIT_GOLDEN.put(1, "string");
        RESOURCE_LIMIT_GOLDEN.put(2, "uint32");
        RESOURCE_LIMIT_GOLDEN.put(3, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(4, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(5, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(6, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(7, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(8, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(9, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(10, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(11, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(12, "uint32");
        RESOURCE_LIMIT_GOLDEN.put(15, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(16, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(18, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(19, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(20, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(21, "uint64");
        RESOURCE_LIMIT_GOLDEN.put(22, "uint64");
    }

    private static final Map<Integer, String> SHUFFLE_CONFIG_GOLDEN = new LinkedHashMap<>();
    static {
        SHUFFLE_CONFIG_GOLDEN.put(1, "BatchSerde");
        SHUFFLE_CONFIG_GOLDEN.put(2, "string");
        SHUFFLE_CONFIG_GOLDEN.put(3, "uint32");
        SHUFFLE_CONFIG_GOLDEN.put(4, "ShufflePersistenceMode");
        SHUFFLE_CONFIG_GOLDEN.put(5, "uint32");
        SHUFFLE_CONFIG_GOLDEN.put(6, "repeated int32");
    }

    private static final Map<Integer, String> SPLIT_INFO_GOLDEN = new LinkedHashMap<>();
    static {
        SPLIT_INFO_GOLDEN.put(1, "string");
        SPLIT_INFO_GOLDEN.put(2, "string");
        SPLIT_INFO_GOLDEN.put(3, "string");
        SPLIT_INFO_GOLDEN.put(4, "string");
        SPLIT_INFO_GOLDEN.put(5, "string");
        SPLIT_INFO_GOLDEN.put(6, "string");
        SPLIT_INFO_GOLDEN.put(7, "uint64");
        SPLIT_INFO_GOLDEN.put(8, "uint64");
        SPLIT_INFO_GOLDEN.put(9, "repeated PartitionValue");
        SPLIT_INFO_GOLDEN.put(10, "google.protobuf.Any");
    }

    // ── Canonical golden snapshot checks ──

    @Property(tries = 10)
    @Tag("proto-golden")
    void canonicalTaskRequestFieldsMatchGolden(@ForAll("taskRequestEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(CANONICAL_TASK_PROTO, "TaskRequest");
        assertThat(parsed)
                .as("Canonical TaskRequest field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Property(tries = 10)
    @Tag("proto-golden")
    void canonicalWriteConfigFieldsMatchGolden(@ForAll("writeConfigEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(CANONICAL_TASK_PROTO, "WriteTaskOptions");
        assertThat(parsed)
                .as("Canonical WriteConfig field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Property(tries = 10)
    @Tag("proto-golden")
    void canonicalResourceLimitFieldsMatchGolden(@ForAll("resourceLimitEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(CANONICAL_COMMON_PROTO, "StageResourceGrant");
        assertThat(parsed)
                .as("Canonical ResourceLimit field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Test
    void canonicalResourceGrantHasTheCompleteLiveInventoryAndReservedRetiredFields() throws IOException {
        var source = Files.readString(CANONICAL_COMMON_PROTO);
        assertThat(parseMessageBody(source, "StageResourceGrant")).isEqualTo(RESOURCE_LIMIT_GOLDEN);
        var start = source.indexOf("message StageResourceGrant {");
        assertThat(start).isNotNegative();
        var end = source.indexOf('}', start);
        assertThat(end).isGreaterThan(start);
        var body = source.substring(start, end);
        assertThat(body).contains("reserved 13, 14, 17;");
        assertThat(body).contains("reserved \"provider_input_tokens\", \"provider_output_tokens\", \"provider_invocations\";");
    }

    @Property(tries = 10)
    @Tag("proto-golden")
    void canonicalShuffleConfigFieldsMatchGolden(@ForAll("shuffleConfigEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(CANONICAL_TASK_PROTO, "ShuffleTaskOptions");
        assertThat(parsed)
                .as("Canonical ShuffleConfig field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    @Property(tries = 10)
    @Tag("proto-golden")
    void canonicalSplitInfoFieldsMatchGolden(@ForAll("splitInfoEntries") Map.Entry<Integer, String> e) {
        Map<Integer, String> parsed = parseMessageFields(CANONICAL_TASK_PROTO, "DataSplit");
        assertThat(parsed)
                .as("Canonical SplitInfo field %d must be type '%s'", e.getKey(), e.getValue())
                .containsEntry(e.getKey(), e.getValue());
    }

    // ── Negative self-test: the parser must actually expose a drift ──

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
