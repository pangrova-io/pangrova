package org.pangrova.testing.e2e.connector;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual SDK manifests and Native grouping, including primitive logical-type boundaries. */
class IcebergPartitionTypesTest extends SqlE2ETestBase {
    private static final String CATALOG = "partition_types";
    private String name;

    @Override protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, "default", icebergWarehouse().toString()));
    }

    @BeforeEach void uniqueName() { name = "types_" + UUID.randomUUID().toString().replace("-", ""); }

    @AfterEach void drainAndDrop() {
        try {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(queryManager.getActiveQueryIds()).isEmpty();
                assertThat(queryManager.getActiveQueryCount()).isZero();
            });
        } finally {
            if (name != null) sqlRunner.executeDdl("DROP TABLE IF EXISTS " + table());
        }
    }

    private String table() { return CATALOG + ".default." + name; }

    private record Sample(String label, Type type, List<Object> values) {}

    private static Sample sample(String label, Type type, Object... values) {
        return new Sample(label, type, Arrays.asList(values));
    }

    private static Stream<Arguments> allPrimitiveCases() {
        var samples = List.of(
                sample("boolean", Types.BooleanType.get(), false, true, false, null),
                sample("int", Types.IntegerType.get(), Integer.MIN_VALUE, 0, Integer.MAX_VALUE, 0, null),
                sample("long", Types.LongType.get(), Long.MIN_VALUE, 0L, Long.MAX_VALUE, 0L, null),
                sample("date", Types.DateType.get(), -719162, -1, 0, 2932896, 0, null),
                sample("time", Types.TimeType.get(), 0L, 1L, 86_399_999_999L, 0L, null),
                sample("timestamp", Types.TimestampType.withoutZone(), -1L, 0L, 1_000_001L, 0L, null),
                sample("timestamptz", Types.TimestampType.withZone(), -1L, 0L, 1_000_001L, 0L, null),
                sample("timestamp_ns", Types.TimestampNanoType.withoutZone(), -1L, 0L, 1_000_000_001L, 0L, null),
                sample("uuid", Types.UUIDType.get(), new UUID(0, 0), new UUID(-1, -1),
                        UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), new UUID(0, 0), null),
                sample("binary", Types.BinaryType.get(), ByteBuffer.wrap(new byte[0]),
                        ByteBuffer.wrap(new byte[] {0, -1}), ByteBuffer.wrap(new byte[] {-128, 0, 1}),
                        ByteBuffer.wrap(new byte[0]), null),
                sample("fixed", Types.FixedType.ofLength(3), ByteBuffer.wrap(new byte[] {0, -1, 1}),
                        ByteBuffer.wrap(new byte[] {-128, 0, 1}), ByteBuffer.wrap(new byte[] {0, -1, 1}), null),
                sample("decimal9", Types.DecimalType.of(9, 2), new BigDecimal("-9999999.99"),
                        new BigDecimal("0.00"), new BigDecimal("9999999.99"), new BigDecimal("0.00"), null),
                sample("decimal38", Types.DecimalType.of(38, 6),
                        new BigDecimal("-99999999999999999999999999999999.999999"), new BigDecimal("0.000000"),
                        new BigDecimal("99999999999999999999999999999999.999999"), new BigDecimal("0.000000"), null));
        return samples.stream().flatMap(sample -> Stream.of(2, 3)
                .filter(version -> version == 3 || sample.type().typeId() != Type.TypeID.TIMESTAMP_NANO)
                .map(version -> Arguments.of(sample.label(), version, sample.type(), sample.values())));
    }

    static Stream<Arguments> primitiveCases() {
        return allPrimitiveCases().filter(args -> !List.of("date", "time", "uuid", "fixed").contains(args.get()[0]));
    }

    private static Stream<Arguments> cases(String label) {
        return allPrimitiveCases().filter(args -> label.equals(args.get()[0]));
    }

    static Stream<Arguments> dateCases() { return cases("date"); }

    @ParameterizedTest(name = "{0} v{1}")
    @MethodSource("dateCases")
    void datePartitionsMatchSdk(String label, int version, Type type, List<Object> values) throws Exception {
        assertPartitionValuesAndCountsMatchSdk(label, version, type, values);
    }

    static Stream<Arguments> timeCases() { return cases("time"); }

    @ParameterizedTest(name = "{0} v{1}")
    @MethodSource("timeCases")
    void timePartitionsMatchSdk(String label, int version, Type type, List<Object> values) throws Exception {
        assertPartitionValuesAndCountsMatchSdk(label, version, type, values);
    }

    static Stream<Arguments> uuidCases() { return cases("uuid"); }

    @ParameterizedTest(name = "{0} v{1}")
    @MethodSource("uuidCases")
    void uuidPartitionsMatchSdk(String label, int version, Type type, List<Object> values) throws Exception {
        assertPartitionValuesAndCountsMatchSdk(label, version, type, values);
    }

    static Stream<Arguments> fixedCases() { return cases("fixed"); }

    @ParameterizedTest(name = "{0} v{1}")
    @MethodSource("fixedCases")
    void fixedPartitionsMatchSdk(String label, int version, Type type, List<Object> values) throws Exception {
        assertPartitionValuesAndCountsMatchSdk(label, version, type, values);
    }

    @ParameterizedTest(name = "{0} v{1}")
    @MethodSource("primitiveCases")
    void actualManifestPartitionValuesAndCountsMatchSdk(String label, int version, Type type,
            List<Object> values) throws Exception {
        assertPartitionValuesAndCountsMatchSdk(label, version, type, values);
    }

    private void assertPartitionValuesAndCountsMatchSdk(String label, int version, Type type,
            List<Object> values) throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + table() + " (id BIGINT)"
                + " TBLPROPERTIES ('format-version' = '" + version + "')");
        var sdk = new HadoopTables(new Configuration()).load(
                icebergWarehouse().resolve("default").resolve(name).toString());
        sdk.updateSchema().addColumn("p", type).commit();
        sdk.updateSpec().addField("p").commit();
        var append = sdk.newFastAppend();
        // Metadata-only feature: real SDK Avro manifests, with no data-file payload reads.
        for (int i = 0; i < values.size(); i++) {
            var partition = new PartitionData(sdk.spec().partitionType());
            partition.set(0, values.get(i));
            append.appendFile(DataFiles.builder(sdk.spec()).withPartition(partition)
                    .withPath(sdk.location() + "/data/value-" + i + ".parquet")
                    .withRecordCount(i + 1).withFileSizeInBytes(100 + i).build());
        }
        append.commit();
        var metadata = MetadataTableUtils.createMetadataTableInstance(sdk, MetadataTableType.PARTITIONS);
        var expected = new ArrayList<Object>();
        try (var tasks = metadata.newScan().planFiles()) {
            for (var task : tasks) {
                try (var rows = ((DataTask) task).rows()) {
                    for (var row : rows) expected.add(sdkValue(metadata.schema().asStruct(), row));
                }
            }
        }
        var actual = sqlRunner.execute("SELECT * FROM " + table() + "$partitions");
        assertThat(actual).as(label + " v" + version).extracting(row -> normalize(row.toRawMap()))
                .containsExactlyInAnyOrderElementsOf(expected);
        var result = sqlRunner.execute("SELECT SUM(record_count) AS n FROM " + table() + "$partitions").get(0);
        assertThat(result.getLong("n")).isEqualTo((long) values.size() * (values.size() + 1) / 2);
    }

    private static Object sdkValue(Type type, Object value) {
        if (value == null) return null;
        return switch (type.typeId()) {
            case STRUCT -> {
                var output = new LinkedHashMap<String, Object>();
                var fields = type.asStructType().fields();
                for (int i = 0; i < fields.size(); i++) {
                    var field = fields.get(i);
                    output.put(field.name(), sdkValue(field.type(), ((StructLike) value).get(i, Object.class)));
                }
                yield output;
            }
            case DATE -> LocalDate.ofEpochDay((int) value);
            case TIME -> LocalTime.ofNanoOfDay((long) value * 1000);
            case TIMESTAMP -> timestamp((long) value, 1_000_000);
            case TIMESTAMP_NANO -> timestamp((long) value, 1_000_000_000);
            case STRING -> value.toString();
            default -> normalize(value);
        };
    }

    private static LocalDateTime timestamp(long value, long units) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(Math.floorDiv(value, units),
                Math.floorMod(value, units) * (1_000_000_000 / units)), ZoneOffset.UTC);
    }

    private static Object normalize(Object value) {
        if (value instanceof Map<?, ?> map) {
            var copy = new LinkedHashMap<Object, Object>();
            map.forEach((key, item) -> copy.put(key, normalize(item)));
            return copy;
        }
        if (value instanceof ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return HexFormat.of().formatHex(bytes);
        }
        if (value instanceof byte[] bytes) return HexFormat.of().formatHex(bytes);
        return value;
    }
}
