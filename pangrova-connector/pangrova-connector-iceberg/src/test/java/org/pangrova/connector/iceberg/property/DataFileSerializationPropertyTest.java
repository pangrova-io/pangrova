package org.pangrova.connector.iceberg.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for Iceberg DataFile serialization round-trip via
 * {@link ContentFileParser}. Verifies that arbitrary DataFile descriptors
 * survive a toJson/fromJson cycle with path, format, recordCount, and
 * fileSizeInBytes preserved exactly.
 *
 * <p><b>Validates: Requirements 2.8, 10.2</b></p>
 */
class DataFileSerializationPropertyTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * For any valid DataFile with arbitrary path characters (including quotes,
     * backslashes, control characters, and Unicode), round-tripping through
     * {@code ContentFileParser.toJson()} and {@code ContentFileParser.fromJson()}
     * preserves path, format, recordCount, and fileSizeInBytes exactly.
     *
     * <p><b>Validates: Requirements 2.8, 10.2</b></p>
     */
    @Property(tries = 200)
    @Tag("Feature_data-pipeline-post-fix-review")
    @Tag("Property_DataFile_serialization_round-trip")
    void dataFile_roundTrip_preservesAllFields(
            @ForAll("dataFileInputs") DataFileInput input) throws Exception {

        DataFile original = DataFiles.builder(UNPARTITIONED)
                .withPath(input.path())
                .withFormat(input.format())
                .withRecordCount(input.recordCount())
                .withFileSizeInBytes(input.fileSizeInBytes())
                .build();

        String json = ContentFileParser.toJson(original, UNPARTITIONED);
        JsonNode jsonNode = MAPPER.readTree(json);
        DataFile restored = (DataFile) ContentFileParser.fromJson(jsonNode, UNPARTITIONED);

        assertThat(restored.path().toString())
                .as("path must survive round-trip")
                .isEqualTo(original.path().toString());
        assertThat(restored.format())
                .as("format must survive round-trip")
                .isEqualTo(original.format());
        assertThat(restored.recordCount())
                .as("recordCount must survive round-trip")
                .isEqualTo(original.recordCount());
        assertThat(restored.fileSizeInBytes())
                .as("fileSizeInBytes must survive round-trip")
                .isEqualTo(original.fileSizeInBytes());
    }

    @Provide
    Arbitrary<DataFileInput> dataFileInputs() {
        Arbitrary<String> paths = specialCharPaths();
        Arbitrary<FileFormat> formats = Arbitraries.of(
                FileFormat.PARQUET, FileFormat.ORC, FileFormat.AVRO);
        Arbitrary<Long> recordCounts = Arbitraries.longs().between(0, 10_000_000L);
        Arbitrary<Long> fileSizes = Arbitraries.longs().between(1, 1_000_000_000L);

        return Combinators.combine(paths, formats, recordCounts, fileSizes)
                .as(DataFileInput::new);
    }

    /**
     * Generates file paths including special characters that stress JSON
     * escaping: quotes, backslashes, control characters, and Unicode.
     */
    private Arbitrary<String> specialCharPaths() {
        Arbitrary<String> normalPaths = Arbitraries.strings()
                .ofMinLength(1).ofMaxLength(200)
                .alpha().numeric()
                .withChars('/', '-', '_', '.');

        Arbitrary<String> specialChars = Arbitraries.of(
                "/data/file with spaces.parquet",
                "/data/file\"quoted\".parquet",
                "/data/back\\slash.parquet",
                "/data/tab\there.parquet",
                "/data/newline\nhere.parquet",
                "/data/unicode-日本語.parquet",
                "/data/emoji-\uD83D\uDE00.parquet",
                "/data/mixed\"quote\\back\ttab.parquet",
                "/data/control\u0001\u001F.parquet",
                "/data/single'quote.parquet",
                "/data/ampersand&equals=.parquet",
                "/data/curly{brace}.parquet",
                "/data/angle<bracket>.parquet"
        );

        Arbitrary<String> randomSpecial = Arbitraries.strings()
                .ofMinLength(1).ofMaxLength(100)
                .withChars('"', '\\', '\t', '\n', '\r', '\u001F',
                        '/', '.', ' ', 'a', 'z', '0', '9');

        return Arbitraries.frequencyOf(
                net.jqwik.api.Tuple.of(3, normalPaths),
                net.jqwik.api.Tuple.of(4, specialChars),
                net.jqwik.api.Tuple.of(3, randomSpecial)
        );
    }

    /**
     * Carrier record for jqwik-generated DataFile construction parameters.
     */
    record DataFileInput(String path, FileFormat format,
                         long recordCount, long fileSizeInBytes) {}
}
