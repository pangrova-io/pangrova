package org.pangrova.connector.paimon.property.handle;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.connector.paimon.handle.PaimonLsmLevel;
import org.pangrova.connector.paimon.handle.PaimonPhysicalFile;
import org.pangrova.connector.paimon.handle.PaimonLsmFile;
import org.pangrova.connector.paimon.handle.PaimonSplit;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests verifying Paimon split payload correctness for native reader contract.
 *
 * <p>Validates:
 * <ul>
 *   <li>COW splits carry real file paths (not tableName) in proto</li>
 *   <li>MOR splits carry populated lsm_levels with real file paths</li>
 *   <li>Missing physical metadata is rejected instead of treating a table name as a file</li>
 * </ul>
 */
class PaimonSplitPayloadPropertyTest {

    @Property(tries = 10)
    void cowSplitProtoCarriesRealFilePath(
            @ForAll("cowSplits") PaimonSplit split) throws Exception {
        byte[] protoBytes = split.toSplitInfoProto();
        DataSplit info = DataSplit.parseFrom(protoBytes);

        assertThat(info.getOpaqueLocation())
                .as("COW split file_path must be the physical file, not tableName")
                .isEqualTo(split.primaryFilePath());
        assertThat(info.getOpaqueLocation())
                .doesNotContain(" ")
                .startsWith("/");
    }

    @Property(tries = 10)
    void morSplitProtoHasPopulatedLsmLevels(
            @ForAll("morSplits") PaimonSplit split) throws Exception {
        byte[] protoBytes = split.toSplitInfoProto();
        DataSplit info = DataSplit.parseFrom(protoBytes);

        assertThat(DataSplitProtoCodec.fileDetail(info).getPaimon().getLsmLevelsList())
                .as("MOR split must have populated lsm_levels")
                .isNotEmpty();
        assertThat(DataSplitProtoCodec.fileDetail(info).getPaimon().getLsmLevelsList())
                .allSatisfy(level -> {
                    assertThat(level.getFilesList()).isNotEmpty();
                    assertThat(level.getFilesList())
                            .allSatisfy(file -> assertThat(file.getOpaqueLocation()).startsWith("/"));
                    assertThat(level.getTotalRecordCount()).isPositive();
                });
        assertThat(DataSplitProtoCodec.fileDetail(info).getPaimon().getNeedsMerge()).isTrue();
    }

    @Property(tries = 10)
    void rejectsMissingPhysicalFilePath(@ForAll("cowSplits") PaimonSplit split) {
        assertThatThrownBy(() -> new PaimonPhysicalFile(
                "", "parquet", split.primaryFile().fileSize(), split.rowCount(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact path and format");
    }

    @Property(tries = 10)
    void splitProtoAlwaysHasSourceIdentity(@ForAll("allSplits") PaimonSplit split) throws Exception {
        byte[] protoBytes = split.toSplitInfoProto();
        DataSplit info = DataSplit.parseFrom(protoBytes);

        assertThat(info.getSourceId())
                .as("source_id must always be set in the generic split envelope")
                .isEqualTo(split.tableName())
                .isNotEmpty();
    }

    // ── Arbitraries ──

    @Provide
    Arbitrary<PaimonSplit> cowSplits() {
        return Combinators.combine(
                tableNames(),
                physicalFilePaths(),
                Arbitraries.longs().between(1, 100_000)
        ).as((tableName, filePath, rowCount) -> new PaimonSplit(
                tableName, 0L, rowCount,
                0, List.of(), "DEDUPLICATE", "NONE",
                1L, 0L, 0L, "APPEND", List.of(),
                false, false,
                Map.of(), false, Map.of(),
                List.of(), List.of(), new PaimonPhysicalFile(filePath, "parquet", 4096, rowCount, 0),
                new org.pangrova.connector.paimon.handle.PaimonMergePolicy(false, false)));
    }

    @Provide
    Arbitrary<PaimonSplit> morSplits() {
        return Combinators.combine(
                tableNames(),
                lsmLevelList(),
                Arbitraries.longs().between(1, 100_000)
        ).as((tableName, levels, rowCount) -> {
            String primaryPath = levels.isEmpty()
                    ? "/data/bucket-0/file.parquet"
                    : levels.get(0).filePaths().get(0);
            return new PaimonSplit(
                    tableName, 0L, rowCount,
                    0, List.of("id"), "DEDUPLICATE", "NONE",
                    1L, 0L, 0L, "PRIMARY_KEY", levels,
                    true, false,
                    Map.of(), false, Map.of(),
                    List.of(), List.of(), new PaimonPhysicalFile(primaryPath, "parquet", 8192, rowCount, 0),
                    new org.pangrova.connector.paimon.handle.PaimonMergePolicy(false, false));
        });
    }

    @Provide
    Arbitrary<PaimonSplit> allSplits() {
        return Arbitraries.oneOf(cowSplits(), morSplits());
    }

    private static Arbitrary<String> tableNames() {
        return Arbitraries.of("orders", "lineitem", "customer", "events", "metrics");
    }

    private static Arbitrary<String> physicalFilePaths() {
        return Combinators.combine(
                Arbitraries.of("/warehouse/db/orders", "/data/lake/events", "/store/metrics"),
                Arbitraries.integers().between(0, 9),
                Arbitraries.of("data-0.parquet", "data-1.parquet", "compact-0.parquet")
        ).as((base, bucket, file) -> base + "/bucket-" + bucket + "/" + file);
    }

    private static Arbitrary<List<PaimonLsmLevel>> lsmLevelList() {
        return lsmLevelArbitrary().list().ofMinSize(1).ofMaxSize(4);
    }

    private static Arbitrary<PaimonLsmLevel> lsmLevelArbitrary() {
        return Combinators.combine(
                Arbitraries.integers().between(0, 6),
                physicalFilePaths().list().ofMinSize(1).ofMaxSize(5),
                Arbitraries.longs().between(1, 50_000)
        ).as((level, paths, rows) -> new PaimonLsmLevel(level, paths.stream().map(path ->
                new PaimonLsmFile(new PaimonPhysicalFile(path, "parquet", 4096, rows, 0),
                        0, rows - 1, null, List.of(), List.of()))
                .toList(), rows));
    }
}
