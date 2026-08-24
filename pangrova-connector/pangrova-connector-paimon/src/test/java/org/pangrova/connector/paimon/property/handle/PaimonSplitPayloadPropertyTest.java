package org.pangrova.connector.paimon.property.handle;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.connector.paimon.handle.PaimonLsmLevel;
import org.pangrova.connector.paimon.handle.PaimonSplit;
import org.pangrova.protocol.v1.SplitInfo;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests verifying Paimon split payload correctness for native reader contract.
 *
 * <p>Validates:
 * <ul>
 *   <li>COW splits carry real file paths (not tableName) in proto</li>
 *   <li>MOR splits carry populated lsm_levels with real file paths</li>
 *   <li>resolveFilePath falls back to tableName only when primaryFilePath is empty</li>
 * </ul>
 */
class PaimonSplitPayloadPropertyTest {

    @Property(tries = 10)
    void cowSplitProtoCarriesRealFilePath(
            @ForAll("cowSplits") PaimonSplit split) throws Exception {
        byte[] protoBytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(protoBytes);

        assertThat(info.getFilePath())
                .as("COW split file_path must be the physical file, not tableName")
                .isEqualTo(split.primaryFilePath());
        assertThat(info.getFilePath())
                .doesNotContain(" ")
                .startsWith("/");
    }

    @Property(tries = 10)
    void morSplitProtoHasPopulatedLsmLevels(
            @ForAll("morSplits") PaimonSplit split) throws Exception {
        byte[] protoBytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(protoBytes);

        assertThat(info.getPaimonInfo().getLsmLevelsList())
                .as("MOR split must have populated lsm_levels")
                .isNotEmpty();
        assertThat(info.getPaimonInfo().getLsmLevelsList())
                .allSatisfy(level -> {
                    assertThat(level.getFilePathsList()).isNotEmpty();
                    assertThat(level.getFilePathsList())
                            .allSatisfy(path -> assertThat(path).startsWith("/"));
                    assertThat(level.getTotalRecordCount()).isPositive();
                });
        assertThat(info.getPaimonInfo().getNeedsMerge()).isTrue();
    }

    @Property(tries = 10)
    void resolveFilePathFallsBackToTableNameWhenEmpty(
            @ForAll("fallbackSplits") PaimonSplit split) throws Exception {
        byte[] protoBytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(protoBytes);

        assertThat(info.getFilePath())
                .as("When primaryFilePath is empty, file_path should fall back to tableName")
                .isEqualTo(split.tableName());
    }

    @Property(tries = 10)
    void splitProtoAlwaysHasTableName(@ForAll("allSplits") PaimonSplit split) throws Exception {
        byte[] protoBytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(protoBytes);

        assertThat(info.getTableName())
                .as("table_name must always be set in proto")
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
                1L, 0L, 0L, "FULL", List.of(),
                false, false, "",
                Map.of(), false, Map.of(),
                List.of(), List.of(), filePath));
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
                    1L, 0L, 0L, "FULL", levels,
                    true, false, "",
                    Map.of(), false, Map.of(),
                    List.of(), List.of(), primaryPath);
        });
    }

    @Provide
    Arbitrary<PaimonSplit> fallbackSplits() {
        return Combinators.combine(
                tableNames(),
                Arbitraries.longs().between(1, 100_000)
        ).as((tableName, rowCount) -> new PaimonSplit(
                tableName, 0L, rowCount,
                -1, List.of(), "DEDUPLICATE", "NONE",
                0L, 0L, 0L, "FULL", List.of(),
                false, false, "",
                Map.of(), false, Map.of(),
                List.of(), List.of(), ""));
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
        ).as(PaimonLsmLevel::new);
    }
}
