package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonLsmLevel;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts physical data file paths and LSM level descriptors from Paimon splits.
 *
 * <p>Paimon stores data files under a path structure:
 * {@code {tableLocation}/{partitionPath}/bucket-{bucketId}/{fileName}}.
 * This extractor resolves the full physical paths needed by the native reader.</p>
 */
final class PaimonDataFileExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDataFileExtractor.class);

    private PaimonDataFileExtractor() {}

    /**
     * Extracts the table's physical base location from a Paimon Table.
     *
     * @return table location URI, or empty string if not a FileStoreTable
     */
    static String extractTableLocation(Table table) {
        if (table instanceof FileStoreTable fileStoreTable) {
            return fileStoreTable.location().toString();
        }
        return "";
    }

    /**
     * Builds the full physical file path for a data file within a Paimon split.
     *
     * <p>Path structure: {@code {tableLocation}/{partitionPath}/bucket-{bucketId}/{fileName}}</p>
     *
     * @param tableLocation  table base directory
     * @param dataSplit      Paimon data split
     * @param fileName       data file name
     * @param partitionValues partition column name-value pairs (empty for non-partitioned)
     */
    static String buildFilePath(
            String tableLocation, DataSplit dataSplit,
            String fileName, Map<String, String> partitionValues) {
        StringBuilder path = new StringBuilder(tableLocation);
        appendPartitionPath(path, partitionValues);
        path.append("/bucket-").append(dataSplit.bucket());
        path.append('/').append(fileName);
        return path.toString();
    }

    /**
     * Extracts the primary data file path for COW / read-optimized reads.
     * Returns the path of the first data file in the split.
     *
     * @param tableLocation   table base directory
     * @param split           Paimon split
     * @param partitionValues partition column name-value pairs (empty for non-partitioned)
     * @return full file path, or empty string if no data files
     */
    static String extractPrimaryFilePath(
            String tableLocation, Split split, Map<String, String> partitionValues) {
        if (!(split instanceof DataSplit dataSplit)) {
            return "";
        }
        List<DataFileMeta> dataFiles = dataSplit.dataFiles();
        if (dataFiles == null || dataFiles.isEmpty()) {
            return "";
        }
        return buildFilePath(tableLocation, dataSplit,
                dataFiles.get(0).fileName(), partitionValues);
    }

    /**
     * Builds LSM level descriptors from a Paimon DataSplit's data files,
     * grouped by {@link DataFileMeta#level()}.
     *
     * @param tableLocation   table base directory
     * @param split           Paimon split
     * @param partitionValues partition column name-value pairs (empty for non-partitioned)
     * @return list of LSM levels with resolved physical file paths; empty if not a DataSplit
     */
    static List<PaimonLsmLevel> extractLsmLevels(
            String tableLocation, Split split, Map<String, String> partitionValues) {
        if (!(split instanceof DataSplit dataSplit)) {
            return List.of();
        }
        List<DataFileMeta> dataFiles = dataSplit.dataFiles();
        if (dataFiles == null || dataFiles.isEmpty()) {
            return List.of();
        }

        Map<Integer, LevelAccumulator> levelMap =
                accumulateByLevel(tableLocation, dataSplit, dataFiles, partitionValues);
        List<PaimonLsmLevel> levels = toLsmLevels(levelMap);

        LOG.debug("event=paimon_lsm_levels_extracted level_count={} total_files={}",
                levels.size(), dataFiles.size());
        return levels;
    }

    private static Map<Integer, LevelAccumulator> accumulateByLevel(
            String tableLocation, DataSplit dataSplit,
            List<DataFileMeta> dataFiles, Map<String, String> partitionValues) {
        // Sort files by minSequenceNumber ascending within each level so that
        // older files get lower reader indices and newer files get higher ones.
        // The native DeduplicateMergeReader uses reader_index as synthetic
        // sequence number when no explicit sequence column is configured —
        // higher reader_index wins the dedup tie-break for equal primary keys.
        List<DataFileMeta> sorted = new ArrayList<>(dataFiles);
        sorted.sort(Comparator.comparingLong(DataFileMeta::minSequenceNumber));

        Map<Integer, LevelAccumulator> levelMap = new LinkedHashMap<>();
        for (DataFileMeta file : sorted) {
            String fullPath = buildFilePath(
                    tableLocation, dataSplit, file.fileName(), partitionValues);
            levelMap.computeIfAbsent(file.level(), LevelAccumulator::new)
                    .addFile(fullPath, file.rowCount());
        }
        return levelMap;
    }

    private static List<PaimonLsmLevel> toLsmLevels(Map<Integer, LevelAccumulator> levelMap) {
        List<PaimonLsmLevel> levels = new ArrayList<>(levelMap.size());
        for (LevelAccumulator acc : levelMap.values()) {
            levels.add(new PaimonLsmLevel(
                    acc.level, List.copyOf(acc.filePaths), acc.totalRecordCount));
        }
        // Sort by level descending: higher levels (older/compacted data) first,
        // level 0 (newest data) last. The native merge reader iterates levels
        // in order, assigning ascending reader indices. With this ordering,
        // newer files get higher reader indices and win the dedup tie-break
        // when no explicit sequence column is configured.
        levels.sort((a, b) -> Integer.compare(b.level(), a.level()));
        return List.copyOf(levels);
    }

    /**
     * Appends the partition path segment ({@code /key=value}) to the path builder.
     * For non-partitioned tables (empty map), no segment is appended.
     */
    private static void appendPartitionPath(
            StringBuilder path, Map<String, String> partitionValues) {
        if (partitionValues.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            path.append('/').append(entry.getKey()).append('=').append(entry.getValue());
        }
    }

    private static final class LevelAccumulator {
        final int level;
        final List<String> filePaths = new ArrayList<>();
        long totalRecordCount;

        LevelAccumulator(int level) {
            this.level = level;
        }

        void addFile(String path, long rowCount) {
            filePaths.add(path);
            totalRecordCount += rowCount;
        }
    }
}
