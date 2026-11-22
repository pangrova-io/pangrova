package org.pangrova.connector.paimon.scan;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.paimon.handle.PaimonLsmLevel;
import org.pangrova.connector.paimon.handle.PaimonLsmFile;
import org.pangrova.connector.paimon.handle.PaimonPhysicalFile;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
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
@Internal
public final class PaimonDataFileExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDataFileExtractor.class);

    private PaimonDataFileExtractor() {}

    /**
     * Extracts the table's physical base location from a Paimon Table.
     *
     * @return table location URI, or empty string if not a FileStoreTable
     */
    public static String extractTableLocation(Table table) {
        if (table instanceof FileStoreTable fileStoreTable) {
            return fileStoreTable.location().toString();
        }
        return "";
    }

    /**
     * Uses the SDK's exact bucket path or manifest-owned external file location.
     * Partition encoding and external-file ownership must not be reconstructed here.
     */
    public static String buildFilePath(DataSplit dataSplit, DataFileMeta file) {
        return file.externalPath().orElseGet(() -> dataSplit.bucketPath() + "/" + file.fileName());
    }

    /**
     * Builds LSM level descriptors from a Paimon DataSplit's data files,
     * grouped by {@link DataFileMeta#level()}.
     *
     * @param evolution       physical-to-target mappings for the selected scan
     * @param dataSplit       exact SDK split
     * @return list of LSM levels with resolved physical file paths; empty if not a DataSplit
     */
    public static List<PaimonLsmLevel> extractLsmLevels(
            PaimonPartitionExtractor.SchemaEvolution evolution, DataSplit dataSplit) {
        List<DataFileMeta> dataFiles = dataSplit.dataFiles();
        if (dataFiles == null || dataFiles.isEmpty()) {
            return List.of();
        }

        Map<Integer, LevelAccumulator> levelMap =
                accumulateByLevel(evolution, dataSplit, dataFiles);
        List<PaimonLsmLevel> levels = toLsmLevels(levelMap);

        LOG.debug("event=paimon_lsm_levels_extracted level_count={} total_files={}",
                levels.size(), dataFiles.size());
        return levels;
    }

    private static Map<Integer, LevelAccumulator> accumulateByLevel(
            PaimonPartitionExtractor.SchemaEvolution evolution, DataSplit dataSplit, List<DataFileMeta> dataFiles) {
        // Sort files by minSequenceNumber ascending within each level so that
        // older files get lower reader indices and newer files get higher ones.
        // The native DeduplicateMergeReader uses reader_index as synthetic
        // sequence number when no explicit sequence column is configured —
        // higher reader_index wins the dedup tie-break for equal primary keys.
        List<DataFileMeta> sorted = new ArrayList<>(dataFiles);
        sorted.sort(Comparator.comparingLong(DataFileMeta::minSequenceNumber));

        Map<Integer, LevelAccumulator> levelMap = new LinkedHashMap<>();
        for (DataFileMeta file : sorted) {
            String fullPath = buildFilePath(dataSplit, file);
            var mapping = evolution.forSource(file.schemaId());
            levelMap.computeIfAbsent(file.level(), LevelAccumulator::new)
                    .addFile(new PaimonLsmFile(new PaimonPhysicalFile(fullPath, file.fileFormat(),
                            file.fileSize(), file.rowCount(), file.schemaId()),
                            file.minSequenceNumber(), file.maxSequenceNumber(), deletionVector(dataSplit, file),
                            mapping.columnMappings(), mapping.typePromotions()));
        }
        return levelMap;
    }

    /** Resolves the manifest-aligned deletion vector for exactly one physical data file. */
    public static org.pangrova.connector.paimon.handle.PaimonDeletionVector deletionVector(
            DataSplit split, DataFileMeta file) {
        if (split.deletionFiles().isEmpty()) return null;
        var files = split.deletionFiles().orElseThrow();
        if (files.size() != split.dataFiles().size()) {
            throw new IllegalArgumentException("Paimon deletion files must align exactly with data files");
        }
        int index = split.dataFiles().indexOf(file);
        if (index < 0) throw new IllegalArgumentException("Physical file does not belong to Paimon split");
        var deletion = files.get(index);
        return deletion == null ? null : new org.pangrova.connector.paimon.handle.PaimonDeletionVector(
                deletion.path(), deletion.offset(), deletion.length(), deletion.cardinality(), file.rowCount());
    }

    private static List<PaimonLsmLevel> toLsmLevels(Map<Integer, LevelAccumulator> levelMap) {
        List<PaimonLsmLevel> levels = new ArrayList<>(levelMap.size());
        for (LevelAccumulator acc : levelMap.values()) {
            levels.add(new PaimonLsmLevel(
                    acc.level, List.copyOf(acc.files), acc.totalRecordCount));
        }
        // Sort by level descending: higher levels (older/compacted data) first,
        // level 0 (newest data) last. The native merge reader iterates levels
        // in order, assigning ascending reader indices. With this ordering,
        // newer files get higher reader indices and win the dedup tie-break
        // when no explicit sequence column is configured.
        levels.sort((a, b) -> Integer.compare(b.level(), a.level()));
        return List.copyOf(levels);
    }

    private static final class LevelAccumulator {
        final int level;
        final List<PaimonLsmFile> files = new ArrayList<>();
        long totalRecordCount;

        LevelAccumulator(int level) {
            this.level = level;
        }

        void addFile(PaimonLsmFile file) {
            files.add(file);
            totalRecordCount = Math.addExact(totalRecordCount, file.physical().rowCount());
        }
    }
}
