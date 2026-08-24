package org.pangrova.coordinator.query.execution.result;

import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.spi.index.vector.VectorSearchResult.ScoredRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Materializes ANN result rows from their row-identity coordinates.
 *
 * <p>After the coordinator collects per-worker Top-K results and merges them
 * into a global Top-K via {@link org.pangrova.coordinator.ai.vector.GlobalTopKMerger},
 * each {@link ScoredRow}
 * carries {@code (fileId, rowOffset, distance)} as its row identity. This
 * class groups the global result by source file so callers can efficiently
 * batch-read rows from each data file.</p>
 *
 * <p>The output {@link MaterializedRow} pairs each scored row with its
 * source-file coordinates for downstream projection — the actual I/O to
 * read column values from the data file is performed by the connector's
 * {@code ColumnValueReader} or by a follow-up scan restricted to the
 * matching row offsets.</p>
 */
public final class VectorRowMaterializer {

    private static final Logger LOG = LoggerFactory.getLogger(VectorRowMaterializer.class);

    private VectorRowMaterializer() {}

    /**
     * Groups global Top-K scored rows by source data file for batch reading.
     *
     * @param merged the globally merged Top-K result from {@link org.pangrova.coordinator.ai.vector.GlobalTopKMerger}
     * @return map of data file path to rows within that file, sorted by rowOffset
     */
    public static Map<String, List<MaterializedRow>> groupByFile(VectorSearchResult merged) {
        Map<String, List<MaterializedRow>> byFile = new HashMap<>();
        int rank = 0;
        String fileId = merged.dataFilePath();
        for (ScoredRow row : merged.neighbors()) {
            if (fileId == null || fileId.isEmpty()) {
                LOG.warn("event=vector_row_missing_file_id rowOffset={} distance={} "
                        + "action=skip_materialization", row.rowOffset(), row.distance());
                rank++;
                continue;
            }
            byFile.computeIfAbsent(fileId, k -> new ArrayList<>())
                    .add(new MaterializedRow(fileId, row.rowOffset(),
                            row.distance(), rank));
            rank++;
        }
        // Sort within each file by rowOffset for sequential I/O
        for (List<MaterializedRow> rows : byFile.values()) {
            rows.sort((a, b) -> Long.compare(a.rowOffset(), b.rowOffset()));
        }
        LOG.info("event=vector_rows_grouped totalRows={} files={}",
                merged.neighbors().size(), byFile.size());
        return byFile;
    }

    /**
     * Extracts the list of row offsets per file for batch read filtering.
     *
     * @param grouped output of {@link #groupByFile(VectorSearchResult)}
     * @return map of data file path to sorted list of row offsets
     */
    public static Map<String, List<Long>> extractRowOffsets(
            Map<String, List<MaterializedRow>> grouped) {
        Map<String, List<Long>> offsets = new HashMap<>(grouped.size());
        for (var entry : grouped.entrySet()) {
            offsets.put(entry.getKey(),
                    entry.getValue().stream()
                            .map(MaterializedRow::rowOffset)
                            .toList());
        }
        return offsets;
    }

    /**
     * A single materialized row from the global Top-K merge, carrying
     * both the file-level row identity for data retrieval and the global
     * rank + distance for result ordering.
     *
     * @param fileId    source data file path
     * @param rowOffset 0-based row position within the data file
     * @param distance  distance from query vector (lower = more similar)
     * @param globalRank 0-based rank in the global Top-K (distance-ascending)
     */
    public record MaterializedRow(String fileId, long rowOffset,
                                   double distance, int globalRank) {}
}
