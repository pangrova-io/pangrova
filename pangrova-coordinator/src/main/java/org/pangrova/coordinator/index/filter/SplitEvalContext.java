package org.pangrova.coordinator.index.filter;

import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexEvaluator;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.statistics.FileColumnStatistics;

import java.util.List;
import java.util.Map;

/**
 * Immutable context shared across all split evaluations within a single
 * {@link IndexSplitFilter#filterSplits} call.
 *
 * <p>All fields are resolved once at the top of filterSplits and reused
 * for every split evaluation, avoiding repeated accessor/metadata lookups.</p>
 *
 * <p>Since L2 evaluation has been moved to the worker, the coordinator no
 * longer needs a {@code FileSystemAccessor}. This context carries only the
 * metadata and native evaluator references needed for L0/L1 pruning and
 * hint generation.</p>
 *
 * @param nativeEvaluator      connector-provided native index evaluator (nullable)
 * @param nativeCapabilities   native index capabilities (empty if no evaluator)
 * @param indexFileMappings    batch-prefetched: indexId → (dataFilePath → indexFilePath)
 * @param descriptorsByIndex   batch-prefetched: indexId → IndexDescriptor
 * @param querySnapshotToken   snapshot token for consistency filtering (nullable)
 * @param fileStatistics       L0 column statistics: dataFilePath → column stats (nullable)
 * @param limitK               query LIMIT value; 0 means no LIMIT
 * @param deleteFilesByPath    set of data file paths that have associated delete files (nullable)
 * @param totalDataFiles       total number of distinct data files in the scan (for coverage calculation)
 */
public record SplitEvalContext(
        NativeIndexEvaluator nativeEvaluator,
        List<NativeIndexCapability> nativeCapabilities,
        Map<String, Map<String, String>> indexFileMappings,
        Map<String, IndexDescriptor> descriptorsByIndex,
        String querySnapshotToken,
        Map<String, List<FileColumnStatistics>> fileStatistics,
        long limitK,
        java.util.Set<String> deleteFilesByPath,
        int totalDataFiles) {

    public SplitEvalContext {
        nativeCapabilities = nativeCapabilities != null
                ? List.copyOf(nativeCapabilities) : List.of();
        indexFileMappings = indexFileMappings != null
                ? Map.copyOf(indexFileMappings) : Map.of();
        descriptorsByIndex = descriptorsByIndex != null
                ? Map.copyOf(descriptorsByIndex) : Map.of();
        deleteFilesByPath = deleteFilesByPath != null
                ? java.util.Set.copyOf(deleteFilesByPath) : java.util.Set.of();
    }

    /** Returns true if the given data file path has associated delete files. */
    public boolean hasDeleteFiles(String dataFilePath) {
        return deleteFilesByPath.contains(dataFilePath);
    }
}
