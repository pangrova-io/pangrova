package org.pangrova.indexing.api.evaluation;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.predicate.IndexPredicate;

import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;

/**
 * Lightweight hint attached to a split by the coordinator, instructing the
 * worker to perform L2 index evaluation before reading data.
 *
 * <p>Contains the minimum information needed for worker-side evaluation:
 * the index identity, the predicate to evaluate, and the index file path
 * for this specific data file.</p>
 *
 * @param indexId       index identifier
 * @param indexType     index type (for engine lookup)
 * @param predicate     predicate to evaluate
 * @param indexFilePath path to the index file for this data file
 * @param generation    published index generation
 * @param indexSizeBytes size of the complete published artifact
 * @param checksum SHA-256 of the complete published artifact, as 64 hex digits
 */
@Experimental
public record IndexEvalHint(
        String indexId,
        IndexType indexType,
        IndexPredicate predicate,
        String indexFilePath,
        int generation,
        long indexSizeBytes,
        String checksum) {

    public IndexEvalHint {
        Objects.requireNonNull(indexId, "indexId");
        Objects.requireNonNull(indexType, "indexType");
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(indexFilePath, "indexFilePath");
        if (generation < 0 || indexSizeBytes < 0) {
            throw new IllegalArgumentException("generation and indexSizeBytes must be non-negative");
        }
        if (checksum == null || checksum.length() != 64 || !checksum.chars().allMatch(HexFormat::isHexDigit)) {
            throw new IllegalArgumentException("checksum must be a SHA-256 hex digest");
        }
        checksum = checksum.toLowerCase(Locale.ROOT);
    }
}
