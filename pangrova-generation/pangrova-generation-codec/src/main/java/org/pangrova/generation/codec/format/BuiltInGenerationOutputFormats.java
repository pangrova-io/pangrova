package org.pangrova.generation.codec.format;

import org.pangrova.generation.api.output.GenerationOutputFormat;
import org.pangrova.generation.api.output.GenerationOutputRole;

import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;

import org.pangrova.common.api.annotation.Internal;

import java.util.Map;

/**
 * Canonical wire-format role layouts shared by admission and publication validation.
 * This is format metadata, not an execution provider registry; unknown formats fail closed.
 */
@Internal
public final class BuiltInGenerationOutputFormats {

    private static final Map<String, GenerationOutputFormat> CONTRACTS = Map.of(
            "pangrova.diskann.segment", new GenerationOutputFormat(
                    GenerationOutputTypeIds.VECTOR_INDEX,
                    Map.of(
                            "DISKANN_GRAPH", source(0),
                            "RAW_VECTORS", source(1),
                            "ROW_LOCATORS", source(2)),
                    Map.of(),
                    Map.of(), Map.of(), true),
            "pangrova.hnsw.segment", vectorContract("HNSW_GRAPH"),
            "pangrova.ivfpq.segment", vectorContract("IVF_PQ_INDEX"),
            "pangrova.bm25.segment", new GenerationOutputFormat(
                    GenerationOutputTypeIds.TEXT_INDEX,
                    Map.of(
                            "BM25_POSTINGS", source(0),
                            "ROW_LOCATORS", source(1),
                            "BM25_STATS_SHARD", workUnit(2)),
                    Map.of("BM25_STATS_SHARD", workUnit(0)),
                    Map.of("BM25_CORPUS_STATS", generation(0)), Map.of(), true),
            "pangrova.generated-values.segment", new GenerationOutputFormat(
                    GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                    Map.of(
                            "GENERATED_VALUES", source(0),
                            "ROW_LOCATORS", source(1)),
                    Map.of(),
                    Map.of(), Map.of(), false),
            "pangrova.tabular-export.arrow-stream", new GenerationOutputFormat(
                    GenerationOutputTypeIds.TABULAR_EXPORT,
                    Map.of(),
                    Map.of(),
                    Map.of(), Map.of("TABULAR_DATA", workUnit(0)), true));

    private BuiltInGenerationOutputFormats() {}

    /** Returns the exact current contract or fails closed for an unknown format. */
    public static GenerationOutputFormat require(String formatId, String artifactType) {
        GenerationOutputFormat contract = CONTRACTS.get(formatId);
        if (contract == null || !contract.artifactType().equals(artifactType)) {
            throw new IllegalStateException(
                    "artifact type and format have no executable contract");
        }
        return contract;
    }

    private static GenerationOutputRole source(int ordinal) {
        return new GenerationOutputRole(ordinal, GenerationOutputScope.SOURCE_OBJECT);
    }

    private static GenerationOutputRole workUnit(int ordinal) {
        return new GenerationOutputRole(ordinal, GenerationOutputScope.WORK_UNIT);
    }

    private static GenerationOutputRole generation(int ordinal) {
        return new GenerationOutputRole(ordinal, GenerationOutputScope.GENERATION);
    }

    private static GenerationOutputFormat vectorContract(String indexRole) {
        return new GenerationOutputFormat(
                GenerationOutputTypeIds.VECTOR_INDEX,
                Map.of(
                        indexRole, source(0),
                        "RAW_VECTORS", source(1),
                        "ROW_LOCATORS", source(2)),
                Map.of(),
                Map.of(), Map.of(), true);
    }

}
