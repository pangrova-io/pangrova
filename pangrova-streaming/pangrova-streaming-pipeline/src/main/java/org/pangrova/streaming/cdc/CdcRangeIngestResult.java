package org.pangrova.streaming.cdc;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/** Actual bounded-range ingestion result; it is not proof of catalog publication. */
@Experimental
public record CdcRangeIngestResult(
        long consumedRecordCount,
        long decodedEnvelopeCount,
        List<byte[]> commitFragments) {

    public CdcRangeIngestResult {
        if (consumedRecordCount < 0 || decodedEnvelopeCount < 0) {
            throw new IllegalArgumentException("CDC ingest counts must be non-negative");
        }
        commitFragments = commitFragments == null
                ? List.of()
                : commitFragments.stream().map(byte[]::clone).toList();
    }

    @Override
    public List<byte[]> commitFragments() {
        return commitFragments.stream().map(byte[]::clone).toList();
    }
}
