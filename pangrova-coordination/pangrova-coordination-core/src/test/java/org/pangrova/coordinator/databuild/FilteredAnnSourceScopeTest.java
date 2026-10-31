package org.pangrova.coordinator.databuild;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.PaimonSplitDetail;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Canonical planning contract checks; actual provider execution is covered by SQL E2E. */
class FilteredAnnSourceScopeTest {
    @Test
    void onlyImmutableDwioScopesWithOneExactSnapshotAreEligible() {
        assertThat(FilteredAnnSourceScope.supports(List.of(split(FileFormat.FILE_FORMAT_PARQUET),
                split(FileFormat.FILE_FORMAT_ORC)), "source", "snapshot")).isTrue();
        for (FileFormat format : List.of(FileFormat.FILE_FORMAT_LANCE,
                FileFormat.FILE_FORMAT_METADATA_JSON, FileFormat.FILE_FORMAT_UNSPECIFIED)) {
            assertThat(FilteredAnnSourceScope.supports(List.of(split(format)), "source", "snapshot")).isFalse();
        }
        assertThat(FilteredAnnSourceScope.supports(List.of(), "source", "snapshot")).isFalse();
    }

    @Test
    void snapshotSourceObjectAndMergeEvidenceAreRequiredBeforeTaskIssuance() {
        DataSplit valid = split(FileFormat.FILE_FORMAT_PARQUET);
        for (DataSplit invalid : List.of(valid.toBuilder().setSourceSnapshot("other").build(),
                valid.toBuilder().setSourceId("other").build(), valid.toBuilder().clearDataObjectId().build())) {
            assertThat(FilteredAnnSourceScope.supports(List.of(invalid), "source", "snapshot")).isFalse();
        }
        var merging = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                .setPaimon(PaimonSplitDetail.newBuilder().setNeedsMerge(true)).build();
        var merge = valid.toBuilder().setConnectorDetail(com.google.protobuf.Any.pack(merging)).build();
        assertThat(FilteredAnnSourceScope.supports(List.of(merge), "source", "snapshot")).isFalse();
    }

    private static DataSplit split(FileFormat format) {
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput("connector",
                "source", "snapshot", "object", "file:///immutable-object", 0, 32, Map.of(),
                FileSplitDetail.newBuilder().setFileFormat(format).build()));
    }
}
