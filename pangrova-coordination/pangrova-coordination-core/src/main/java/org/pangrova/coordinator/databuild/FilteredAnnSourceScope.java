package org.pangrova.coordinator.databuild;

import java.util.List;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;

/** Planning evidence from real enumerated immutable splits, before a Native task exists. */
final class FilteredAnnSourceScope {
    private FilteredAnnSourceScope() {}

    static boolean supports(List<DataSplit> splits, String sourceId, String snapshot) {
        if (splits.isEmpty() || sourceId.isBlank() || snapshot.isBlank()) return false;
        for (DataSplit split : splits) {
            if (!split.getSourceId().equals(sourceId) || !split.getSourceSnapshot().equals(snapshot)
                    || split.getDataObjectId().isBlank()) return false;
            var detail = DataSplitProtoCodec.fileDetail(split);
            if ((detail.getFileFormat() != FileFormat.FILE_FORMAT_PARQUET
                    && detail.getFileFormat() != FileFormat.FILE_FORMAT_ORC)
                    || detail.hasSystemTable() || (detail.hasPaimon() && detail.getPaimon().getNeedsMerge())) {
                return false;
            }
        }
        return true;
    }
}
