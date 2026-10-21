package org.pangrova.worker.copy;

import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.WorkerLocalCopyInput;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Objects;

/** Resolves one frozen source into the immediate Worker-to-JNI request, with no bearer material. */
public final class CopyNativeTaskInput {
    private CopyNativeTaskInput() {}

    /** Freezes one exact private input under its task cleanup owner and requested-read grant. */
    public static WorkerLocalCopyInput prepare(SubmitTaskRequest envelope, CopyStageLeases runtime,
                                      CopyMaterialization ownership) throws IOException {
        Objects.requireNonNull(runtime, "runtime");
        var request = envelope.taskRequest();
        if (envelope.copySourceRead() == null || !request.hasCopySourceStagingCommandSha256()
                || request.getSplitsCount() != 1) {
            throw new IllegalArgumentException("COPY Native input requires one exact private source binding");
        }
        var split = request.getSplits(0);
        if (!split.getConnectorDetail().is(FileSplitDetail.class)
                || DataSplitProtoCodec.fileDetail(split).getFileFormat() != FileFormat.FILE_FORMAT_PARQUET
                || split.hasCatalog() || split.getStartOffset() != 0 || split.getLength() <= 0
                || split.getSourceSnapshot().isBlank()
                || !split.getDataObjectId().equals(envelope.copySourceRead().scope().canonicalSourceUrl())) {
            throw new IllegalArgumentException("COPY Native input requires a frozen complete Parquet object");
        }
        var file = runtime.materialize(envelope, ownership);
        var proof = file.proof();
        if (!proof.sourceVersionId().equals(split.getSourceSnapshot()) || proof.contentSize() != split.getLength()) {
            throw new IllegalArgumentException("COPY Native input differs from its frozen source version or length");
        }
        return WorkerLocalCopyInput.newBuilder().setSplitId(split.getSplitId())
                .setSourceVersionId(proof.sourceVersionId()).setLocalFilePath(file.path().toString())
                .setContentSha256(ByteString.copyFrom(java.util.HexFormat.of().parseHex(proof.contentSha256())))
                .setContentSize(proof.contentSize()).setPreNativeReadBytes(ownership.budget().requestedBytes()).build();
    }
}
