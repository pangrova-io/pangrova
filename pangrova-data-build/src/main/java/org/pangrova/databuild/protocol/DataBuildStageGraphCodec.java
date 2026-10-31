package org.pangrova.databuild.protocol;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;

/** Owns the versioned persisted representation and integrity identity of a durable stage graph. */
@Internal
public final class DataBuildStageGraphCodec {
    private static final String DESCRIPTION = "Durable stage graph";

    private DataBuildStageGraphCodec() {}

    /** Encodes a graph, rejecting envelopes above the same byte limit used for replay. */
    public static byte[] encode(DataBuildStageGraph graph) {
        return DataBuildCanonicalJson.encode(graph, DESCRIPTION);
    }

    /** Hashes the exact canonical representation, including every stage binding. */
    public static String sha256(DataBuildStageGraph graph) {
        return DigestUtils.sha256Hex(encode(graph));
    }

    /** Verifies size, digest and canonical bytes before accepting a persisted graph. */
    public static DataBuildStageGraph decode(byte[] bytes, String expectedSha256) {
        return DataBuildCanonicalJson.decode(bytes, expectedSha256, DataBuildStageGraph.class, DESCRIPTION,
                "Durable stage graph bytes or checksum are invalid");
    }
}
