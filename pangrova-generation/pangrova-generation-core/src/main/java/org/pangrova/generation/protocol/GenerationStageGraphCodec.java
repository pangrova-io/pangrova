package org.pangrova.generation.protocol;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.execution.GenerationStageGraph;

/** Owns the versioned persisted representation and integrity identity of a durable stage graph. */
@Internal
public final class GenerationStageGraphCodec {
    private static final String DESCRIPTION = "Durable stage graph";

    private GenerationStageGraphCodec() {}

    /** Encodes a graph, rejecting envelopes above the same byte limit used for replay. */
    public static byte[] encode(GenerationStageGraph graph) {
        return GenerationCanonicalJson.encode(graph, DESCRIPTION);
    }

    /** Hashes the exact canonical representation, including every stage binding. */
    public static String sha256(GenerationStageGraph graph) {
        return DigestUtils.sha256Hex(encode(graph));
    }

    /** Verifies size, digest and canonical bytes before accepting a persisted graph. */
    public static GenerationStageGraph decode(byte[] bytes, String expectedSha256) {
        return GenerationCanonicalJson.decode(bytes, expectedSha256, GenerationStageGraph.class, DESCRIPTION,
                "Durable stage graph bytes or checksum are invalid");
    }
}
