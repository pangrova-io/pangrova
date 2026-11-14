package org.pangrova.streaming.api.source;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.streaming.api.source.CdcSourceReader;

import java.util.Map;

/** A stateless Coordinator provider for partitioned, checkpointable change records.
 * Discovery and validation acquire no external resources. Each open transfers one distinct
 * reader to a pipeline, which owns its poll, seek, explicit offset commit and close lifecycle.
 * A partially failed open releases all acquired resources before throwing. */
@Experimental
public interface CdcSourceFactory {
    int API_VERSION = 1;

    /** Lowercase SQL identifier, unique among source factories on the Coordinator. */
    String identifier();

    int apiVersion();

    /** Rejects invalid settings before the stream definition is persisted. */
    void validate(Map<String, String> properties);

    /** The checkpoint identity is stable across recovery and unique to a stream creation. */
    CdcSourceReader open(String checkpointId, Map<String, String> properties);
}
