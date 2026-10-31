package org.pangrova.metadata.generation;

import org.pangrova.connector.spi.coordination.WriteCommitEntry;

import java.util.List;

/** Owns atomic generated-build submission and acknowledgement of its authenticated COMMITTED write. */
@org.pangrova.common.api.annotation.Internal
public interface GeneratedValueBuildSubmissionStore {
    /** Persists every declared build and descriptor and acknowledges the exact redo anchor in one transaction. */
    void submitCommittedBuilds(WriteCommitEntry write, List<GeneratedValueBuildSubmission> builds);
}
