package org.pangrova.persistence.generation.api.generatedvalue;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.persistence.query.api.commit.WriteCommitEntry;

import java.util.List;

/** Owns atomic generated-build submission and acknowledgement of its authenticated COMMITTED write. */
@org.pangrova.common.api.annotation.Internal
@Experimental
public interface GeneratedValueBuildSubmissionStore {
    /** Persists every declared build and descriptor and acknowledges the exact redo anchor in one transaction. */
    void submitCommittedBuilds(WriteCommitEntry write, List<GeneratedValueBuildSubmission> builds);
}
