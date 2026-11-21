package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** An absent data snapshot is distinct from a source that has no snapshot capability. */
@Experimental
public enum ReadSnapshotState { SNAPSHOT, EMPTY, METADATA, UNVERSIONED }
