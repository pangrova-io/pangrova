package org.pangrova.streaming.writer.enums;

/** Change attributed to a merge-tree row. */
public enum ChangelogRowKind {
    INSERT,
    UPDATE_BEFORE,
    UPDATE_AFTER,
    DELETE
}
