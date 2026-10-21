package org.pangrova.connector.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import java.util.List;

/** Retains source provenance while the SDK applies its own ADD/DELETE merge rules. */
record PaimonManifestSourceEntry(ManifestEntry entry, String manifestName) implements FileEntry {
    @Override public FileKind kind() { return entry.kind(); }
    @Override public BinaryRow partition() { return entry.partition(); }
    @Override public int bucket() { return entry.bucket(); }
    @Override public int totalBuckets() { return entry.totalBuckets(); }
    @Override public int level() { return entry.level(); }
    @Override public String fileName() { return entry.fileName(); }
    @Override public String externalPath() { return entry.externalPath(); }
    @Override public Identifier identifier() { return entry.identifier(); }
    @Override public BinaryRow minKey() { return entry.minKey(); }
    @Override public BinaryRow maxKey() { return entry.maxKey(); }
    @Override public List<String> extraFiles() { return entry.extraFiles(); }
    @Override public long rowCount() { return entry.rowCount(); }
    @Override public Long firstRowId() { return entry.firstRowId(); }
}
