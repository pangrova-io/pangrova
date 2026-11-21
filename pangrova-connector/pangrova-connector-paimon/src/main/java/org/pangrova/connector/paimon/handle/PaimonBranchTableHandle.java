package org.pangrova.connector.paimon.handle;

import java.net.URI;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

/** A directory-backed Paimon $branches relation, separate from JSON metadata objects. */
public record PaimonBranchTableHandle(
        PaimonTableHandle baseTable,
        List<BranchDirectory> directories,
        String sourceIdentity) implements ConnectorTableHandle {

    /** The branch directory status pinned for a local Native stat. */
    public record BranchDirectory(String name, String uri, long modificationTimeMillis,
            long device, long inode, int zoneOffsetSeconds) {
        public BranchDirectory {
            if (name == null || name.isBlank() || name.equals(".") || name.equals("..")
                    || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0
                    || name.indexOf('\0') >= 0 || uri == null
                    || !matchesBranchPath(uri, name)
                    || modificationTimeMillis < 0 || device < 0 || inode <= 0
                    || zoneOffsetSeconds < ZoneOffset.MIN.getTotalSeconds()
                    || zoneOffsetSeconds > ZoneOffset.MAX.getTotalSeconds()) {
                throw new IllegalArgumentException("Invalid pinned Paimon branch directory");
            }
        }
    }

    private static boolean matchesBranchPath(String uri, String name) {
        try {
            URI parsed = URI.create(uri);
            if (!"file".equals(parsed.getScheme()) || parsed.getAuthority() != null) {
                return false;
            }
            Path path = Path.of(parsed);
            Path fileName = path.getFileName();
            Path parent = path.getParent();
            Path parentName = parent == null ? null : parent.getFileName();
            return path.isAbsolute() && fileName != null && parentName != null
                    && fileName.toString().equals("branch-" + name)
                    && parentName.toString().equals("branch");
        } catch (IllegalArgumentException failure) {
            return false;
        }
    }

    public PaimonBranchTableHandle {
        Objects.requireNonNull(baseTable, "baseTable");
        directories = List.copyOf(directories);
        Objects.requireNonNull(sourceIdentity, "sourceIdentity");
        if (sourceIdentity.isBlank() && !directories.isEmpty()) {
            throw new IllegalArgumentException("Unbound branch relation cannot carry directories");
        }
    }

    public static PaimonBranchTableHandle selector(PaimonTableHandle baseTable) {
        return new PaimonBranchTableHandle(baseTable, List.of(), "");
    }

    public boolean bound() { return !sourceIdentity.isBlank(); }

    public String tableName() { return baseTable.tableName() + "$branches"; }

    public List<ConnectorTableMetadata.Column> typedColumns() {
        return List.of(
                new ConnectorTableMetadata.Column("branch_name", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("create_time", PangrovaType.TIMESTAMP));
    }
}
