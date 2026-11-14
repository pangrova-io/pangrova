package org.pangrova.table.identity;

import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.pangrova.table.api.ownership.TableControlRecord;

import java.time.Instant;
import java.util.Optional;

/** Connector evidence recorded by ownership, observation and rename workflows. */
public final class TablePhysicalIdentity {
    private TablePhysicalIdentity() {}

    public static boolean matchesIdentity(TableControlRecord record,
            Optional<TableIdentity> identity) {
        if (identity.isEmpty() || !identity.get().hasFormatUuid()) {
            return true;
        }
        if (!hasFormatUuid(record)) {
            return true;
        }
        return sameIdentity(record, identity.get());
    }

    public static boolean sameIdentity(TableControlRecord record, TableIdentity identity) {
        return record.format().equals(identity.format())
                && record.formatTableUuid().equals(identity.formatTableUuid())
                && java.util.Objects.equals(record.formatLocation(), identity.location());
    }

    public static boolean hasFormatUuid(TableControlRecord record) {
        return record.formatTableUuid() != null && !record.formatTableUuid().isBlank();
    }

    public static TableControlRecord.Builder withIdentity(TableControlRecord.Builder builder,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version, Instant now) {
        identity.ifPresent(value -> builder
                .format(value.format())
                .formatTableUuid(value.formatTableUuid())
                .formatLocation(value.location())
                .sourceSignature(sourceSignature(value)));
        if (version.isPresent()) {
            builder.version(version.get().token(), now);
        }
        return builder;
    }

    public static TableControlRecord.Builder withIdentity(TableControlRecord record,
            Optional<TableIdentity> identity, Optional<TableVersionToken> version, Instant now) {
        return withIdentity(record.toBuilder().updatedAt(now), identity, version, now);
    }

    private static String sourceSignature(TableIdentity identity) {
        return identity.format() + '|' + identity.formatTableUuid() + '|' + identity.location();
    }
}
