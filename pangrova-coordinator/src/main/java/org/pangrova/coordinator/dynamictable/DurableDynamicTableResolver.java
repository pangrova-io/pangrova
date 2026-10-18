package org.pangrova.coordinator.dynamictable;

import org.pangrova.metadata.dynamictable.JdbcDynamicTablePublicationStore;
import org.pangrova.planner.mv.DynamicTableNotReadyException;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Durable logical Dynamic Table resolution; each SQL rewrite owns its publication pins. */
public final class DurableDynamicTableResolver implements DynamicTableRewriter.PublishedTargetResolver {

    private static final Pattern NAME_PART = Pattern.compile("[A-Za-z_][A-Za-z_0-9]*");

    private final JdbcDynamicTablePublicationStore publications;
    private final String tenantId;
    private final String defaultCatalog;
    private final String defaultSchema;

    /** Creates a resolver that reads the current durable head on each compilation. */
    public DurableDynamicTableResolver(JdbcDynamicTablePublicationStore publications, String tenantId,
            String defaultCatalog, String defaultSchema) {
        this.publications = Objects.requireNonNull(publications, "publications");
        this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
        this.defaultCatalog = defaultCatalog;
        this.defaultSchema = defaultSchema;
    }

    @Override
    public Optional<DynamicTableRewriter.PublishedTarget> resolve(List<String> nameParts) {
        QualifiedTableName logical = qualify(nameParts);
        if (logical == null) {
            return Optional.empty();
        }
        return publications.load(tenantId, logical.toString())
                .map(publication -> {
                    if (publication.publishedTable() == null) {
                        throw new DynamicTableNotReadyException(logical.toString());
                    }
                    return new DynamicTableRewriter.PublishedTarget(logical,
                            parsePublishedTable(publication.publishedTable()));
                });
    }

    @Override
    public Optional<QualifiedTableName> canonicalLogical(List<String> nameParts) {
        return Optional.ofNullable(qualify(nameParts));
    }

    private QualifiedTableName qualify(List<String> source) {
        if (source.isEmpty() || source.size() > 3 || source.stream().anyMatch(part -> !valid(part))) {
            return null;
        }
        List<String> parts = new ArrayList<>(source);
        if (parts.size() == 2 && defaultCatalog != null) {
            parts.add(0, defaultCatalog);
        }
        if (parts.size() == 1 && defaultCatalog != null && defaultSchema != null) {
            parts.add(0, defaultSchema);
            parts.add(0, defaultCatalog);
        }
        if (parts.size() != 3 || parts.stream().anyMatch(part -> !valid(part))) {
            return null;
        }
        return new QualifiedTableName(parts.get(0).toLowerCase(Locale.ROOT),
                parts.get(1).toLowerCase(Locale.ROOT), parts.get(2).toLowerCase(Locale.ROOT));
    }

    private static QualifiedTableName parsePublishedTable(String name) {
        String[] parts = name.split("\\.", -1);
        if (parts.length != 3 || java.util.Arrays.stream(parts).anyMatch(part -> !valid(part))) {
            throw new IllegalStateException("Invalid persisted Dynamic Table generation identity");
        }
        return new QualifiedTableName(parts[0], parts[1], parts[2]);
    }

    private static boolean valid(String part) {
        return NAME_PART.matcher(part).matches();
    }
}
