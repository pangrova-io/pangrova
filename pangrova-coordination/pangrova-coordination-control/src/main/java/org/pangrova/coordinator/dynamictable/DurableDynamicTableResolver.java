package org.pangrova.coordinator.dynamictable;

import org.pangrova.materialization.api.domain.QualifiedName;
import org.pangrova.coordinator.materialization.PlanningMaterializations;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/** Durable logical Dynamic Table resolution; each SQL rewrite owns its publication pins. */
public final class DurableDynamicTableResolver implements DynamicTableRewriter.PublishedTargetResolver {

    private static final Pattern NAME_PART = Pattern.compile("[A-Za-z_][A-Za-z_0-9]*");

    private final PlanningMaterializations publications;
    private final String defaultCatalog;
    private final String defaultSchema;
    private final java.util.function.Consumer<
            org.pangrova.materialization.api.port.PublishedMaterialization> readerPins;

    /**
     * Creates a resolver that leases every published generation it exposes to planning.
     *
     * <p>Leasing happens before the target is handed to the planner, so garbage
     * collection cannot collect a generation between resolution and execution.
     *
     * @param publications query-local tenant publication directory
     * @param defaultCatalog session default catalog
     * @param defaultSchema session default schema
     * @param readerPins per-query lease callback
     */
    public DurableDynamicTableResolver(PlanningMaterializations publications,
            String defaultCatalog, String defaultSchema,
            java.util.function.Consumer<org.pangrova.materialization.api.port.PublishedMaterialization>
                    readerPins) {
        this.publications = Objects.requireNonNull(publications, "publications");
        this.defaultCatalog = defaultCatalog;
        this.defaultSchema = defaultSchema;
        this.readerPins = Objects.requireNonNull(readerPins, "readerPins");
    }

    @Override
    public Optional<DynamicTableRewriter.PublishedTarget> resolve(List<String> nameParts) {
        QualifiedTableName logical = qualify(nameParts);
        if (logical == null) {
            return Optional.empty();
        }
        return publications.snapshot().find(QualifiedName.parse(logical.toString()))
                .map(published -> {
                    readerPins.accept(published);
                    return new DynamicTableRewriter.PublishedTarget(logical,
                            parsePublishedTable(published.physicalTable().qualifiedName()));
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
