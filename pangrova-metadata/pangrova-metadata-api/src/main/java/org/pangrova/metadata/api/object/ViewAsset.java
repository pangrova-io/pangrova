package org.pangrova.metadata.api.object;

import org.pangrova.metadata.api.enums.AssetType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * {@link AssetType#VIEW} asset: a logical SQL view.
 *
 * <p>Unlike a {@link MaterializedViewAsset}, a logical view is rewritten on
 * every reference and stores no data. Pangrova tracks the SQL definition, the
 * projected column list (so callers can resolve types without re-parsing),
 * and the set of base tables the view depends on so that lineage and
 * cache-invalidation pipelines can fan out correctly.</p>
 *
 * @param catalog          containing catalog
 * @param schema           containing schema
 * @param name             view name
 * @param definitionSql    canonical SQL the view expands to
 * @param columns          projected columns; may be empty if not yet bound
 * @param dependentTables  base tables referenced by {@code definitionSql}; used
 *                         for invalidation and lineage. May be empty when not
 *                         yet analyzed.
 * @param securityMode     {@link ViewSecurityMode#DEFINER} or {@code INVOKER};
 *                         {@code DEFINER} runs with the view owner's privileges,
 *                         {@code INVOKER} with the caller's.
 * @param owner            principal that owns the view (may be {@code ""})
 * @param properties       free-form properties (description, tags, etc.)
 * @param createdAt        creation timestamp
 * @param updatedAt        last-update timestamp
 */
public record ViewAsset(
        String catalog,
        String schema,
        String name,
        String definitionSql,
        List<ColumnMetadata> columns,
        List<QualifiedTableRef> dependentTables,
        ViewSecurityMode securityMode,
        String owner,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt) implements Asset {

    public ViewAsset {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(name, "name must not be null");
        checkNotNull(definitionSql, "definitionSql must not be null");
        checkNotNull(createdAt, "createdAt must not be null");
        checkNotNull(updatedAt, "updatedAt must not be null");
        columns = columns == null ? List.of() : List.copyOf(columns);
        dependentTables = dependentTables == null ? List.of() : List.copyOf(dependentTables);
        securityMode = securityMode == null ? ViewSecurityMode.DEFINER : securityMode;
        owner = owner == null ? "" : owner;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override
    public AssetType type() {
        return AssetType.VIEW;
    }

    /** SQL-92 view privilege evaluation mode. */
    public enum ViewSecurityMode {
        /** Run with the privileges of the principal that created the view. */
        DEFINER,
        /** Run with the privileges of the principal that invokes the view. */
        INVOKER
    }
}
