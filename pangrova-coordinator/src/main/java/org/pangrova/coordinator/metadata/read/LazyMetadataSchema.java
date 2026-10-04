package org.pangrova.coordinator.metadata.read;

import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.exception.MetadataResolutionException;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Lazily resolves tables from CatalogRegistry when Calcite requests them.
 * Delegates to ConnectorMetadata.listTables() and getTableMetadata() for column resolution.
 *
 * <p>Backend failures propagate as {@link MetadataResolutionException} to distinguish
 * "metadata service unavailable" from "table not found".</p>
 */
final class LazyMetadataSchema extends AbstractSchema {

    private static final Logger LOG = LoggerFactory.getLogger(LazyMetadataSchema.class);
    private static final long CACHE_TTL_MS = 30_000;

    /** Global DDL generation counter. Incremented after CREATE/DROP/ALTER TABLE. */
    private static final java.util.concurrent.atomic.AtomicLong DDL_GENERATION =
            new java.util.concurrent.atomic.AtomicLong();

    private final CatalogRegistry catalogRegistry;
    private final String catalog;
    private final String schema;
    private final ConnectorSession session;
    private final PlanningReadBindings readBindings;
    private volatile Map<String, Table> cachedTableMap;
    private volatile Map<String, Table> fallbackTableMap;
    private volatile long cacheTimestamp;
    private volatile long cacheGeneration;

    LazyMetadataSchema(CatalogRegistry catalogRegistry, String catalog, String schema,
                       ConnectorSession authenticatedSession) {
        this(catalogRegistry, catalog, schema, authenticatedSession,
                catalogRegistry == null ? null : new PlanningReadBindings(catalogRegistry, authenticatedSession));
    }

    LazyMetadataSchema(CatalogRegistry catalogRegistry, String catalog, String schema,
                       ConnectorSession authenticatedSession, PlanningReadBindings readBindings) {
        this.catalogRegistry = catalogRegistry;
        this.catalog = catalog;
        this.schema = schema;
        this.session = PlanningConnectorSession.bind(authenticatedSession, catalog, schema);
        this.readBindings = readBindings;
    }

    /**
     * Calcite's case-insensitive lookup consults table names before calling
     * getTable. Virtual system tables are deliberately not enumerated in the
     * physical table list, so resolve only supported suffixes on demand.
     */
    @Override
    public Lookup<Table> tables() {
        Lookup<Table> physical = super.tables();
        return new Lookup<>() {
            @Override
            public Table get(String name) {
                Optional<String> suffix = systemTableSuffix(name);
                if (suffix.isPresent()) {
                    return resolveTableDirect(name);
                }
                return name.contains("$") ? null : physical.get(name);
            }

            @Override
            public Named<Table> getIgnoreCase(String name) {
                Optional<String> suffix = systemTableSuffix(name);
                if (suffix.isEmpty()) {
                    return name.contains("$") ? null : physical.getIgnoreCase(name);
                }
                String baseName = name.substring(0, name.lastIndexOf('$'));
                Named<Table> base = physical.getIgnoreCase(baseName);
                String canonicalBase = base == null ? findUncachedBaseName(baseName) : base.name();
                if (canonicalBase == null) {
                    return null;
                }
                String canonicalName = canonicalBase + "$" + suffix.get();
                Table table = resolveTableDirect(canonicalName);
                return table == null ? null : new Named<>(canonicalName, table);
            }

            @Override
            public Set<String> getNames(LikePattern pattern) {
                return physical.getNames(pattern);
            }
        };
    }

    private static Optional<String> systemTableSuffix(String name) {
        if (name == null) {
            return Optional.empty();
        }
        int separator = name.lastIndexOf('$');
        if (separator <= 0 || separator == name.length() - 1) {
            return Optional.empty();
        }
        String suffix = name.substring(separator + 1).toLowerCase(Locale.ROOT);
        return "snapshots".equals(suffix) || "history".equals(suffix)
                ? Optional.of(suffix) : Optional.empty();
    }

    /**
     * Another coordinator may create a table while this schema's physical name
     * cache is still fresh. Resolve only that cache miss against the connector,
     * preserving the canonical base name required by case-sensitive catalogs.
     */
    private String findUncachedBaseName(String requestedName) {
        if (catalogRegistry == null) {
            return null;
        }
        try {
            CatalogHandle handle = catalogRegistry.getCatalog(catalog);
            ConnectorMetadata metadata = handle.getMetadata(session);
            String match = null;
            for (SchemaTableName table : metadata.listTables(session, Optional.of(schema))) {
                if (!table.schema().equals(schema) || !table.table().equalsIgnoreCase(requestedName)) {
                    continue;
                }
                if (table.table().equals(requestedName)) {
                    return table.table();
                }
                if (match != null && !match.equals(table.table())) {
                    throw new IllegalStateException("ambiguous case-insensitive table name: " + requestedName);
                }
                match = table.table();
            }
            return match;
        } catch (MetadataResolutionException e) {
            throw e;
        } catch (Exception e) {
            throw new MetadataResolutionException(catalog, schema, requestedName, e);
        }
    }

    /**
     * Returns a table map that falls back to a direct connector lookup when
     * a table is not present in the cached map. This handles tables created
     * externally (e.g. via the Iceberg Java API or another coordinator) that
     * were not present when the table map was last refreshed.
     *
     * <p>Since {@code AbstractSchema.getTable(String)} is final and delegates
     * to {@code getTableMap().get(name)}, the fallback is embedded in the
     * map's {@code get()} method.</p>
     */
    @Override
    protected Map<String, Table> getTableMap() {
        if (catalogRegistry == null) {
            if (cachedTableMap == null) {
                cachedTableMap = new LinkedHashMap<>();
                fallbackTableMap = cachedTableMap;
                cacheTimestamp = System.currentTimeMillis();
                cacheGeneration = DDL_GENERATION.get();
            }
            return cachedTableMap;
        }
        long currentGen = DDL_GENERATION.get();
        boolean genMismatch = cacheGeneration != currentGen;
        boolean expired = isCacheExpired();
        if (cachedTableMap != null && !expired && !genMismatch) {
            return fallbackTableMap;
        }
        LOG.info("event=table_map_refresh catalog={} schema={} reason={} gen={}/{}",
                catalog, schema,
                genMismatch ? "ddl_generation" : "ttl_expired",
                cacheGeneration, currentGen);
        return refreshTableMap();
    }

    /**
     * Attempts a direct table lookup from the connector metadata, bypassing
     * the cached table map. Returns null if the table does not exist.
     */
    private Table resolveTableDirect(String name) {
        if (catalogRegistry == null) {
            return null;
        }
        try {
            CatalogHandle handle = catalogRegistry.getCatalog(catalog);
            ConnectorMetadata metadata = handle.getMetadata(session);
            SchemaTableName stn = new SchemaTableName(schema, name);
            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, stn);
            if (tableHandle == null) {
                return null;
            }
            ConnectorTableMetadata tableMeta = metadata.getTableMetadata(session, tableHandle);
            Table resolved = resolvedTable(tableMeta);
            LOG.debug("event=direct_table_resolved catalog={} schema={} table={}",
                    catalog, schema, name);
            return resolved;
        } catch (MetadataResolutionException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("event=direct_table_lookup_failed catalog={} schema={} table={} error={}",
                    catalog, schema, name, e.getMessage(), e);
            throw new MetadataResolutionException(catalog, schema, name, e);
        }
    }

    /** Invalidates the cached table map. Called after DDL operations. */
    void invalidateCache() {
        cachedTableMap = null;
        fallbackTableMap = null;
        cacheTimestamp = 0;
    }

    /**
     * Bumps the global DDL generation, causing all LazyMetadataSchema instances
     * to refresh their table cache on next access. Thread-safe.
     */
    static void bumpDdlGeneration() {
        DDL_GENERATION.incrementAndGet();
    }

    private boolean isCacheExpired() {
        return (System.currentTimeMillis() - cacheTimestamp) > CACHE_TTL_MS;
    }

    private Map<String, Table> refreshTableMap() {
        Map<String, Table> resolved = resolveTablesFromConnector();
        cachedTableMap = resolved;
        fallbackTableMap = buildFallbackMap(resolved);
        cacheTimestamp = System.currentTimeMillis();
        cacheGeneration = DDL_GENERATION.get();
        return fallbackTableMap;
    }

    /**
     * Wraps the resolved table map with a {@code get()} override that falls
     * back to {@link #resolveTableDirect(String)} for cache-miss lookups.
     */
    private Map<String, Table> buildFallbackMap(Map<String, Table> base) {
        return new java.util.AbstractMap<>() {
            @Override
            public java.util.Set<Entry<String, Table>> entrySet() {
                return base.entrySet();
            }

            @Override
            public Table get(Object key) {
                Table t = base.get(key);
                if (t != null || !(key instanceof String name)) {
                    return t;
                }
                return resolveTableDirect(name);
            }

            @Override
            public boolean containsKey(Object key) {
                return base.containsKey(key) || get(key) != null;
            }
        };
    }

    private Map<String, Table> resolveTablesFromConnector() {
        try {
            CatalogHandle handle = catalogRegistry.getCatalog(catalog);
            ConnectorMetadata metadata = handle.getMetadata(session);
            List<SchemaTableName> tables = metadata.listTables(session, Optional.of(schema));
            return buildTableMap(metadata, session, tables);
        } catch (MetadataResolutionException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("event=metadata_resolution_failed catalog={} schema={} error={}",
                    catalog, schema, e.getMessage(), e);
            throw new MetadataResolutionException(catalog, schema, e);
        }
    }

    private Map<String, Table> buildTableMap(
            ConnectorMetadata metadata, ConnectorSession session,
            List<SchemaTableName> tables) {
        Map<String, Table> tableMap = new LinkedHashMap<>();
        Map<String, Throwable> tableErrors = new LinkedHashMap<>();
        for (SchemaTableName stn : tables) {
            Table calciteTable = resolveOneTable(metadata, session, stn, tableErrors);
            if (calciteTable != null) {
                tableMap.put(stn.table(), calciteTable);
            }
        }
        if (!tableErrors.isEmpty()) {
            MetadataResolutionException aggregated =
                    new MetadataResolutionException(catalog, schema, tableErrors.keySet().iterator().next(),
                            tableErrors.values().iterator().next());
            tableErrors.entrySet().stream().skip(1)
                    .forEach(entry -> aggregated.addSuppressed(entry.getValue()));
            LOG.warn("event=table_resolution_partial_failure catalog={} schema={} failedTables={} resolvedTables={}",
                    catalog, schema, tableErrors.size(), tableMap.size());
            throw aggregated;
        }
        return tableMap;
    }

    private Table resolveOneTable(
            ConnectorMetadata metadata, ConnectorSession session,
            SchemaTableName stn, Map<String, Throwable> tableErrors) {
        try {
            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, stn);
            if (tableHandle == null) {
                return null;
            }
            ConnectorTableMetadata tableMeta = metadata.getTableMetadata(session, tableHandle);
            return resolvedTable(tableMeta);
        } catch (Exception e) {
            LOG.warn("event=table_resolution_failed catalog={} schema={} table={} error={}",
                    catalog, schema, stn.table(), e.getMessage(), e);
            tableErrors.put(stn.table(), e);
            return null;
        }
    }

    private MetadataResolvedTable resolvedTable(ConnectorTableMetadata metadata) {
        return new MetadataResolvedTable(metadata, () -> readBindings.resolve(catalog, schema,
                metadata.table().table()).orElseThrow(() -> new IllegalStateException("Selected table disappeared")));
    }

}
