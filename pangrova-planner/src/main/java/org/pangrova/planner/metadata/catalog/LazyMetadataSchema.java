package org.pangrova.planner.metadata.catalog;

import org.pangrova.metadata.api.catalog.CatalogReadAccess;
import org.pangrova.metadata.api.exception.MetadataResolutionException;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Lazily resolves tables from CatalogReadAccess when Calcite requests them.
 * Discovers connector table names and defers columns to the authenticated read binding.
 *
 * <p>Backend failures propagate as {@link MetadataResolutionException} to distinguish
 * "metadata service unavailable" from "table not found".</p>
 */
public final class LazyMetadataSchema extends AbstractSchema {

    private static final Logger LOG = LoggerFactory.getLogger(LazyMetadataSchema.class);
    private static final long CACHE_TTL_MS = 30_000;

    /** Global DDL generation counter. Incremented after CREATE/DROP/ALTER TABLE. */
    private static final java.util.concurrent.atomic.AtomicLong DDL_GENERATION =
            new java.util.concurrent.atomic.AtomicLong();

    private final String catalog;
    private final String schema;
    private final PlanningReadBindingResolver readBindings;
    private Set<String> cachedTableNames;
    private long namesTimestamp;
    private long namesGeneration;

    private final Lookup<Table> lookup = new TableLookup();
    private final Map<String, Table> tableMap = new TableMap();

    private final class TableMap extends AbstractMap<String, Table> {
        @Override
        public Table get(Object key) {
            return key instanceof String name ? lookup.get(name) : null;
        }

        @Override
        public boolean containsKey(Object key) {
            return get(key) != null;
        }

        @Override
        public Set<String> keySet() {
            return tableNames();
        }

        @Override
        public int size() {
            return tableNames().size();
        }

        @Override
        public Set<Entry<String, Table>> entrySet() {
            Set<Entry<String, Table>> entries = new LinkedHashSet<>();
            for (String name : tableNames()) {
                Table table = lookup.get(name);
                if (table != null) entries.add(Map.entry(name, table));
            }
            return Collections.unmodifiableSet(entries);
        }
    }

    public LazyMetadataSchema(CatalogReadAccess catalogRegistry, String catalog, String schema,
                       ConnectorSession authenticatedSession) {
        this(catalog, schema,
                catalogRegistry == null ? null : new PlanningReadBindingResolver(catalogRegistry, authenticatedSession));
    }

    LazyMetadataSchema(String catalog, String schema, PlanningReadBindingResolver readBindings) {
        this.catalog = catalog;
        this.schema = schema;
        this.readBindings = readBindings;
    }

    /**
     * Resolve named tables directly. Calcite's default lookup materializes every
     * table and its columns even when a query names only one table. Enumeration
     * remains available for callers that explicitly request table names.
     */
    @Override
    public Lookup<Table> tables() {
        return lookup;
    }

    private final class TableLookup implements Lookup<Table> {
        @Override
        public Table get(String name) {
            Optional<String> suffix = systemTableSuffix(name);
            if (suffix.isPresent()) {
                return resolveTableDirect(name);
            }
            return name.contains("$") ? null : resolveTableDirect(name);
        }

        @Override
        public Named<Table> getIgnoreCase(String name) {
            Optional<String> suffix = systemTableSuffix(name);
            if (suffix.isEmpty()) {
                if (name.contains("$")) {
                    return null;
                }
                Table exact = resolveTableDirect(name, true);
                if (exact != null) return new Named<>(name, exact);
                String canonicalName = resolveBaseName(name);
                Table table = canonicalName == null ? null : resolveTableDirect(canonicalName);
                return table == null ? null : new Named<>(canonicalName, table);
            }
            String baseName = name.substring(0, name.lastIndexOf('$'));
            String canonicalBase = resolveBaseName(baseName);
            if (canonicalBase == null) {
                return null;
            }
            String canonicalName = canonicalBase + "$" + suffix.get();
            Table table = resolveTableDirect(canonicalName);
            return table == null ? null : new Named<>(canonicalName, table);
        }

        @Override
        public Set<String> getNames(LikePattern pattern) {
            Set<String> matches = new LinkedHashSet<>();
            var matchesPattern = pattern.matcher();
            for (String name : tableNames()) {
                if (matchesPattern.apply(name)) {
                    matches.add(name);
                }
            }
            return matches;
        }
    }

    private synchronized String resolveBaseName(String requestedName) {
        var previous = cachedTableNames;
        var names = tableNames(false, requestedName);
        String match = canonicalName(names, requestedName);
        if (match == null && previous != null && names == previous) {
            // A cached miss can be caused by a DDL operation on another coordinator.
            return canonicalName(tableNames(true, requestedName), requestedName);
        }
        return match;
    }

    private String canonicalName(Set<String> names, String requestedName) {
        if (names.contains(requestedName)) return requestedName;
        String match = null;
        for (String name : names) {
            if (name.equalsIgnoreCase(requestedName)) {
                if (match != null && !match.equals(name)) {
                    throw new MetadataResolutionException(catalog, schema, requestedName,
                            new IllegalStateException("ambiguous case-insensitive table name: " + requestedName));
                }
                match = name;
            }
        }
        return match;
    }

    /** List names without loading the schema and column metadata of every table. */
    private synchronized Set<String> tableNames() {
        return tableNames(false, null);
    }

    private Set<String> tableNames(boolean refresh, String requestedName) {
        if (readBindings == null) {
            return Set.of();
        }
        long generation = DDL_GENERATION.get();
        if (!refresh && cachedTableNames != null && namesGeneration == generation
                && System.currentTimeMillis() - namesTimestamp <= CACHE_TTL_MS) {
            return cachedTableNames;
        }
        try {
            Set<String> names = new LinkedHashSet<>();
            for (SchemaTableName table : readBindings.listTableNames(catalog, schema)) {
                if (table.schema().equals(schema)) {
                    names.add(table.table());
                }
            }
            cachedTableNames = Collections.unmodifiableSet(names);
            namesTimestamp = System.currentTimeMillis();
            namesGeneration = generation;
            return cachedTableNames;
        } catch (MetadataResolutionException e) {
            throw e;
        } catch (Exception e) {
            if (requestedName != null) {
                throw new MetadataResolutionException(catalog, schema, requestedName, e);
            }
            throw new MetadataResolutionException(catalog, schema, e);
        }
    }

    private static Optional<String> systemTableSuffix(String name) {
        return org.pangrova.connector.api.enums.MetadataTableKind.fromTableName(name)
                .map(org.pangrova.connector.api.enums.MetadataTableKind::suffix);
    }

    /** Supports Calcite accessors that use the schema map instead of the Lookup API. */
    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    /**
     * Attempts a direct table lookup from the connector metadata, bypassing
     * the cached table map. Returns null if the table does not exist.
     */
    private Table resolveTableDirect(String name) {
        return resolveTableDirect(name, false);
    }

    private Table resolveTableDirect(String name, boolean requireExactSpelling) {
        if (readBindings == null) {
            return null;
        }
        try {
            var table = new QualifiedTableName(catalog, schema, name);
            Table resolved = requireExactSpelling
                    ? readBindings.discoverExactSpelling(table) : readBindings.discover(table);
            if (resolved != null) {
                LOG.debug("event=direct_table_resolved catalog={} schema={} table={}", catalog, schema, name);
            }
            return resolved;
        } catch (MetadataResolutionException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("event=direct_table_lookup_failed catalog={} schema={} table={} error={}",
                    catalog, schema, name, e.getMessage(), e);
            throw new MetadataResolutionException(catalog, schema, name, e);
        }
    }

    /**
     * Bumps the global DDL generation, causing all LazyMetadataSchema instances
     * to refresh their table cache on next access. Thread-safe.
     */
    static void bumpDdlGeneration() {
        DDL_GENERATION.incrementAndGet();
    }
}
