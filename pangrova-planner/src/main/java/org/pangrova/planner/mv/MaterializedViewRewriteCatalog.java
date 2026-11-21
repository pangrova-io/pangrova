package org.pangrova.planner.mv;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Query-local source of published materialized views eligible for transparent rewrite.
 *
 * <p>The implementation is supplied by the durable materialization catalog. It
 * must already have applied authorization and lifecycle guards: the planner
 * trusts every returned view to be published, readable by the requester, and
 * bounded by {@link PublishedView#maxStaleness()}.
 */
public interface MaterializedViewRewriteCatalog {

    /**
     * Returns published materialized views whose definition reads any referenced table.
     *
     * @param referencedTables lower-case dot-separated base table names
     * @return eligible views, possibly empty
     */
    List<PublishedView> candidates(Set<String> referencedTables);

    /**
     * Published materialized view exposed to the planner.
     *
     * @param catalog logical catalog
     * @param schema logical schema
     * @param name logical object name
     * @param definitionSql immutable definition SQL
     * @param physicalCatalog catalog of the published physical generation
     * @param physicalSchema schema of the published physical generation
     * @param physicalTable table of the published physical generation
     * @param definitionRevision definition revision that produced the generation
     * @param publishedAt publication instant
     * @param maxStaleness maximum accepted age of the published generation
     */
    record PublishedView(
            String catalog,
            String schema,
            String name,
            String definitionSql,
            String physicalCatalog,
            String physicalSchema,
            String physicalTable,
            long definitionRevision,
            Instant publishedAt,
            Duration maxStaleness) {

        public PublishedView {
            if (blank(catalog) || blank(schema) || blank(name) || blank(definitionSql)
                    || blank(physicalCatalog) || blank(physicalSchema) || blank(physicalTable)) {
                throw new IllegalArgumentException(
                        "materialized view identity, definition, and physical table are required");
            }
            if (definitionRevision < 1) {
                throw new IllegalArgumentException("definitionRevision must be positive");
            }
            if (publishedAt == null || maxStaleness == null
                    || maxStaleness.isNegative() || maxStaleness.isZero()) {
                throw new IllegalArgumentException(
                        "publication instant and a positive max staleness are required");
            }
        }

        /** Returns the lower-case logical identity used by plan table references. */
        public String qualifiedName() {
            return (catalog + "." + schema + "." + name).toLowerCase(java.util.Locale.ROOT);
        }

        private static boolean blank(String value) {
            return value == null || value.isBlank();
        }
    }
}
