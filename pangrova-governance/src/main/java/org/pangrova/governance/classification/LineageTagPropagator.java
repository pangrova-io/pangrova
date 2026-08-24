package org.pangrova.governance.classification;

import org.pangrova.governance.lineage.LineageCatalog;
import org.pangrova.governance.lineage.LineageRecord;
import org.pangrova.governance.lineage.LineageRecord.ColumnDerivationEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Propagates column tags (especially PII classifications) from source columns to
 * derived columns using stored lineage records.
 *
 * <p>When a tag is applied to a source column, this propagator scans the lineage
 * catalog for all derivation entries that reference that source column and
 * automatically applies the same tag to the derived (output) columns. This
 * closes the G-SEC-04 gap where tagging a source column had no effect on
 * derived objects produced by CTAS, INSERT...SELECT, or view definitions.
 *
 * <p>The propagation is transitive within a single invocation: if column A
 * feeds column B, and B feeds C, tagging A will propagate to B and then a
 * subsequent lineage scan will pick up B's tag for C on the next event cycle.
 *
 * <p>Thread-safety: delegates to thread-safe {@link ColumnTagService} and
 * {@link LineageCatalog}.
 */
public final class LineageTagPropagator implements TagChangedEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(LineageTagPropagator.class);

    private final ColumnTagService columnTagService;
    private final LineageCatalog lineageCatalog;

    /**
     * Creates a propagator that uses the given tag service and lineage catalog.
     *
     * @param columnTagService the tag service to read existing tags and write propagated ones
     * @param lineageCatalog   the lineage catalog to query for column derivations
     */
    public LineageTagPropagator(ColumnTagService columnTagService, LineageCatalog lineageCatalog) {
        this.columnTagService = Objects.requireNonNull(columnTagService, "columnTagService");
        this.lineageCatalog = Objects.requireNonNull(lineageCatalog, "lineageCatalog");
    }

    /**
     * On any column-level tag change, scans lineage for derived columns and
     * propagates the tag to them.
     */
    @Override
    public void onTagChanged(TagChangedEvent event) {
        if (event.columnName() == null) {
            // Table-level tag change — propagation is column-only
            return;
        }
        if (event.newValue() == null) {
            // Tag removal — do not auto-remove from derived columns (conservative:
            // the derived column may have its own independent tag assignment)
            return;
        }

        String sourceTable = qualifiedTableName(event.catalog(), event.schema(), event.table());
        String sourceColumn = event.columnName();
        String tagKey = event.tagKey();
        String tagValue = event.newValue();

        propagateToDownstream(sourceTable, sourceColumn, tagKey, tagValue);
    }

    /**
     * Scans all lineage records for derivations that reference the given source column
     * and applies the tag to each derived output column.
     *
     * @param sourceTable  fully qualified source table name
     * @param sourceColumn source column name
     * @param tagKey       tag key to propagate
     * @param tagValue     tag value to propagate
     * @return the number of derived columns that received the propagated tag
     */
    public int propagateToDownstream(String sourceTable, String sourceColumn,
                                      String tagKey, String tagValue) {
        int propagated = 0;
        for (LineageRecord record : lineageCatalog.findBySourceColumn(sourceTable, sourceColumn)) {
            for (ColumnDerivationEntry derivation : record.columnDerivations()) {
                if (!referencesSource(derivation, sourceTable, sourceColumn)) {
                    continue;
                }
                propagated += propagateToTargetTables(
                        record, derivation, sourceTable, sourceColumn, tagKey, tagValue);
            }
        }
        if (propagated > 0) {
            LOG.info("event=lineage_tag_propagation_complete source={}.{} propagated_count={}",
                    sourceTable, sourceColumn, propagated);
        }
        return propagated;
    }

    private static boolean referencesSource(ColumnDerivationEntry derivation,
                                            String sourceTable, String sourceColumn) {
        return derivation.sourceColumns().stream()
                .anyMatch(sc -> sc.table().equals(sourceTable)
                        && sc.column().equals(sourceColumn));
    }

    private int propagateToTargetTables(LineageRecord record, ColumnDerivationEntry derivation,
                                        String sourceTable, String sourceColumn,
                                        String tagKey, String tagValue) {
        int propagated = 0;
        // Determine the target table for this derivation
        for (String targetTable : record.targetTables()) {
            String[] parts = splitQualifiedName(targetTable);
            if (parts == null) {
                continue;
            }

            String targetColumn = derivation.outputColumn();
            Map<String, String> existingTags = columnTagService.getTags(
                    parts[0], parts[1], parts[2], targetColumn);

            if (tagValue.equals(existingTags.get(tagKey))) {
                // Already tagged with same value — skip
                continue;
            }

            columnTagService.assignTag(
                    parts[0], parts[1], parts[2], targetColumn,
                    tagKey, existingTags.get(tagKey), tagValue);
            propagated++;
            LOG.info("event=tag_propagated source={}.{} target={}.{} tag={}={}",
                    sourceTable, sourceColumn, targetTable, targetColumn,
                    tagKey, tagValue);
        }
        return propagated;
    }

    private static String qualifiedTableName(String catalog, String schema, String table) {
        return catalog + "." + schema + "." + table;
    }

    /**
     * Splits a qualified table name ({@code catalog.schema.table}) into its parts.
     * Returns null if the name does not have exactly 3 parts.
     */
    static String[] splitQualifiedName(String qualifiedName) {
        String[] parts = qualifiedName.split("\\.", 3);
        if (parts.length != 3) {
            LOG.warn("event=invalid_qualified_name name={}", qualifiedName);
            return null;
        }
        return parts;
    }
}
