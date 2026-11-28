package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.persistence.indexing.api.metadata.IndexDescriptors.MaintenancePage;
import org.pangrova.persistence.jdbc.operation.JdbcOperations;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;

/** Durable keyset progress; descriptor mutations remain owned by lifecycle fences. */
final class JdbcIndexMaintenanceScanStore extends JdbcIndexMetadataStoreBase {
    private static final String PAGE_PREFIX = "SELECT * FROM index_descriptors WHERE state=? AND index_id<=? ";
    private static final String PAGE_SUFFIX = "ORDER BY index_id LIMIT ?";
    private static final String FIRST_PAGE = PAGE_PREFIX + PAGE_SUFFIX;
    private static final String NEXT_PAGE = PAGE_PREFIX + "AND index_id>? " + PAGE_SUFFIX;
    private static final String FIRST_PAGE_BEFORE_CUTOFF = PAGE_PREFIX + "AND updated_at<? " + PAGE_SUFFIX;
    private static final String NEXT_PAGE_BEFORE_CUTOFF =
            PAGE_PREFIX + "AND index_id>? AND updated_at<? " + PAGE_SUFFIX;

    JdbcIndexMaintenanceScanStore(DataSource dataSource) {
        super(dataSource);
    }

    MaintenancePage loadMaintenancePage(IndexState state, Instant cutoff, int limit) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        Cursor cursor = executeQuery("SELECT revision, last_index_id, upper_index_id "
                        + "FROM index_descriptor_maintenance_cursors WHERE state=?",
                ps -> ps.setString(1, state.name()), rows -> {
                    if (!rows.next()) throw new IllegalArgumentException("Unsupported maintenance state: " + state);
                    return new Cursor(rows.getLong(1), rows.getString(2), rows.getString(3));
                });
        String upper = cursor.upperIndexId();
        if (upper == null) {
            upper = executeQuery("SELECT index_id FROM index_descriptors WHERE state=? "
                            + "ORDER BY index_id DESC LIMIT 1", ps -> ps.setString(1, state.name()),
                    rows -> rows.next() ? rows.getString(1) : null);
        }
        if (upper == null) return new MaintenancePage(state, cursor.revision(), null, List.of(), true);
        List<IndexDescriptor> candidates = loadCandidates(state, cursor.lastIndexId(), upper, cutoff, limit);
        return new MaintenancePage(state, cursor.revision(), upper, candidates, candidates.size() < limit);
    }

    private List<IndexDescriptor> loadCandidates(
            IndexState state, String lastIndexId, String upperIndexId, Instant cutoff, int limit) {
        // Four complete constant statements keep every input bound and preserve the keyset range predicates.
        SqlConsumer binder = ps -> {
            ps.setString(1, state.name());
            ps.setString(2, upperIndexId);
            int parameter = 3;
            if (lastIndexId != null) ps.setString(parameter++, lastIndexId);
            if (cutoff != null) JdbcOperations.setInstant(ps, parameter++, exclusiveCutoff(cutoff));
            ps.setInt(parameter, limit);
        };
        if (lastIndexId == null) {
            return cutoff == null
                    ? executeQuery(FIRST_PAGE, binder, IndexMetadataMapper::mapDescriptorList)
                    : executeQuery(FIRST_PAGE_BEFORE_CUTOFF, binder, IndexMetadataMapper::mapDescriptorList);
        }
        return cutoff == null
                ? executeQuery(NEXT_PAGE, binder, IndexMetadataMapper::mapDescriptorList)
                : executeQuery(NEXT_PAGE_BEFORE_CUTOFF, binder, IndexMetadataMapper::mapDescriptorList);
    }

    boolean completeMaintenancePage(MaintenancePage page) {
        String last = page.exhausted() ? null : page.candidates().get(page.candidates().size() - 1).indexId();
        String upper = page.exhausted() ? null : page.upperIndexId();
        return executeUpdateCount("UPDATE index_descriptor_maintenance_cursors "
                        + "SET revision=revision+1, last_index_id=?, upper_index_id=? WHERE state=? AND revision=?",
                ps -> {
                    ps.setString(1, last);
                    ps.setString(2, upper);
                    ps.setString(3, page.state().name());
                    ps.setLong(4, page.revision());
                }) == 1;
    }

    private static Instant exclusiveCutoff(Instant cutoff) {
        // Stored timestamps lie on the microsecond grid. A sub-microsecond cutoff
        // includes the preceding grid point, so ceil before binding a strict <.
        Instant canonical = JdbcOperations.canonicalInstant(cutoff);
        return canonical.equals(cutoff) ? canonical : canonical.plusNanos(1_000);
    }

    private record Cursor(long revision, String lastIndexId, String upperIndexId) {}
}
