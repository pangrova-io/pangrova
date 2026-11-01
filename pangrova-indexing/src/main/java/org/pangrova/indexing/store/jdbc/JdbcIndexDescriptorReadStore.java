package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.control.IndexMetadataMapper;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;

import org.pangrova.security.authz.DeferredAuthorizationGuard;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.indexing.control.IndexMetadataMapper.mapDescriptor;

/** Read-only descriptor queries shared by index scheduling and maintenance. */
final class JdbcIndexDescriptorReadStore extends JdbcStoreSupport {

    JdbcIndexDescriptorReadStore(DataSource dataSource, DeferredAuthorizationGuard authorizationGuard) {
        super(dataSource, authorizationGuard);
    }

    List<IndexDescriptor> findIndexesByState(IndexState state) {
        String sql = "SELECT * FROM index_descriptors WHERE state=?";
        return executeQuery(sql, ps -> ps.setString(1, state.name()),
                IndexMetadataMapper::mapDescriptorList);
    }

    Map<String, IndexDescriptor> findIndexesByIds(List<String> indexIds) {
        if (indexIds.isEmpty()) {
            return Map.of();
        }
        String sql = "SELECT * FROM index_descriptors WHERE index_id IN ("
                + placeholders(indexIds.size()) + ")";
        return executeQuery(sql, ps -> {
            int parameter = 1;
            for (String id : indexIds) {
                ps.setString(parameter++, id);
            }
        }, rows -> {
            Map<String, IndexDescriptor> result = new HashMap<>();
            while (rows.next()) {
                IndexDescriptor descriptor = mapDescriptor(rows);
                result.put(descriptor.indexId(), descriptor);
            }
            return result;
        });
    }

    List<IndexDescriptor> findIndexesByStatePaginated(IndexState state, int offset, int limit) {
        String sql = "SELECT * FROM index_descriptors WHERE state=? "
                + "ORDER BY updated_at OFFSET ? LIMIT ?";
        return executeQuery(sql, ps -> {
            ps.setString(1, state.name());
            ps.setInt(2, offset);
            ps.setInt(3, limit);
        }, IndexMetadataMapper::mapDescriptorList);
    }
}
