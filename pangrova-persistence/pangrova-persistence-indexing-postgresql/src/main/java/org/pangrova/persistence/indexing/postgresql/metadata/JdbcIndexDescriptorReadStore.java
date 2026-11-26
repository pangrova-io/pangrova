package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;

import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.mapDescriptor;

/** Read-only descriptor queries shared by index scheduling and maintenance. */
final class JdbcIndexDescriptorReadStore extends JdbcStoreSupport {

    JdbcIndexDescriptorReadStore(DataSource dataSource) {
        super(dataSource);
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
        try (var connection = dataSource().getConnection();
                var ids = JdbcArraySelection.create(connection, indexIds);
                var statement = connection.prepareStatement(
                        "SELECT * FROM index_descriptors WHERE index_id = ANY (?)")) {
            statement.setArray(1, ids.array());
            try (var rows = statement.executeQuery()) {
                Map<String, IndexDescriptor> result = new HashMap<>();
                while (rows.next()) {
                    IndexDescriptor descriptor = mapDescriptor(rows);
                    result.put(descriptor.indexId(), descriptor);
                }
                return result;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Index descriptor selection failed", failure);
        }
    }
}
