package org.pangrova.testing.e2e;

import org.pangrova.indexing.store.jdbc.ArtifactCurrentAuthority;
import org.pangrova.metadata.core.TableControlDeferredPolicy;

import org.pangrova.indexing.control.ArtifactPublicationRouter;
import org.pangrova.indexing.control.ArtifactPublicationStore;
import org.pangrova.indexing.api.ArtifactTypeIds;
import org.pangrova.indexing.store.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.indexing.store.jdbc.JdbcIndexArtifactPublicationStore;
import org.pangrova.indexing.store.jdbc.JdbcTabularExportPublicationStore;

import javax.sql.DataSource;
import java.util.Map;

/** Real production JDBC publication backends shared by the E2E lifecycle and Native dispatcher. */
final class E2eArtifactPublications {
    private E2eArtifactPublications() {}

    static Map<String, ArtifactPublicationStore> stores(DataSource dataSource) {
        var index = new JdbcIndexArtifactPublicationStore(dataSource, new ArtifactCurrentAuthority(TableControlDeferredPolicy::requireCurrent));
        return Map.of(ArtifactTypeIds.VECTOR_INDEX, index, ArtifactTypeIds.TEXT_INDEX, index,
                ArtifactTypeIds.TABULAR_EXPORT, new JdbcTabularExportPublicationStore(dataSource, new ArtifactCurrentAuthority(TableControlDeferredPolicy::requireCurrent)),
                ArtifactTypeIds.GENERATED_COLUMN_VALUES, new JdbcGeneratedValueArtifactPublicationStore(dataSource, new ArtifactCurrentAuthority(TableControlDeferredPolicy::requireCurrent)));
    }

    static ArtifactPublicationStore router(DataSource dataSource) {
        return new ArtifactPublicationRouter(stores(dataSource));
    }
}
