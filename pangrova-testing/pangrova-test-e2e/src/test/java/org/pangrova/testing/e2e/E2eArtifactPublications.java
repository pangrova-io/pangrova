package org.pangrova.testing.e2e;

import org.pangrova.metadata.artifact.ArtifactPublicationRouter;
import org.pangrova.metadata.artifact.ArtifactPublicationStore;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.artifact.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.metadata.artifact.jdbc.JdbcIndexArtifactPublicationStore;
import org.pangrova.metadata.artifact.jdbc.JdbcTabularExportPublicationStore;

import javax.sql.DataSource;
import java.util.Map;

/** Real production JDBC publication backends shared by the E2E lifecycle and Native dispatcher. */
final class E2eArtifactPublications {
    private E2eArtifactPublications() {}

    static Map<String, ArtifactPublicationStore> stores(DataSource dataSource) {
        var index = new JdbcIndexArtifactPublicationStore(dataSource);
        return Map.of(ArtifactTypeIds.VECTOR_INDEX, index, ArtifactTypeIds.TEXT_INDEX, index,
                ArtifactTypeIds.TABULAR_EXPORT, new JdbcTabularExportPublicationStore(dataSource),
                ArtifactTypeIds.GENERATED_COLUMN_VALUES, new JdbcGeneratedValueArtifactPublicationStore(dataSource));
    }

    static ArtifactPublicationStore router(DataSource dataSource) {
        return new ArtifactPublicationRouter(stores(dataSource));
    }
}
