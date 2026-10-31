package org.pangrova.testing.integration.e2e;


import org.pangrova.testing.integration.control.ai.ModelAuthorityFixtures;

import org.pangrova.databuild.assembly.DataBuildPublicationFactory;
import org.pangrova.databuild.api.publication.DataBuildPublicationStore;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;

import javax.sql.DataSource;
import java.util.Map;

/** Real production JDBC publication backends shared by the E2E lifecycle and Native dispatcher. */
final class E2eDataBuildPublications {
    private E2eDataBuildPublications() {}

    static Map<String, DataBuildPublicationStore> stores(DataSource dataSource) {
        var index = ModelAuthorityFixtures.artifacts().indexPublications(dataSource);
        return Map.of(DataBuildOutputTypeIds.VECTOR_INDEX, index, DataBuildOutputTypeIds.TEXT_INDEX, index,
                DataBuildOutputTypeIds.TABULAR_EXPORT, ModelAuthorityFixtures.artifacts().tabularPublications(dataSource),
                DataBuildOutputTypeIds.GENERATED_COLUMN_VALUES, ModelAuthorityFixtures.artifacts().generatedPublications(dataSource));
    }

    static DataBuildPublicationStore router(DataSource dataSource) {
        return DataBuildPublicationFactory.route(stores(dataSource));
    }
}
