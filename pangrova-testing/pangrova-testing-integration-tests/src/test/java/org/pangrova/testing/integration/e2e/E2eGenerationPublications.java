package org.pangrova.testing.integration.e2e;

import org.pangrova.testing.integration.control.ai.ModelAuthorityFixtures;

import org.pangrova.generation.publication.GenerationPublicationRouter;
import org.pangrova.persistence.generation.api.publication.GenerationPublicationStore;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;

import javax.sql.DataSource;
import java.util.Map;

/** Real production JDBC publication backends shared by the E2E lifecycle and Native dispatcher. */
final class E2eGenerationPublications {
    private E2eGenerationPublications() {}

    private static Map<String, GenerationPublicationStore> stores(DataSource dataSource) {
        var index = ModelAuthorityFixtures.artifacts().indexPublications(dataSource);
        return Map.of(GenerationOutputTypeIds.VECTOR_INDEX, index, GenerationOutputTypeIds.TEXT_INDEX, index,
                GenerationOutputTypeIds.TABULAR_EXPORT, ModelAuthorityFixtures.artifacts().tabularPublications(dataSource),
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES, ModelAuthorityFixtures.artifacts().generatedPublications(dataSource));
    }

    static GenerationPublicationStore router(DataSource dataSource) {
        return GenerationPublicationRouter.route(stores(dataSource));
    }
}
