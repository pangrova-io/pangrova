package org.pangrova.testing.integration.e2e;

import org.pangrova.testing.integration.control.ai.ModelAuthorityFixtures;

import org.pangrova.generation.assembly.GenerationPublicationFactory;
import org.pangrova.generation.api.publication.GenerationPublicationStore;
import org.pangrova.generation.api.GenerationOutputTypeIds;

import javax.sql.DataSource;
import java.util.Map;

/** Real production JDBC publication backends shared by the E2E lifecycle and Native dispatcher. */
final class E2eGenerationPublications {
    private E2eGenerationPublications() {}

    static Map<String, GenerationPublicationStore> stores(DataSource dataSource) {
        var index = ModelAuthorityFixtures.artifacts().indexPublications(dataSource);
        return Map.of(GenerationOutputTypeIds.VECTOR_INDEX, index, GenerationOutputTypeIds.TEXT_INDEX, index,
                GenerationOutputTypeIds.TABULAR_EXPORT, ModelAuthorityFixtures.artifacts().tabularPublications(dataSource),
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES, ModelAuthorityFixtures.artifacts().generatedPublications(dataSource));
    }

    static GenerationPublicationStore router(DataSource dataSource) {
        return GenerationPublicationFactory.route(stores(dataSource));
    }
}
