package org.pangrova.client.http;

import org.pangrova.common.client.metadata.MetadataColumnInfo;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.client.exception.PangrovaProtocolException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogMetadataParserTest {
    @Test
    void nullTableResponseIsAProtocolFailureNotANullPointer() {
        assertThatThrownBy(() -> CatalogMetadataParser.parseColumns(null))
                .isInstanceOf(PangrovaProtocolException.class)
                .satisfies(error -> assertThat(((PangrovaProtocolException) error).errorCode())
                        .isEqualTo("CATALOG_METADATA_INVALID"));
    }

    @Test
    void readsExactSelectedSchemaAndRequiredFlag() throws Exception {
        var response = json("""
                {"metadata":{"currentSchemaId":7,"schemas":[
                  {"schemaId":6,"fields":[{"id":1,"name":"old","type":"BIGINT","required":false}]},
                  {"schemaId":7,"fields":[{"id":2,"name":"embedding","type":"VECTOR(3)","required":true}]}
                ]}}
                """);
        assertThat(CatalogMetadataParser.parseColumns(response))
                .containsExactly(new MetadataColumnInfo(2, "embedding", "VECTOR(3)", true));
    }

    @Test
    void malformedTableMetadataCannotBecomeEmptyOrPartialColumns() throws Exception {
        for (String body : List.of("{}", "[]", "{\"metadata\":null}",
                "{\"metadata\":{\"currentSchemaId\":\"7\",\"schemas\":[]}}",
                "{\"metadata\":{\"currentSchemaId\":7,\"schemas\":[]}}",
                """
                {"metadata":{"currentSchemaId":7,"schemas":[{"schemaId":7,"fields":[
                  {"id":2,"name":"good","type":"BIGINT","required":true},
                  {"id":3,"name":"bad","type":"BIGINT"}
                ]}]}}
                """,
                """
                {"metadata":{"currentSchemaId":7,"schemas":[
                  {"schemaId":7,"fields":[]},{"schemaId":7,"fields":[]}
                ]}}
                """)) {
            var response = json(body);
            assertThatThrownBy(() -> CatalogMetadataParser.parseColumns(response))
                    .as(body).isInstanceOf(PangrovaProtocolException.class);
        }
    }

    @Test
    void malformedIdentifiersAndNamespacesAreProtocolFailures() throws Exception {
        for (String body : List.of("{}", "{\"identifiers\":[{}]}",
                "{\"identifiers\":[{\"namespace\":[],\"name\":\"x\"}]}",
                "{\"identifiers\":[{\"namespace\":[\"catalog\"],\"name\":null}]}")) {
            var response = json(body);
            assertThatThrownBy(() -> CatalogMetadataParser.parseTables(response))
                    .as(body).isInstanceOf(PangrovaProtocolException.class);
        }
        assertThatThrownBy(() -> CatalogMetadataParser.parseCatalogs(null))
                .isInstanceOf(PangrovaProtocolException.class);
        assertThatThrownBy(() -> CatalogMetadataParser.parseSchemas(json("{\"namespaces\":[[\"other\",\"s\"]]}"),
                "catalog")).isInstanceOf(PangrovaProtocolException.class);
    }

    private static JsonNode json(String body) throws Exception {
        return JsonMapperFactory.create().readTree(body);
    }
}
