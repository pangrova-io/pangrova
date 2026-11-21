package org.pangrova.server.coordinator.http.endpoint.catalog;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogRestResponseTest {

    @Test
    void namespaceResponseDoesNotChangeWhenSourceCollectionsAreMutated() {
        List<String> namespace = new ArrayList<>(List.of("catalog"));
        Map<String, String> properties = new HashMap<>(Map.of("owner", "first"));
        var response = new CatalogRestResponse.CreateNamespaceResponse(namespace, properties);

        namespace.add("unexpected");
        properties.put("owner", "second");

        assertThat(response.namespace()).containsExactly("catalog");
        assertThat(response.properties()).containsEntry("owner", "first");
        assertThatThrownBy(() -> response.namespace().add("unexpected"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void namespaceListingOwnsNestedLists() {
        List<String> name = new ArrayList<>(List.of("catalog"));
        List<List<String>> names = new ArrayList<>();
        names.add(name);
        var response = new CatalogRestResponse.ListNamespacesResponse(names);

        name.add("unexpected");
        names.clear();

        assertThat(response.namespaces()).containsExactly(List.of("catalog"));
        assertThatThrownBy(() -> response.namespaces().get(0).add("unexpected"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
