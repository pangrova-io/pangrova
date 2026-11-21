package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexMetadataStoreCommitLogDefaultsTest {

    @Test
    void missingCommitLogImplementationFailsClosed() {
        IndexMetadataStore store = (IndexMetadataStore) Proxy.newProxyInstance(
                IndexMetadataStore.class.getClassLoader(),
                new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, args) -> InvocationHandler.invokeDefault(proxy, method, args));

        assertThatThrownBy(() -> store.pollUnprocessedCommitEvents(10, 1L))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("commit-event polling");
        assertThatThrownBy(() -> store.markCommitEventProcessed(1L, 1L))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("commit-event acknowledgement");
    }
}
