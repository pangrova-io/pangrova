package org.pangrova.connector.iceberg.lance;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for Lance format model metadata registration.
 */
class IcebergLanceFormatModelTest {

    @Test
    void formatNameReturnsLance() {
        IcebergLanceFormatModel model = new IcebergLanceFormatModel();
        assertThat(model.formatName()).isEqualTo("lance");
    }

    @Test
    void fileExtensionReturnsDotLance() {
        IcebergLanceFormatModel model = new IcebergLanceFormatModel();
        assertThat(model.fileExtension()).isEqualTo(".lance");
    }

    @Test
    void toStringContainsFormatDetails() {
        IcebergLanceFormatModel model = new IcebergLanceFormatModel();
        assertThat(model.toString())
                .contains("lance")
                .contains(".lance");
    }

    @Test
    void registryLookupFindsRegisteredModel() {
        FormatModelRegistry registry = new FormatModelRegistry();
        IcebergLanceFormatModel model = new IcebergLanceFormatModel();
        registry.register(model);

        assertThat(registry.lookup("lance")).isPresent().contains(model);
    }

    @Test
    void registryLookupReturnsEmptyForUnknownFormat() {
        FormatModelRegistry registry = new FormatModelRegistry();
        assertThat(registry.lookup("unknown")).isEmpty();
    }

    @Test
    void registryRecognizesLanceExtension() {
        FormatModelRegistry registry = new FormatModelRegistry();
        registry.register(new IcebergLanceFormatModel());

        assertThat(registry.isRegisteredExtension(".lance")).isTrue();
        assertThat(registry.isRegisteredExtension(".parquet")).isFalse();
    }

    @Test
    void registryDuplicateRegistrationIsIdempotent() {
        FormatModelRegistry registry = new FormatModelRegistry();
        IcebergLanceFormatModel first = new IcebergLanceFormatModel();
        IcebergLanceFormatModel second = new IcebergLanceFormatModel();

        registry.register(first);
        registry.register(second);

        assertThat(registry.lookup("lance")).isPresent().contains(second);
    }
}
