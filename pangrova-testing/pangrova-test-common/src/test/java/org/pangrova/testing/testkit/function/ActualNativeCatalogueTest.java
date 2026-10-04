package org.pangrova.testing.testkit.function;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.TestProjectPaths;

import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

class ActualNativeCatalogueTest {
    @Test
    void readsAllOriginalExporterBytesWithoutSharingMutableResults() throws IOException {
        var source = TestProjectPaths.findProjectRoot().resolve(
                "pangrova-protocol/src/test/resources/function-contracts/actual-native-catalogue.bin");
        byte[] actual = ActualNativeCatalogue.bytes();
        assertThat(actual).isEqualTo(Files.readAllBytes(source));
        assertThat(actual).isNotEmpty();
        actual[0] ^= 1;
        assertThat(ActualNativeCatalogue.bytes()).isEqualTo(Files.readAllBytes(source));
    }
}
