package org.pangrova.worker.file;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.enums.StorageType;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.filesystem.api.object.ObjectLocation;

import static org.assertj.core.api.Assertions.assertThat;

/** Configuration isolation across independent filesystem profiles through actual writes. */
class ObjectStoreAccessorFactoryIdentityTest {
    @TempDir Path directory;

    @Test
    void delimiterCharactersCannotMakeTwoProfilesWriteIntoTheSameRoot() throws Exception {
        Path firstRoot = directory.resolve("second");
        String name = "profile|LOCAL|" + directory.resolve("first");
        Path secondRoot = Path.of(directory.resolve("first") + "|LOCAL|" + firstRoot);
        var first = local(name, firstRoot);
        var second = local("profile", secondRoot);
        try (var factory = ObjectStoreAccessorFactory.discover()) {
            factory.accessor(first).put(new ObjectLocation(first, "stage"), "item",
                    new ByteArrayInputStream(new byte[]{1}), 1, "application/octet-stream");
            factory.accessor(second).put(new ObjectLocation(second, "stage"), "item",
                    new ByteArrayInputStream(new byte[]{2}), 1, "application/octet-stream");
        }
        assertThat(Files.readAllBytes(firstRoot.resolve("stage/item"))).containsExactly(1);
        assertThat(Files.readAllBytes(secondRoot.resolve("stage/item"))).containsExactly(2);
    }

    private static StorageProfile local(String name, Path root) {
        return new StorageProfile(name, StorageType.LOCAL, root.toString(), "", "", "",
                Map.of(), Map.of());
    }

}
