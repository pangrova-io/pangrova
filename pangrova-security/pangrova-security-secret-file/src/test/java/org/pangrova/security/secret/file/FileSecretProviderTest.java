package org.pangrova.security.secret.file;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.security.api.secret.SecretReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests FileSecretProvider path validation and secret resolution.
 * Validates: R7.13 — path traversal prevention.
 */
class FileSecretProviderTest {

    @TempDir
    Path secretsDir;

    private FileSecretProvider provider;

    @BeforeEach
    void setUp() {
        provider = new FileSecretProvider(secretsDir);
    }

    private String resolve(String reference) {
        return provider.resolve(SecretReference.of("FILE", reference));
    }

    @Test
    void prefixIsFile() {
        assertThat(provider.prefix()).isEqualTo("FILE");
    }

    @Test
    void resolveReadsFirstLineFromAllowedPath() throws IOException {
        Path secretFile = secretsDir.resolve("db-password.txt");
        Files.writeString(secretFile, "s3cret\nextra-line");

        String result = resolve(secretFile.toString());
        assertThat(result).isEqualTo("s3cret");
    }

    @Test
    void resolveRelativePathWithinAllowedDir() throws IOException {
        Path secretFile = secretsDir.resolve("api-key.txt");
        Files.writeString(secretFile, "key-123");

        String result = resolve("api-key.txt");
        assertThat(result).isEqualTo("key-123");
    }

    @Test
    void rejectPathTraversalWithDotDot() {
        assertThatThrownBy(() -> resolve("../../../etc/passwd"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("outside allowed directory");
    }

    @Test
    void rejectAbsolutePathOutsideAllowedDir() {
        assertThatThrownBy(() -> resolve("/tmp/evil-secret.txt"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("outside allowed directory");
    }

    @Test
    void rejectTraversalWithIntermediateDotDot() {
        assertThatThrownBy(() -> resolve("subdir/../../outside.txt"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("outside allowed directory");
    }

    @Test
    void allowNestedPathWithinAllowedDir() throws IOException {
        Path nested = secretsDir.resolve("sub");
        Files.createDirectories(nested);
        Path secretFile = nested.resolve("nested-secret.txt");
        Files.writeString(secretFile, "nested-value");

        String result = resolve(secretFile.toString());
        assertThat(result).isEqualTo("nested-value");
    }

    @Test
    void validatePathNormalizesCorrectly() throws IOException {
        // Create the target file so the path is valid
        Path subDir = secretsDir.resolve("a");
        Files.createDirectories(subDir);
        Path target = secretsDir.resolve("b.txt");
        Files.writeString(target, "test");

        // A path that normalizes to within the allowed dir should pass
        Path valid = provider.validatePath(secretsDir.resolve("a/../b.txt").toString());
        assertThat(valid.normalize()).isEqualTo(target.toAbsolutePath().normalize());
    }

    @Test
    void resolveEmptyFileReturnsEmptyString() throws IOException {
        Path emptyFile = secretsDir.resolve("empty.txt");
        Files.writeString(emptyFile, "");

        String result = resolve(emptyFile.toString());
        assertThat(result).isEmpty();
    }

    @Test
    void resolveNonexistentFileThrowsConfigurationException() {
        assertThatThrownBy(() -> resolve(secretsDir.resolve("missing.txt").toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Cannot read secret file");
    }
}
