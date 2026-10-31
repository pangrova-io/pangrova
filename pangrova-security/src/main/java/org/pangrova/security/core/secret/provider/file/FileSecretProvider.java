package org.pangrova.security.core.secret.provider.file;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.security.api.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves secrets by reading the first line of a file.
 *
 * <p>All paths are validated against an allowed base directory to prevent
 * path traversal attacks. The reference path is normalized and checked
 * to ensure it resides within the allowed directory.</p>
 */
public final class FileSecretProvider implements SecretProvider {

    private static final Logger LOG = LoggerFactory.getLogger(FileSecretProvider.class);

    /** Default allowed directory: /etc/pangrova/secrets */
    static final Path DEFAULT_ALLOWED_DIR = Path.of("/etc/pangrova/secrets");

    private final Path allowedDirectory;

    public FileSecretProvider() {
        this(DEFAULT_ALLOWED_DIR);
    }

    public FileSecretProvider(Path allowedDirectory) {
        this.allowedDirectory = allowedDirectory.toAbsolutePath().normalize();
    }

    @Override
    public String prefix() {
        return "FILE";
    }

    @Override
    public String resolve(SecretReference reference) {
        Path resolved = validatePath(reference.key());
        try {
            return Files.readString(resolved).lines().findFirst().orElse("");
        } catch (IOException e) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "Cannot read secret file: " + resolved);
        }
    }

    /**
     * Validates that the given reference path is within the allowed directory.
     * Normalizes the path to prevent traversal via "../" sequences.
     *
     * @param reference the file path reference
     * @return the validated, normalized absolute path
     * @throws ConfigurationException if the path escapes the allowed directory
     */
    Path validatePath(String reference) {
        Path requested = Path.of(reference).normalize();
        Path absolute = requested.isAbsolute()
                ? requested
                : allowedDirectory.resolve(requested).normalize();

        if (!absolute.startsWith(allowedDirectory)) {
            LOG.warn("event=secret_path_rejected reference={} normalizedPath={} allowedDir={}",
                    reference, absolute, allowedDirectory);
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "Secret file path is outside allowed directory: " + reference);
        }
        return absolute;
    }
}
