package org.pangrova.security.secret.file;

import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.security.api.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardOpenOption;
import java.util.Set;

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
        if (!prefix().equals(reference.prefix())) {
            throw new IllegalArgumentException("Invalid file secret prefix");
        }
        Path resolved = validatePath(reference.key());
        try {
            Path root = allowedDirectory.toRealPath();
            Path target = resolved.toRealPath();
            if (!target.startsWith(root) || target.equals(root)) {
                throw new ConfigurationException(PangrovaErrorCode.CONFIG_MISSING_KEY,
                        "Secret file path is outside allowed directory");
            }
            try (DirectoryStream<Path> directory = Files.newDirectoryStream(root)) {
                if (directory instanceof SecureDirectoryStream<Path> secure) {
                    return readRelative(secure, root.relativize(target));
                }
                // The secret mount and its directory entries must be controlled by the administrator.
                try (var input = Files.newInputStream(target, LinkOption.NOFOLLOW_LINKS);
                        var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                    return firstLine(reader);
                }
            }
        } catch (IOException e) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "Cannot read secret file: " + resolved);
        }
    }

    /** Open each canonical component relative to its parent descriptor, fencing symlink replacements. */
    private static String readRelative(SecureDirectoryStream<Path> directory, Path relative) throws IOException {
        Path name = relative.getName(0);
        if (relative.getNameCount() > 1) {
            try (var child = directory.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS)) {
                return readRelative(child, relative.subpath(1, relative.getNameCount()));
            }
        }
        try (var channel = directory.newByteChannel(name, Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
                var reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8))) {
            return firstLine(reader);
        }
    }

    private static String firstLine(BufferedReader reader) throws IOException {
        String value = reader.readLine();
        return value == null ? "" : value;
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
