package org.pangrova.security.secret.file;

import org.pangrova.security.api.secret.SecretProviderFactory;

/** Creates a process-local FILE secret resolver. */
public final class FileSecretProviderFactory implements SecretProviderFactory {
    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public String prefix() {
        return "FILE";
    }

    @Override
    public FileSecretProvider create() {
        return new FileSecretProvider();
    }
}
