package org.pangrova.security.secret.env;

import org.pangrova.security.api.secret.SecretProviderFactory;

/** Creates a process-local ENV secret resolver. */
public final class EnvSecretProviderFactory implements SecretProviderFactory {
    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public String prefix() {
        return "ENV";
    }

    @Override
    public EnvSecretProvider create() {
        return new EnvSecretProvider();
    }
}
