package org.pangrova.security.secret;

/**
 * SPI for resolving secret references by prefix (e.g. ENV, FILE, VAULT).
 */
public interface SecretProvider {

    String prefix();

    String resolve(String reference);
}
