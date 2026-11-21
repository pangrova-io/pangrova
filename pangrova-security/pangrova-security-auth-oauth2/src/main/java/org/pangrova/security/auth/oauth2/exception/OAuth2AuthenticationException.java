package org.pangrova.security.auth.oauth2.exception;

/**
 * Exception for unexpected authentication errors (I/O, parsing, etc.).
 */
public final class OAuth2AuthenticationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public OAuth2AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }
}
