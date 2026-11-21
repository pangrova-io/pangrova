package org.pangrova.security.auth.oauth2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.jsonwebtoken.CompressionException;
import io.jsonwebtoken.io.CompressionAlgorithm;

/** Caps the expanded bytes handed to the signed claims parser for each supported compression format. */
record BoundedJwtCompression(CompressionAlgorithm algorithm) implements CompressionAlgorithm {
    @Override
    public String getId() {
        return algorithm.getId();
    }

    @Override
    public OutputStream compress(OutputStream destination) {
        return algorithm.compress(destination);
    }

    @Override
    public InputStream decompress(InputStream source) {
        int limit = SecureJwtParser.MAX_PAYLOAD_DECOMPRESSION_BYTES;
        try (InputStream decoded = algorithm.decompress(source)) {
            byte[] payload = decoded.readNBytes(limit + 1);
            if (payload.length > limit) {
                throw new CompressionException("JWT payload exceeds the size limit");
            }
            return new ByteArrayInputStream(payload);
        } catch (IOException e) {
            throw new CompressionException("Invalid compressed JWT payload", e);
        }
    }
}
