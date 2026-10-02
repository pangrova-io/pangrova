package org.pangrova.metadata.service;

import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.spi.connector.ConnectorSession;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.TreeMap;

/** Authenticated cache scope; secrets and connection properties are stored only as a digest. */
record MetadataResolutionCacheKey(
        String tenantId, String user, String sessionDigest, QualifiedTableRef tableRef) {
    static MetadataResolutionCacheKey of(MetadataSessionContext session, QualifiedTableRef tableRef) {
        ConnectorSession connector = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        return new MetadataResolutionCacheKey(session.tenantId(), connector.getUser(), digest(session, connector),
                tableRef);
    }

    private static String digest(MetadataSessionContext session, ConnectorSession connector) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            update(digest, session.securityToken());
            new TreeMap<>(connector.getProperties()).forEach((key, value) -> {
                update(digest, key);
                update(digest, value);
            });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required", impossible);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
