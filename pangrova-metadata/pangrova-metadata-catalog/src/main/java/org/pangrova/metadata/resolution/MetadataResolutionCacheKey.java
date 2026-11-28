package org.pangrova.metadata.resolution;

import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.connector.DefaultConnectorSession;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.TreeMap;

/** Authenticated cache scope; secrets and connection properties are stored only as a digest. */
record MetadataResolutionCacheKey(
        String tenantId, String user, String sessionDigest, QualifiedTableRef tableRef,
                CatalogDeploymentIdentity deployment) {
    static MetadataResolutionCacheKey of(MetadataSessionContext session, QualifiedTableRef tableRef,
            CatalogDeploymentIdentity deployment) {
        ConnectorSession connector = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        return new MetadataResolutionCacheKey(session.tenantId(), connector.getUser(), digest(session, connector),
                tableRef, deployment);
    }

    private static String digest(MetadataSessionContext session, ConnectorSession connector) {
        MessageDigest digest = Sha256Digests.sha256();
        update(digest, session.securityToken());
        new TreeMap<>(connector.getProperties()).forEach((key, value) -> {
            update(digest, key);
            update(digest, value);
        });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
