package org.pangrova.coordinator.file;

import java.net.URI;
import java.util.Objects;
import java.util.UUID;

import jakarta.ws.rs.core.UriBuilder;
import org.pangrova.file.core.space.FileSpaceAccessService;
import org.pangrova.spi.security.UserIdentity;

/** Role-authorized stable Stage links; bytes are transferred by the selected Worker. */
public final class StageFileUrlService {
    private final FileSpaceAccessService access;

    /** The issuer owns the shared durable authority boundary for both links and tickets. */
    public StageFileUrlService(FileSpaceAccessService access) {
        this.access = Objects.requireNonNull(access, "access");
    }

    /** Builds a credential-free URL. Construction does not require an online Worker or an existing object. */
    public URI build(UserIdentity user, String catalog, String schema, String name, String path, URI baseUri) {
        String relative = FileSpaceAccessService.requireObjectPath(path);
        var stage = access.stageLink(user, catalog, schema, name, relative);
        URI result = UriBuilder.fromUri(baseUri).path("v1/stage-files/{id}")
                .queryParam("path", relative).build(stage.space().id());
        access.stageLinkIssued(stage);
        return result;
    }

    /** Re-authorizes each access and exchanges the stable identity for one bounded Worker read ticket. */
    public URI read(UserIdentity user, UUID id, String path) {
        var grant = access.stageRead(user, id, path);
        return UriBuilder.fromUri(grant.workerUrl()).path("v1/file-space/object")
                .queryParam("descriptor", grant.descriptor())
                .queryParam("signature", grant.signature()).build();
    }

}
