package org.pangrova.metadata.file;

import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.file.FileSpace;

import java.util.function.Supplier;

/** Current tenant projection and an authority read boundary covering complete ticket issuance. */
public interface FileSpaceAuthority {
    /** Holds the shared authorization gate until the supplied issuance operation has returned. */
    <T> T read(Supplier<T> operation);

    /** Resolves exact active membership and namespace; authentication claims cannot invent membership. */
    UserIdentity resolve(UserIdentity authenticated, FileSpace space);
}
