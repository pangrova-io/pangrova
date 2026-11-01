package org.pangrova.spi.file.authority;

import org.pangrova.spi.security.UserIdentity;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.file.space.FileSpace;

import java.util.function.Supplier;

/** Current tenant projection and an authority read boundary covering complete ticket issuance. */
@Internal
public interface FileSpaceAuthority {
    /** Holds the shared authorization gate until the supplied issuance operation has returned. */
    <T> T read(Supplier<T> operation);

    /** Resolves exact active membership and namespace; authentication claims cannot invent membership. */
    UserIdentity resolve(UserIdentity authenticated, FileSpace space);
}
