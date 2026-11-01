package org.pangrova.shuffle.storage.s3;

import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageContext;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageFactory;

/** {@code ServiceLoader} factory for the S3 accepted-batch storage backend. */
public final class S3ShuffleObjectStorageFactory implements ShuffleObjectStorageFactory {

    public static final String NAME = "s3";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleObjectStorage create(ShuffleObjectStorageContext context) {
        return S3ShuffleObjectStorage.create(context);
    }
}
