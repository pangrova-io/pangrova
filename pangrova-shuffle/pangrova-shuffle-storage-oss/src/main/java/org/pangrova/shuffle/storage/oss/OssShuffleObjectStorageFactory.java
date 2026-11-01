package org.pangrova.shuffle.storage.oss;

import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageContext;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorageFactory;

/** {@code ServiceLoader} factory for the Aliyun OSS accepted-batch storage backend. */
public final class OssShuffleObjectStorageFactory implements ShuffleObjectStorageFactory {

    public static final String NAME = "oss";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ShuffleObjectStorage create(ShuffleObjectStorageContext context) {
        return OssShuffleObjectStorage.create(context);
    }
}
