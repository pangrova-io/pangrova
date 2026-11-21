package org.pangrova.filesystem.oss.blob;

import org.pangrova.filesystem.api.blob.BlobStorage;
import org.pangrova.filesystem.api.blob.BlobStorageFactory;

import java.util.Map;

public final class OssBlobStorageFactory implements BlobStorageFactory {
    @Override
    public String providerId() {
        return "oss";
    }

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public BlobStorage create(Map<String, String> configuration) {
        return new OssBlobStorage(OssBlobStorageSettingsParser.parse(configuration));
    }
}
