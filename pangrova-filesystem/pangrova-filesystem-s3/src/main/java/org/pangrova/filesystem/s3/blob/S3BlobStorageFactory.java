package org.pangrova.filesystem.s3.blob;

import org.pangrova.filesystem.api.blob.BlobStorage;
import org.pangrova.filesystem.api.blob.BlobStorageFactory;

import java.util.Map;

public final class S3BlobStorageFactory implements BlobStorageFactory {
    @Override
    public String providerId() {
        return "s3";
    }

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public BlobStorage create(Map<String, String> configuration) {
        return new S3BlobStorage(S3BlobStorageSettingsParser.parse(configuration));
    }
}
