package org.pangrova.filesystem.s3.blob;

import org.pangrova.filesystem.api.blob.BlobStorage;
import org.pangrova.filesystem.api.blob.ManagedBlobStorage;
import org.pangrova.filesystem.api.blob.ManagedBlobStorageFactory;

import java.util.Map;

public final class S3BlobStorageFactory implements ManagedBlobStorageFactory {
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

    @Override
    public ManagedBlobStorage createForCopy(Map<String, String> configuration) {
        var settings = S3BlobStorageSettingsParser.parse(configuration);
        if (settings.endpoint() == null) {
            throw new IllegalArgumentException("Managed COPY staging requires file.storage.s3.endpoint");
        }
        return S3BlobStorage.forCopyStaging(settings);
    }
}
