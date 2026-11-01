/**
 * S3 blob provider, encryption policy, SDK configuration and bounded COPY transport.
 *
 * <p>Owned by pangrova-storage-s3. Consumers are storage providers and explicit
 * process assembly; governed FILE and COPY consumers use the API. Authorization, source
 * version authority and query lifecycle do not belong in this package.
 * Provider constructors and transport policy are internal APIs for assembly and S3 source adapters.
 */
package org.pangrova.storage.s3.blob;
