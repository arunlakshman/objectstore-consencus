package org.cloud.objectstore.consensus.api.watcher;


import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.data.LeaderElectionConfig;
import org.cloud.objectstore.consensus.api.data.ObjectStore;
import org.cloud.objectstore.consensus.api.data.ObjectWatcherConfig;
import org.cloud.objectstore.consensus.common.watcher.DefaultS3ObjectWatcher;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.ScheduledThreadPoolExecutor;


@Slf4j
public class ObjectWatcherFactory {

    public static ObjectStoreWatcher createObjectWatcher(ObjectWatcherConfig config,
                                                         ObjectStore objectStore,
                                                         Object objectStoreClient) {
        log.info("Creating ObjectWatcher with config: {}, objectStore: {}, objectStoreClient: {}",
                config, objectStore, objectStoreClient);

        switch (objectStore) {
            case S3:
                if (objectStoreClient instanceof S3Client) {
                    log.info("Using S3 object store with client: {}", objectStoreClient.getClass().getName());
                    return new DefaultS3ObjectWatcher(config, (S3Client) objectStoreClient,
                            config.getBucketName(),
                            config.getObjectkey(),
                            new ScheduledThreadPoolExecutor(1));
                } else {
                    log.error("Invalid client for S3: {}", objectStoreClient.getClass().getName());
                    throw new IllegalArgumentException("Invalid client for S3: " + objectStoreClient.getClass().getName());
                }
        }
        log.error("Unsupported object store: {}", objectStore);
        throw new IllegalArgumentException("Unsupported object store: " + objectStore);

    }
}
