package org.cloud.objectstore.consensus.api.leaderelection;

import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.data.LeaderElectionConfig;
import org.cloud.objectstore.consensus.api.data.ObjectStore;
import org.cloud.objectstore.consensus.common.leaderelection.DefaultLeaderElector;
import org.cloud.objectstore.consensus.common.lock.ObjectStoreBasedLock;
import org.cloud.objectstore.consensus.s3.lock.S3Lock;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Factory class for creating instances of {@link LeaderElector}.
 */
@Slf4j
public class LeaderElectorFactory {

    /**
     * Creates a {@link LeaderElector} based on the provided configuration, object store, and client.
     *
     * @param config            the leader election configuration
     * @param objectStore       the type of object store
     * @param objectStoreClient the client for the object store
     * @return a new instance of {@link LeaderElector}
     * @throws IllegalArgumentException if the object store is unsupported or the client is invalid
     */
    public static LeaderElector createLeaderElector(LeaderElectionConfig config,
                                                    ObjectStore objectStore,
                                                    Object objectStoreClient) {

        log.info("Creating LeaderElector with config: {}, objectStore: {}, objectStoreClient: {}",
                config, objectStore, objectStoreClient);

        final ObjectStoreBasedLock lock;
        switch (objectStore) {
            case S3:
                if (objectStoreClient instanceof S3Client) {
                    log.info("Using S3 object store with client: {}", objectStoreClient.getClass().getName());
                    lock = new S3Lock((S3Client) objectStoreClient,
                            config.getBucketName(),
                            config.getLeaderKey(),
                            config.getHolderIdentity());
                } else {
                    log.error("Invalid client for S3: {}", objectStoreClient.getClass().getName());
                    throw new IllegalArgumentException("Invalid client for S3: " + objectStoreClient.getClass().getName());
                }
                break;
            default:
                log.error("Unsupported object store: {}", objectStore);
                throw new IllegalArgumentException("Unsupported object store: " + objectStore);
        }

        log.info("Successfully created LeaderElector with lock: {}", lock);
        return new DefaultLeaderElector(config, lock, new ScheduledThreadPoolExecutor(1));
    }
}