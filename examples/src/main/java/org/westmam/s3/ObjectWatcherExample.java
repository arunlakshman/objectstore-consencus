package org.westmam.s3;

import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.data.LeaderElectionConfig;
import org.cloud.objectstore.consensus.api.data.ObjectStore;
import org.cloud.objectstore.consensus.api.data.ObjectState;
import org.cloud.objectstore.consensus.api.data.ObjectWatcherConfig;
import org.cloud.objectstore.consensus.api.watcher.ObjectStoreWatcher;
import org.cloud.objectstore.consensus.api.watcher.ObjectWatcherFactory;
import org.cloud.objectstore.consensus.api.watcher.WatchCallbackHandler;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.UUID;

@Slf4j
public class ObjectWatcherExample {

    public static void main(String[] args) {
        testObjectWatcher();
    }

    private static void testObjectWatcher() {
        S3Client s3Client = S3Client.builder().build();

        ObjectWatcherConfig config = ObjectWatcherConfig.builder()
                .bucketName("allaks-output-dump")
                .Objectkey("test-ha/leader")
                // Make calls to s3 every 15 seconds
                .retryPeriod(Duration.ofSeconds(5))
                .build();

        ObjectStoreWatcher objectStoreWatcher = ObjectWatcherFactory.createObjectWatcher(config, ObjectStore.S3, s3Client);

        objectStoreWatcher.startWatch(new WatchCallbackHandler() {
            @Override
            public void onAdded(ObjectState objectState) {
                log.info("Object added: {}", objectState);
            }

            @Override
            public void onModified(ObjectState objectState) {
                log.info("Object modified: {}", objectState);
            }

            @Override
            public void onDeleted(ObjectState objectState) {
                log.info("Object deleted: {}", objectState);
            }

            @Override
            public void onError(ObjectState objectState) {
                log.error("Error: {}", objectState);
            }

            @Override
            public void handleError(Throwable throwable) {
                log.error("Error: ", throwable);
            }
        });
    }
}
