package org.cloud.objectstore.consensus.api.data;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.cloud.objectstore.consensus.api.leaderelection.LeaderCallbacks;

import java.time.Duration;

/**
 * Configuration class for the object watcher.
 */
@Getter
@Builder
@RequiredArgsConstructor
@ToString
public class ObjectWatcherConfig {

    /**
     * The interval between attempts to watch the object. i.e. the time between calls made to the object store.
     */
    private final Duration retryPeriod;

    // Object Metadata

    /**
     * The name of the bucket where the leader election metadata is stored.
     */
    private final String bucketName;

    /**
     * The key used to identify the leader in the bucket.
     */
    private final String Objectkey;

}