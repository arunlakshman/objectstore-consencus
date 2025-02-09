package org.cloud.objectstore.consensus.api.data;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.cloud.objectstore.consensus.api.leaderelection.LeaderCallbacks;

import java.time.Duration;

/**
 * Configuration class for leader election in a distributed system.
 */
@Getter
@Builder
@RequiredArgsConstructor
@ToString
public class LeaderElectionConfig {

    /**
     * The duration that non-leader candidates will wait to force acquire leadership.
     */
    private final Duration leaseDuration;

    /**
     * The deadline by which the leader must renew its lease.
     * <p>
     * Example : If the leaseDuration is 30 seconds and the renewDeadline is 25 seconds,
     * The lease renewal should be attempted every 25 seconds, essentially giving a 5 second buffer.
     * </p>
     *
     * <b>This should be less always than the lease duration.</b>
     */
    private final Duration renewDeadline;

    /**
     * The interval between attempts to check for leadership. i.e. the time between calls made to the object store.
     * <p>
     * Example : If the retryPeriod is 30 seconds,
     * The leader election process will be attempted every 30 seconds.
     * </p>
     */
    private final Duration retryPeriod;

    /**
     * Callbacks for leader election events.
     */
    private final LeaderCallbacks leaderCallbacks;

    /**
     * Indicates if the lease should be released when the election thread is cancelled.
     */
    private final boolean releaseOnCancel;

    /**
     * The name of the leader election configuration.
     */
    private final String name;

    // Object Metadata

    /**
     * The name of the bucket where the leader election metadata is stored.
     */
    private final String bucketName;

    /**
     * The key used to identify the leader in the bucket.
     */
    private final String leaderKey;

    /**
     * The identity of the holder of the leadership.
     */
    private final String holderIdentity;

}