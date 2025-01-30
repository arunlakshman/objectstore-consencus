package org.cloud.objectstore.consensus.api.data;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.cloud.objectstore.consensus.api.LeaderCallbacks;

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
     * The interval between attempts by the acting master to renew a leadership slot.
     */
    private final Duration renewDeadline;

    /**
     * The duration the clients should wait between retries of the leader election.
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