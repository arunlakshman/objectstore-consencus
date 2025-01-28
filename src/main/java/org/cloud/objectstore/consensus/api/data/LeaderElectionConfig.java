package org.cloud.objectstore.consensus.api.data;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.cloud.objectstore.consensus.api.LeaderCallbacks;

import java.time.Duration;

@Getter
@Builder
@RequiredArgsConstructor
@ToString
public class LeaderElectionConfig {

    private final Duration leaseDuration;
    private final Duration renewDeadline;
    private final Duration retryPeriod;
    private final LeaderCallbacks leaderCallbacks;
    private final boolean releaseOnCancel;
    private final String name;

    // Object Metadata
    private final String bucketName;
    private final String leaderKey;
    private final String holderIdentity;

}
