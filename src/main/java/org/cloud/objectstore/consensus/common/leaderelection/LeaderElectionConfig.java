package org.cloud.objectstore.consensus.common.leaderelection;

import lombok.Builder;
import lombok.Getter;
import org.cloud.objectstore.consensus.api.Lock;

import java.time.Duration;

@Getter
@Builder
public class LeaderElectionConfig {

    private final Lock lock;
    private final Duration leaseDuration;
    private final Duration renewDeadline;
    private final Duration retryPeriod;
    private final LeaderCallbacks leaderCallbacks;
    private final boolean releaseOnCancel;
    private final String name;

    public LeaderElectionConfig(Lock lock, Duration leaseDuration, Duration renewDeadline, Duration retryPeriod,
                                LeaderCallbacks leaderCallbacks, boolean releaseOnCancel, String name) {
        this.lock = lock;
        this.leaseDuration = leaseDuration;
        this.renewDeadline = renewDeadline;
        this.retryPeriod = retryPeriod;
        this.leaderCallbacks = leaderCallbacks;
        this.releaseOnCancel = releaseOnCancel;
        this.name = name;
    }

    public Lock getLock() {
        return lock;
    }

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    public Duration getRenewDeadline() {
        return renewDeadline;
    }

    public Duration getRetryPeriod() {
        return retryPeriod;
    }

    public LeaderCallbacks getLeaderCallbacks() {
        return leaderCallbacks;
    }

    public boolean isReleaseOnCancel() {
        return releaseOnCancel;
    }

    public String getName() {
        return name;
    }

}
