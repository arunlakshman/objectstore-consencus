package org.cloud.objectstore.consensus.common.leaderelection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * LeaderElectionRecord is the record that is stored in the leader election annotation.
 * This information should be used for observational purposes only and could be replaced
 * with a random string (e.g. UUID) with only slight modification of this code.
 */
@Builder(toBuilder = true)
@Getter
public class LeaderElectionRecord {

    private final String holderIdentity;
    private final Duration leaseDuration;
    @JsonFormat(timezone = "UTC", pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    private final ZonedDateTime acquireTime;
    @JsonFormat(timezone = "UTC", pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    private final ZonedDateTime renewTime;
    private final int leaderTransitions;

    @JsonCreator
    public LeaderElectionRecord(
            @JsonProperty("holderIdentity") String holderIdentity, @JsonProperty("leaseDuration") Duration leaseDuration,
            @JsonProperty("acquireTime") ZonedDateTime acquireTime, @JsonProperty("renewTime") ZonedDateTime renewTime,
            @JsonProperty("leaderTransitions") int leaderTransitions) {

        this.holderIdentity = holderIdentity;
        this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration is required");
        this.acquireTime = Objects.requireNonNull(acquireTime, "acquireTime is required");
        this.renewTime = Objects.requireNonNull(renewTime, "renewTime is required");
        this.leaderTransitions = leaderTransitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LeaderElectionRecord that = (LeaderElectionRecord) o;
        return leaderTransitions == that.leaderTransitions &&
                Objects.equals(holderIdentity, that.holderIdentity) &&
                Objects.equals(leaseDuration, that.leaseDuration) &&
                Objects.equals(acquireTime, that.acquireTime) &&
                Objects.equals(renewTime, that.renewTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(holderIdentity, leaseDuration, acquireTime, renewTime, leaderTransitions);
    }

}

