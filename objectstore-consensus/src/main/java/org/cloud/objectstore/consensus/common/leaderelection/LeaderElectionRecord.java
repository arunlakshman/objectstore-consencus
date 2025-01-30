package org.cloud.objectstore.consensus.common.leaderelection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;

/**
 * LeaderElectionRecord is the record that is stored in the leader election annotation.
 * This information should be used for observational purposes only and could be replaced
 * with a random string (e.g. UUID) with only slight modification of this code.
 */
@Builder(toBuilder = true)
@Getter
public class LeaderElectionRecord {

    // Metadata keys for storing leader election record
    private static final String META_HOLDER_IDENTITY = "holder-identity";
    private static final String META_LEASE_DURATION = "lease-duration-seconds";
    private static final String META_ACQUIRE_TIME = "acquire-time";
    private static final String META_RENEW_TIME = "renew-time";
    private static final String META_LEADER_TRANSITIONS = "leader-transitions";

    private final String holderIdentity;
    private final Duration leaseDuration;
    @JsonFormat(timezone = "UTC", pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    private final ZonedDateTime acquireTime;
    @JsonFormat(timezone = "UTC", pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    private final ZonedDateTime renewTime;
    private final int leaderTransitions;
    private final String etag;

    @JsonCreator
    public LeaderElectionRecord(
            @JsonProperty("holderIdentity") String holderIdentity, @JsonProperty("leaseDuration") Duration leaseDuration,
            @JsonProperty("acquireTime") ZonedDateTime acquireTime, @JsonProperty("renewTime") ZonedDateTime renewTime,
            @JsonProperty("leaderTransitions") int leaderTransitions, String etag) {

        this.holderIdentity = holderIdentity;
        this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration is required");
        this.acquireTime = Objects.requireNonNull(acquireTime, "acquireTime is required");
        this.renewTime = Objects.requireNonNull(renewTime, "renewTime is required");
        this.leaderTransitions = leaderTransitions;
        this.etag = etag;
    }

    public static Map<String, String> metadataFromRecord(LeaderElectionRecord record) {
        return Map.of(
                META_HOLDER_IDENTITY, record.getHolderIdentity(),
                META_LEASE_DURATION, String.valueOf(record.getLeaseDuration().getSeconds()),
                META_ACQUIRE_TIME, record.getAcquireTime().toString(),
                META_RENEW_TIME, record.getRenewTime().toString(),
                META_LEADER_TRANSITIONS, String.valueOf(record.getLeaderTransitions())
        );
    }

    public static LeaderElectionRecord recordFromMetadata(Map<String, String> metadata, String etag) {
        return new LeaderElectionRecord(
                metadata.get(META_HOLDER_IDENTITY),
                Duration.ofSeconds(Long.parseLong(metadata.get(META_LEASE_DURATION))),
                ZonedDateTime.parse(metadata.get(META_ACQUIRE_TIME)),
                ZonedDateTime.parse(metadata.get(META_RENEW_TIME)),
                Integer.parseInt(metadata.get(META_LEADER_TRANSITIONS)),
                etag);
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

