package org.westmam.s3;

import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.leaderelection.LeaderCallbacks;
import org.cloud.objectstore.consensus.api.leaderelection.LeaderElector;
import org.cloud.objectstore.consensus.api.leaderelection.LeaderElectorFactory;
import org.cloud.objectstore.consensus.api.data.LeaderElectionConfig;
import org.cloud.objectstore.consensus.api.data.ObjectStore;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
public class LeaderElectionExample {

    public static void main(String[] args) {
        testLeaderElection();
    }

    public static void testLeaderElection() {

        S3Client s3Client = S3Client.builder().build();

        Runnable onStartLeading = () -> log.info("Starting to lead");
        Runnable onStopLeading = () -> log.info("Stopping leading");
        Consumer<String> onNewLeader = id -> log.info("New leader elected: {}", id);
        LeaderCallbacks leaderCallbacks = new LeaderCallbacks(onStartLeading, onStopLeading, onNewLeader);

        LeaderElectionConfig config = LeaderElectionConfig.builder()
                .bucketName("allaks-output-dump")
                .leaderKey("test-ha/leader")
                .leaseDuration(Duration.ofSeconds(45))
                .holderIdentity(UUID.randomUUID().toString())
                // Make calls to s3 every 15 seconds
                .retryPeriod(Duration.ofSeconds(15))
                .renewDeadline(Duration.ofSeconds(45))
                .releaseOnCancel(true)
                .leaderCallbacks(leaderCallbacks)
                .build();

        LeaderElector leaderElector = LeaderElectorFactory.createLeaderElector(config, ObjectStore.S3, s3Client);

        leaderElector.start();
    }

}
