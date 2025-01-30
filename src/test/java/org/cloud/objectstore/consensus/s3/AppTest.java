package org.cloud.objectstore.consensus.s3;

import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.LeaderCallbacks;
import software.amazon.awssdk.services.s3.S3Client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

import org.cloud.objectstore.consensus.api.LeaderElector;
import org.cloud.objectstore.consensus.api.LeaderElectorFactory;
import org.cloud.objectstore.consensus.api.data.LeaderElectionConfig;
import org.cloud.objectstore.consensus.api.data.ObjectStore;

/**
 * Unit test for simple App.
 */
@Slf4j
public class AppTest {

    public static void main(String[] args) {
        new AppTest().shouldAnswerWithTrue();
    }
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        log.info("Test case executed successfully");

        S3Client s3Client = S3Client.builder().build();

        Runnable onStartLeading = () -> log.info("Starting to lead");
        Runnable onStopLeading = () -> log.info("Stopping leading");
        Consumer<String> onNewLeader = id -> log.info("New leader elected: {}", id);
        LeaderCallbacks leaderCallbacks = new LeaderCallbacks(onStartLeading, onStopLeading, onNewLeader);

        LeaderElectionConfig config = LeaderElectionConfig.builder()
                .bucketName("allaks-output-dump")
                .leaderKey("test-ha/leader")
                .leaseDuration(Duration.ofSeconds(23))
                .holderIdentity(UUID.randomUUID().toString())
                .retryPeriod(Duration.ofSeconds(10))
                .renewDeadline(Duration.ofSeconds(15))
                .releaseOnCancel(true)
                .leaderCallbacks(leaderCallbacks)
                .build();

        LeaderElector leaderElector = LeaderElectorFactory.createLeaderElector(config, ObjectStore.S3, s3Client);

        leaderElector.start();

        assertTrue(true);

        sleep(1543 * 1000);
    }

    public static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.error("Error sleeping", e);
        }
    }

}
