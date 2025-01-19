package org.cloud.objectstore.consensus.s3;

import org.cloud.objectstore.consensus.s3.leaderelection.LeaderElectionConfig;
import org.cloud.objectstore.consensus.s3.leaderelection.LeaderElectionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.cloud.objectstore.consensus.s3.utils.Utils.scheduleWithVariableRate;

public class S3LeaderElector {

    // Metadata keys for storing leader election record
    private static final String META_HOLDER_IDENTITY = "holder-identity";
    private static final String META_LEASE_DURATION = "lease-duration-seconds";
    private static final String META_ACQUIRE_TIME = "acquire-time";
    private static final String META_RENEW_TIME = "renew-time";
    private static final String META_LEADER_TRANSITIONS = "leader-transitions";

    private static final Logger LOGGER = LoggerFactory.getLogger(S3LeaderElector.class);
    protected static final Double JITTER_FACTOR = 1.2;
    private final S3Client s3Client;
    private final String bucketName;
    private final String leaderKey;
    private final LeaderElectionConfig leaderElectionConfig;
    private final AtomicReference<LeaderElectionRecord> observedRecord = new AtomicReference<>();
    private final Executor executor;
    private boolean started;
    private boolean stopped;

    public S3LeaderElector(S3Client s3Client,
                           String bucketName,
                           String leaderKey,
                           LeaderElectionConfig config,
                           Executor executor) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.leaderKey = leaderKey;
        this.leaderElectionConfig = config;
        this.executor = executor;
    }

    public CompletableFuture<?> start() {
        synchronized (this) {
            if (started || stopped) {
                throw new IllegalStateException("LeaderElector may only be used once");
            }
            started = true;
        }
        LOGGER.debug("S3 Leader election started");
        CompletableFuture<Void> result = new CompletableFuture<>();

        CompletableFuture<?> acquireFuture = acquire();
        acquireFuture.whenComplete((v, t) -> {
            if (t == null) {
                CompletableFuture<?> renewFuture = renewWithTimeout();
                result.whenComplete((v1, t1) -> renewFuture.cancel(true));
                renewFuture.whenComplete((v1, t1) -> {
                    stopLeading();
                    if (t1 != null) {
                        result.completeExceptionally(t1);
                    } else {
                        result.complete(null);
                    }
                });
            } else {
                if (!(t instanceof CancellationException)) {
                    LOGGER.error("Exception during leader election", t);
                }
                stopLeading();
            }
        });
        result.whenComplete((v, t) -> acquireFuture.cancel(true));
        return result;
    }

    private synchronized void stopLeading() {
        stopped = true;
        LeaderElectionRecord current = observedRecord.get();
        if (current == null || !isLeader(current)) {
            return;
        }
        if (leaderElectionConfig.isReleaseOnCancel()) {
            try {
                if (release()) {
                    return;
                }
            } catch (Exception e) {
                LOGGER.error("Exception occurred while releasing leadership", e);
            }
        }
        leaderElectionConfig.getLeaderCallbacks().onStopLeading();
    }

    private CompletableFuture<Void> acquire() {
        LOGGER.debug("Attempting to acquire leader lease...");
        return loop(completion -> {
            try {
                if (tryAcquireOrRenew()) {
                    completion.complete(null);
                    LOGGER.debug("Acquired lease");
                } else {
                    LOGGER.debug("Failed to acquire lease, retrying...");
                }
            } catch (Exception e) {
                LOGGER.warn("Exception while acquiring lock, retrying...", e);
            }
        }, () -> jitter(leaderElectionConfig.getRetryPeriod(), JITTER_FACTOR).toMillis(), executor);
    }

    protected final boolean isLeader(LeaderElectionRecord record) {
        return leaderElectionConfig.getLock().identity().equals(record.getHolderIdentity());
    }

    protected final boolean canBecomeLeader(LeaderElectionRecord record) {
        return record.getHolderIdentity() == null ||
                record.getHolderIdentity().isEmpty() ||
                now().isAfter(record.getRenewTime()
                        .plus(leaderElectionConfig.getLeaseDuration()));
    }

    private CompletableFuture<Void> renewWithTimeout() {
        LOGGER.debug("Attempting to renew leader lease...");
        AtomicLong renewBy = new AtomicLong(System.currentTimeMillis() +
                leaderElectionConfig.getRenewDeadline().toMillis());

        return loop(completion -> {
            if (System.currentTimeMillis() > renewBy.get()) {
                LOGGER.debug("Renew deadline reached");
                completion.complete(null);
                return;
            }
            try {
                if (tryAcquireOrRenew()) {
                    renewBy.set(System.currentTimeMillis() +
                            leaderElectionConfig.getRenewDeadline().toMillis());
                } else {
                    completion.complete(null);
                }
            } catch (Exception e) {
                LOGGER.warn("Exception during renewal", e);
            }
        }, () -> leaderElectionConfig.getRetryPeriod().toMillis(), executor);
    }

    private void updateObserved(LeaderElectionRecord record) {
        LeaderElectionRecord current = observedRecord.getAndSet(record);
        if (record != current &&
                !record.getHolderIdentity().equals(
                        current != null ? current.getHolderIdentity() : null)) {

            String currentLeader = current != null ? current.getHolderIdentity() : null;
            String newLeader = record.getHolderIdentity();

            LOGGER.debug("Leader changed from {} to {}", currentLeader, newLeader);
            leaderElectionConfig.getLeaderCallbacks().onNewLeader(newLeader);

            if (leaderElectionConfig.getLock().identity().equals(currentLeader)) {
                leaderElectionConfig.getLeaderCallbacks().onStopLeading();
            } else if (leaderElectionConfig.getLock().identity().equals(newLeader)) {
                leaderElectionConfig.getLeaderCallbacks().onStartLeading();
            }
        }
    }

    protected static ZonedDateTime now() {
        return ZonedDateTime.now(ZoneOffset.UTC);
    }

    protected static Duration jitter(Duration duration, double maxFactor) {
        maxFactor = maxFactor > 0 ? maxFactor : 1.0;
        return duration.plusMillis(Double.valueOf(duration.toMillis() *
                Math.random() * maxFactor).longValue());
    }

    protected static CompletableFuture<Void> loop(
            Consumer<CompletableFuture<?>> consumer,
            LongSupplier delaySupplier,
            Executor executor) {
        CompletableFuture<Void> completion = new CompletableFuture<>();
        scheduleWithVariableRate(completion, executor,
                () -> consumer.accept(completion), 0,
                delaySupplier,
                TimeUnit.MILLISECONDS);
        return completion;
    }

    synchronized boolean tryAcquireOrRenew() {
        if (stopped) {
            return false;
        }

        try {
            // Try to get current leader record from object metadata
            LeaderElectionRecord oldRecord = getCurrentLeader();

            if (oldRecord == null) {
                // No current leader - try to become leader by creating initial object
                LeaderElectionRecord newRecord = new LeaderElectionRecord(
                        leaderElectionConfig.getLock().identity(),
                        leaderElectionConfig.getLeaseDuration(),
                        now(),
                        now(),
                        0
                );

                createNewLeader(newRecord);
                updateObserved(newRecord);
                return true;
            }

            updateObserved(oldRecord);
            boolean isLeader = isLeader(oldRecord);

            if (!isLeader && !canBecomeLeader(oldRecord)) {
                LOGGER.debug("Lock is held by {} and has not yet expired",
                        oldRecord.getHolderIdentity());
                return false;
            }

            // Update leader record using conditional copy
            LeaderElectionRecord newRecord = new LeaderElectionRecord(
                    leaderElectionConfig.getLock().identity(),
                    leaderElectionConfig.getLeaseDuration(),
                    isLeader ? oldRecord.getAcquireTime() : now(),
                    now(),
                    oldRecord.getLeaderTransitions() + (isLeader ? 0 : 1)
            );

            leaderElectionConfig.getLock().update(newRecord);
            updateObserved(newRecord);
            return true;

        } catch (S3Exception e) {
            if (e.statusCode() == 412) {
                // Precondition failed - someone else modified the object
                return false;
            }
            throw e;
        }
    }

    private LeaderElectionRecord getCurrentLeader() {
        try {
            HeadObjectResponse response = s3Client.headObject(req -> req
                    .bucket(bucketName)
                    .key(leaderKey)
                    .build());

            return recordFromMetadata(response.metadata());

        } catch (NoSuchKeyException e) {
            return null;
        }
    }

    private void createNewLeader(LeaderElectionRecord record) {
        // Create an empty object with leader metadata
        s3Client.putObject(req -> req
                        .bucket(bucketName)
                        .key(leaderKey)
                        .metadata(metadataFromRecord(record))
                        .build(),
                RequestBody.empty());
    }

    public synchronized boolean release() {
        LeaderElectionRecord current = getCurrentLeader();
        if (current == null || !isLeader(current)) {
            return false;
        }

        ZonedDateTime now = now();
        LeaderElectionRecord newRecord = new LeaderElectionRecord(
                "",
                Duration.ofSeconds(1),
                now,
                now,
                current.getLeaderTransitions()
        );

        leaderElectionConfig.getLock().update(newRecord);
        updateObserved(newRecord);
        return true;
    }

    private Map<String, String> metadataFromRecord(LeaderElectionRecord record) {
        return Map.of(
                META_HOLDER_IDENTITY, record.getHolderIdentity(),
                META_LEASE_DURATION, String.valueOf(record.getLeaseDuration().getSeconds()),
                META_ACQUIRE_TIME, record.getAcquireTime().toString(),
                META_RENEW_TIME, record.getRenewTime().toString(),
                META_LEADER_TRANSITIONS, String.valueOf(record.getLeaderTransitions())
        );
    }

    private LeaderElectionRecord recordFromMetadata(Map<String, String> metadata) {
        return new LeaderElectionRecord(
                metadata.get(META_HOLDER_IDENTITY),
                Duration.ofSeconds(Long.parseLong(metadata.get(META_LEASE_DURATION))),
                ZonedDateTime.parse(metadata.get(META_ACQUIRE_TIME)),
                ZonedDateTime.parse(metadata.get(META_RENEW_TIME)),
                Integer.parseInt(metadata.get(META_LEADER_TRANSITIONS))
        );
    }
}