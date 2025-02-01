package org.cloud.objectstore.consensus.common.leaderelection;

import lombok.extern.slf4j.Slf4j;
import org.cloud.objectstore.consensus.api.LeaderElector;
import org.cloud.objectstore.consensus.api.data.LeaderElectionConfig;
import org.cloud.objectstore.consensus.common.lock.ObjectStoreBasedLock;
import org.cloud.objectstore.consensus.exceptions.LeaderConflictWriteException;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.cloud.objectstore.consensus.common.utils.SchedulerUtils.scheduleWithVariableRate;

/**
 * Default implementation of the {@link LeaderElector} interface.
 */
@Slf4j
public class DefaultLeaderElector implements LeaderElector {

    protected static final Double JITTER_FACTOR = 1.2;
    private final LeaderElectionConfig leaderElectionConfig;
    /**
     * The object store based lock used for leader election.
     */
    private final ObjectStoreBasedLock lock;
    /**
     * In memory view of the latest leadership record.
     */
    private final AtomicReference<LeaderElectionRecord> observedRecord = new AtomicReference<>();
    private final Executor executor;
    private boolean started;
    private boolean stopped;

    public DefaultLeaderElector(LeaderElectionConfig config,
                                ObjectStoreBasedLock lock,
                                Executor executor) {
        this.leaderElectionConfig = config;
        this.lock = lock;
        this.executor = executor;
        log.info("DefaultLeaderElector created with config: {}, lock: {}, executor: {}", config, lock, executor);
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

    @Override
    public CompletableFuture<Void> start() {
        // Ensure that the leader election process is only started once
        synchronized (this) {
            if (started || stopped) {
                throw new IllegalStateException("LeaderElector may only be used once");
            }
            started = true;
            log.info("Leader election process started");
        }

        CompletableFuture<Void> leaderElectionProcess = new CompletableFuture<>();
        CompletableFuture<Void> acquireLeaseFuture = acquire();
        // Once the lease is acquired, then we can start the lease renewal process
        acquireLeaseFuture.whenComplete((v, t) -> {
            if (t == null) {
                log.info("Leader election: Acquired lease");
                // Start the lease renewal process
                CompletableFuture<Void> leaseRenewalFuture = renewWithTimeout();

                // Once the lease renewal is lost, then we can stop leading
                // and complete the leader election process
                leaseRenewalFuture.whenComplete((v1, t1) -> {
                    log.info("Leader election: Lease renewal lost, will stop leading");
                    stopLeading();
                    if (t1 != null) {
                        leaderElectionProcess.completeExceptionally(t1);
                    } else {
                        leaderElectionProcess.complete(null);
                    }
                });

                // Whenever we complete the leader election process, we should cancel the lease renewal
                leaderElectionProcess.whenComplete((v1, t1) -> leaseRenewalFuture.cancel(true));

            } else {
                if (!(t instanceof CancellationException)) {
                    log.error("Exception during leader election", t);
                }
                stopLeading();
            }
        });

        leaderElectionProcess.whenComplete((v, t) -> acquireLeaseFuture.cancel(true));
        return leaderElectionProcess;
    }

    private synchronized void stopLeading() {
        stopped = true;
        log.info("Stopping leading");
        LeaderElectionRecord current = observedRecord.get();
        if (current == null || !isLeader(current)) {
            return;
        }
        if (leaderElectionConfig.isReleaseOnCancel()) {
            try {
                if (release()) {
                    log.info("Leadership released successfully");
                    return;
                }
            } catch (Exception e) {
                log.error("Exception occurred while releasing leadership", e);
            }
        }
        leaderElectionConfig.getLeaderCallbacks().onStopLeading();
    }

    private CompletableFuture<Void> acquire() {
        log.info("Attempting to acquire leader lease...");
        return loop(completion -> {
            try {
                if (tryAcquireOrRenew()) {
                    completion.complete(null);
                    log.info("Acquired lease");
                } else {
                    log.info("Failed to acquire lease, retrying...");
                }
            } catch (Exception e) {
                log.error("Exception while acquiring lock, retrying...", e);
            }
        }, () -> jitter(leaderElectionConfig.getRetryPeriod(), JITTER_FACTOR).toMillis(), executor);
    }

    protected final boolean isLeader(LeaderElectionRecord record) {
        boolean isLeader = lock.identity().equals(record.getHolderIdentity());
        log.info("Checking if current identity is leader: {}", isLeader);
        return isLeader;
    }

    protected final boolean canBecomeLeader(LeaderElectionRecord record) {
        boolean canBecomeLeader = record.getHolderIdentity() == null ||
                record.getHolderIdentity().isEmpty() ||
                now().isAfter(record.getRenewTime()
                        .plus(leaderElectionConfig.getLeaseDuration()));
        log.info("Checking if can become leader: {}", canBecomeLeader);
        return canBecomeLeader;
    }

    /**
     * Attempts to renew the leadership lease.
     *
     * @return a future that completes when the lease is lost
     */
    private CompletableFuture<Void> renewWithTimeout() {
        log.info("Attempting to renew leader lease...");
        AtomicLong renewBy = new AtomicLong(System.currentTimeMillis() +
                leaderElectionConfig.getRenewDeadline().toMillis());

        return loop(completion -> {
            // If the renewal deadline has been reached, then we should stop leading
            if (System.currentTimeMillis() > renewBy.get()) {
                log.info("Renew deadline reached");
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
                log.warn("Exception during renewal", e);
            }
        }, () -> leaderElectionConfig.getRetryPeriod().toMillis(), executor);
    }

    /**
     * Updates the in memory view of the leadership record and notifies the callbacks if the leader has changed.
     *
     * @param record the new leadership record
     */
    private void updateObserved(LeaderElectionRecord record) {
        LeaderElectionRecord current = observedRecord.getAndSet(record);
        if (record != current &&
                !record.getHolderIdentity().equals(
                        current != null ? current.getHolderIdentity() : null)) {

            String currentLeader = current != null ? current.getHolderIdentity() : null;
            String newLeader = record.getHolderIdentity();

            log.info("Leader changed from {} to {}", currentLeader, newLeader);
            leaderElectionConfig.getLeaderCallbacks().onNewLeader(newLeader);

            if (lock.identity().equals(currentLeader)) {
                leaderElectionConfig.getLeaderCallbacks().onStopLeading();
            } else if (lock.identity().equals(newLeader)) {
                leaderElectionConfig.getLeaderCallbacks().onStartLeading();
            }
        }
    }

    /**
     * Tries to acquire or renew the leadership.
     *
     * @return true if leadership was acquired or renewed, false otherwise
     */
    synchronized boolean tryAcquireOrRenew() {
        if (stopped) {
            log.info("Stopped, will not try to acquire or renew");
            return false;
        }

        try {
            Optional<LeaderElectionRecord> oldRecord = lock.get();

            if (oldRecord.isEmpty()) {
                log.info("No current leader, trying to become leader by creating initial object");
                // No current leader - try to become leader by creating initial object
                LeaderElectionRecord newRecord = new LeaderElectionRecord(
                        lock.identity(),
                        leaderElectionConfig.getLeaseDuration(),
                        now(),
                        now(),
                        0,
                        "");

                lock.create(newRecord);
                updateObserved(newRecord);
            } else {
                updateObserved(oldRecord.get());
                boolean isLeader = isLeader(oldRecord.get());

                if (!isLeader && !canBecomeLeader(oldRecord.get())) {
                    log.info("Lock is held by {} and has not yet expired",
                            oldRecord.get().getHolderIdentity());
                    return false;
                }

                // Update leader record using conditional copy
                LeaderElectionRecord newRecord = new LeaderElectionRecord(
                        lock.identity(),
                        leaderElectionConfig.getLeaseDuration(),
                        isLeader ? oldRecord.get().getAcquireTime() : now(),
                        now(),
                        oldRecord.get().getLeaderTransitions() + (isLeader ? 0 : 1),
                        "");

                lock.update(oldRecord.get().getEtag(), newRecord);
                updateObserved(newRecord);
            }
            log.info("Successfully acquired or renewed leadership");
            return true;

        } catch (LeaderConflictWriteException e) {
            // Another leader has been elected
            log.info("Another leader has been elected");
            return false;
        }
    }

    public synchronized boolean release() {
        Optional<LeaderElectionRecord> current = lock.get();
        if (current.isEmpty() || !isLeader(current.get())) {
            log.info("No current leader or not the leader, nothing to release");
            return false;
        }

        ZonedDateTime now = now();
        LeaderElectionRecord newRecord = new LeaderElectionRecord(
                "",
                Duration.ofSeconds(1),
                now,
                now,
                current.get().getLeaderTransitions(),
                "");

        lock.update(current.get().getEtag(), newRecord);
        updateObserved(newRecord);
        log.info("Leadership released");
        return true;
    }
}