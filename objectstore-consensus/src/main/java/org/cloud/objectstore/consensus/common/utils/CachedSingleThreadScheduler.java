package org.cloud.objectstore.consensus.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Maintains a single thread daemon scheduler, which will terminate the thread
 * when not in use.
 *
 * <br>
 * It is not intended for long-running tasks,
 * but it does not assume the task can be handed off to the common pool
 *
 * <br>
 * This is very similar to the CompletableFuture.Delayer, but provides a scheduler method
 */
@Slf4j
public class CachedSingleThreadScheduler {

    /**
     * Default time-to-live for the scheduler in milliseconds.
     */
    public static final long DEFAULT_TTL_MILLIS = TimeUnit.SECONDS.toMillis(10);

    /**
     * Time-to-live for the scheduler in milliseconds.
     */
    private final long ttlMillis;

    /**
     * The scheduled thread pool executor.
     */
    private ScheduledThreadPoolExecutor executor;

    /**
     * Constructs a CachedSingleThreadScheduler with the default TTL.
     */
    public CachedSingleThreadScheduler() {
        this(DEFAULT_TTL_MILLIS);
        log.info("CachedSingleThreadScheduler created with default TTL: {} ms", DEFAULT_TTL_MILLIS);
    }

    /**
     * Constructs a CachedSingleThreadScheduler with a specified TTL.
     *
     * @param ttlMillis the time-to-live in milliseconds
     */
    public CachedSingleThreadScheduler(long ttlMillis) {
        this.ttlMillis = ttlMillis;
        log.info("CachedSingleThreadScheduler created with TTL: {} ms", ttlMillis);
    }

    /**
     * Create a {@link ThreadFactory} with daemon threads and a thread
     * name based upon the object passed in.
     *
     * @param forObject the object to base the thread name on
     * @return a ThreadFactory that creates daemon threads
     */
    static ThreadFactory daemonThreadFactory(Object forObject) {
        String name = forObject.getClass().getSimpleName() + "-" + System.identityHashCode(forObject);
        log.info("Creating daemon thread factory with name: {}", name);
        return daemonThreadFactory(name);
    }

    /**
     * Create a {@link ThreadFactory} with daemon threads and a specified thread name.
     *
     * @param name the base name for the threads
     * @return a ThreadFactory that creates daemon threads
     */
    static ThreadFactory daemonThreadFactory(String name) {
        return new ThreadFactory() {
            final ThreadFactory threadFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                Thread ret = threadFactory.newThread(r);
                ret.setName(name + "-" + ret.getName());
                ret.setDaemon(true);
                log.info("Created new daemon thread: {}", ret.getName());
                return ret;
            }
        };
    }

    /**
     * Schedules a command to be run periodically with a fixed delay between
     * the end of one execution and the start of the next.
     *
     * @param command      the task to execute
     * @param initialDelay the time to delay first execution
     * @param delay        the delay between the termination of one execution and the commencement of the next
     * @param unit         the time unit of the initialDelay and delay parameters
     * @return a ScheduledFuture representing pending completion of the task
     */
    public synchronized ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                                  long initialDelay,
                                                                  long delay,
                                                                  TimeUnit unit) {
        log.info("Scheduling task with fixed delay: initialDelay={} {}, delay={} {}", initialDelay, unit, delay, unit);
        startExecutor();
        return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * Schedules a command to be executed after the given delay.
     *
     * @param command the task to execute
     * @param delay   the time from now to delay execution
     * @param unit    the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task
     */
    public synchronized ScheduledFuture<?> schedule(Runnable command,
                                                    long delay,
                                                    TimeUnit unit) {
        log.info("Scheduling task with delay: delay={} {}", delay, unit);
        startExecutor();
        return executor.schedule(command, delay, unit);
    }

    /**
     * Starts the executor if it is not already started and schedules a task
     * to check for shutdown.
     */
    private void startExecutor() {
        if (executor == null) {
            log.info("Starting executor");
            executor = new ScheduledThreadPoolExecutor(1, daemonThreadFactory(this));
            executor.setRemoveOnCancelPolicy(true);
            executor.scheduleWithFixedDelay(this::shutdownCheck, ttlMillis, ttlMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Checks if the executor should be shut down and shuts it down if the queue is empty.
     */
    private synchronized void shutdownCheck() {
        if (executor.getQueue().isEmpty()) {
            log.info("Shutting down executor due to empty queue");
            executor.shutdownNow();
            executor = null;
        }
    }

    /**
     * Checks if the executor is currently running.
     *
     * @return true if the executor is running, false otherwise
     */
    synchronized boolean hasExecutor() {
        boolean hasExecutor = executor != null;
        log.info("Checking if executor is running: {}", hasExecutor);
        return hasExecutor;
    }
}