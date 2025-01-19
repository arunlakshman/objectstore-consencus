package org.cloud.objectstore.consensus.common.utils;

import org.cloud.objectstore.consensus.common.CachedSingleThreadScheduler;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class Utils {


    /**
     * Create a {@link ThreadFactory} with daemon threads and a thread
     * name based upon the object passed in.
     */
    public static ThreadFactory daemonThreadFactory(Object forObject) {
        String name = forObject.getClass().getSimpleName() + "-" + System.identityHashCode(forObject);
        return daemonThreadFactory(name);
    }

    public static ThreadFactory daemonThreadFactory(String name) {
        return new ThreadFactory() {
            final ThreadFactory threadFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                Thread ret = threadFactory.newThread(r);
                ret.setName(name + "-" + ret.getName());
                ret.setDaemon(true);
                return ret;
            }
        };
    }

    private static final CachedSingleThreadScheduler SHARED_SCHEDULER = new CachedSingleThreadScheduler();

    /**
     * Schedule a repeated task to run in the given {@link Executor} - which should run the task in a different thread as to not
     * hold the scheduling thread.
     * <p>
     *
     * @param nextDelay provides the relative next delay - that is the values are applied cumulatively to the initial start
     *                  time. Supplying a fixed value produces a fixed rate.
     */
    public static void scheduleWithVariableRate(CompletableFuture<?> completion, Executor executor, Runnable command,
                                                long initialDelay,
                                                LongSupplier nextDelay, TimeUnit unit) {
        AtomicReference<ScheduledFuture<?>> currentScheduledFuture = new AtomicReference<>();
        AtomicLong next = new AtomicLong(unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS) + Math.max(0, initialDelay));
        schedule(() -> CompletableFuture.runAsync(command, executor), initialDelay, unit, completion, nextDelay, next,
                currentScheduledFuture);
        // remove on cancel is true, so this may proactively clean up
        completion.whenComplete((v, t) -> Optional.ofNullable(currentScheduledFuture.get()).ifPresent(s -> s.cancel(true)));
    }

    private static void schedule(Supplier<CompletableFuture<?>> runner, long delay, TimeUnit unit,
                                 CompletableFuture<?> completion, LongSupplier nextDelay, AtomicLong next,
                                 AtomicReference<ScheduledFuture<?>> currentScheduledFuture) {
        currentScheduledFuture.set(SHARED_SCHEDULER.schedule(() -> {
            if (completion.isDone()) {
                return;
            }
            CompletableFuture<?> runAsync = runner.get();
            runAsync.whenComplete((v, t) -> {
                if (t != null) {
                    completion.completeExceptionally(t);
                } else if (!completion.isDone()) {
                    schedule(runner, next.addAndGet(nextDelay.getAsLong()) - unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS),
                            unit, completion, nextDelay, next, currentScheduledFuture);
                }
            });
        }, delay, unit));
    }

    /**
     * Schedule a task to run in the given {@link Executor} - which should run the task in a different thread as to not
     * hold the scheduling thread
     */
    public static CompletableFuture<Void> schedule(Executor executor, Runnable command, long delay, TimeUnit unit) {
        // to be replaced in java 9+ with CompletableFuture.runAsync(command, CompletableFuture.delayedExecutor(delay, unit, executor));
        CompletableFuture<Void> result = new CompletableFuture<>();
        ScheduledFuture<?> scheduledFuture = SHARED_SCHEDULER.schedule(() -> {
            try {
                executor.execute(command);
                result.complete(null);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        }, delay, unit);
        result.whenComplete((v, t) -> {
            scheduledFuture.cancel(true);
        });
        return result;
    }

}