package org.cloud.objectstore.consensus.common.utils;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class SchedulerUtils {


    private static final CachedSingleThreadScheduler SHARED_SCHEDULER = new CachedSingleThreadScheduler();

    /**
     * Schedule a repeated task to run in the given {@link Executor} - which should run the task in a different thread as to not
     * hold the scheduling thread.
     *
     * @param completion   the CompletableFuture to complete when the task is done
     * @param executor     the executor to run the task
     * @param command      the task to run
     * @param initialDelay the initial delay before the task is run
     * @param nextDelay    a supplier that provides the delay before the next execution
     * @param unit         the time unit of the delays
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

    /**
     * Helper method to schedule a task with a variable rate.
     *
     * @param runner                 the task to run
     * @param delay                  the delay before the task is run
     * @param unit                   the time unit of the delay
     * @param completion             the CompletableFuture to complete when the task is done
     * @param nextDelay              a supplier that provides the delay before the next execution
     * @param next                   the next execution time
     * @param currentScheduledFuture the current scheduled future
     */
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

}