package org.cloud.objectstore.consensus.api.leaderelection;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.function.Consumer;

@Slf4j
public class LeaderCallbacks {

    private final Runnable onStartLeading;
    private final Runnable onStopLeading;
    private final Consumer<String> onNewLeader;

    public LeaderCallbacks(Runnable onStartLeading, Runnable onStopLeading, Consumer<String> onNewLeader) {
        this.onStartLeading = Objects.requireNonNull(onStartLeading, "onStartLeading callback is required");
        this.onStopLeading = Objects.requireNonNull(onStopLeading, "onStopLeading callback is required");
        this.onNewLeader = Objects.requireNonNull(onNewLeader, "onNewLeader callback is required");
    }

    public void onStartLeading() {
        log.info("Starting to lead");
        onStartLeading.run();
    }

    public void onStopLeading() {
        log.info("Stopping leading");
        onStopLeading.run();
    }

    public void onNewLeader(String id) {
        log.info("New leader elected: {}", id);
        onNewLeader.accept(id);
    }
}