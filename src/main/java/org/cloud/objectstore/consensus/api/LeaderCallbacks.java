package org.cloud.objectstore.consensus.api;

import java.util.Objects;
import java.util.function.Consumer;

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
        onStartLeading.run();
    }

    public void onStopLeading() {
        onStopLeading.run();
    }

    public void onNewLeader(String id) {
        onNewLeader.accept(id);
    }
}
