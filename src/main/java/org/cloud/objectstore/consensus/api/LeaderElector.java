package org.cloud.objectstore.consensus.api;

import java.util.concurrent.CompletableFuture;

public interface LeaderElector {

    /**
     * Start the leader election process
     *
     * @return a future that completes when the leader election process is started
     */
    CompletableFuture<Void> start();
}
