package org.cloud.objectstore.consensus.api.watcher;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for watching objects on the object store.
 */
public interface ObjectStoreWatcher {

    /**
     * Start watching objects on the object store.
     *
     * @param handler the callback handler for the watcher
     * @return a future that starts immediately when the watch is started
     */
    CompletableFuture<Void> startWatch(WatchCallbackHandler handler);

}
