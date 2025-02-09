package org.cloud.objectstore.consensus.api.watcher;


import org.cloud.objectstore.consensus.api.data.ObjectState;

/**
 * Callback handler for object (on the object store) watcher.
 */
public interface WatchCallbackHandler {

    void onAdded(ObjectState objectState);

    void onModified(ObjectState objectState);

    void onDeleted(ObjectState objectState);

    void onError(ObjectState objectState);

    void handleError(Throwable throwable);
}
