package org.cloud.objectstore.consensus.api.atomic;

import org.cloud.objectstore.consensus.api.data.ObjectState;

import java.util.function.Function;

public interface ObjectStoreAtomicOperations {

    /**
     * Update the object state in the object store.
     *
     * @param updateFunction the function to update the object state
     * @return true if the update was successful, false otherwise
     */
    boolean update(Function<ObjectState, ObjectState> updateFunction);
}
