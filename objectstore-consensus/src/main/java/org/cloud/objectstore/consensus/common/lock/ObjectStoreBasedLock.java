package org.cloud.objectstore.consensus.common.lock;

import org.cloud.objectstore.consensus.common.leaderelection.LeaderElectionRecord;
import org.cloud.objectstore.consensus.exceptions.LeaderConflictWriteException;
import org.cloud.objectstore.consensus.exceptions.LeaderElectionException;

import java.util.Optional;

/**
 * Lock interface for leader election on top of an object storage system like S3
 */
public interface ObjectStoreBasedLock {

    /**
     * Returns the current {@link LeaderElectionRecord} or empty if none.
     *
     * @return the current LeaderElectionRecord or null if none
     * @throws LeaderElectionException if there is an error getting the record
     */
    Optional<LeaderElectionRecord> get() throws LeaderElectionException;

    /**
     * Attempt to create a new {@link LeaderElectionRecord}.
     *
     * @param leaderElectionRecord to update
     * @throws LeaderConflictWriteException if there is an error creating the record
     */
    void create(LeaderElectionRecord leaderElectionRecord) throws LeaderConflictWriteException;

    /**
     * Attempts to update the current {@link LeaderElectionRecord}.
     *
     * @param oldEtag              E-tag of the current record
     * @param leaderElectionRecord to update
     * @throws LeaderConflictWriteException if there is an error updating the record
     */
    void update(String oldEtag, LeaderElectionRecord leaderElectionRecord) throws LeaderConflictWriteException;

    /**
     * Returns the unique id of the lock holder.
     * <p>
     * This id is to compare ids with current {@link LeaderElectionRecord#getHolderIdentity()}
     * to check for leadership.
     *
     * @return unique id for the lock
     */
    String identity();

    /**
     * Full description of the current lock.
     *
     * @return lock description
     */
    String describe();
}
