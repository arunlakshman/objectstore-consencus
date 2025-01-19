package org.cloud.objectstore.consensus.lock;

import org.cloud.objectstore.consensus.s3.leaderelection.LeaderElectionRecord;

/**
 * Lock interface for leader election on top of an object storage system like S3
 */
public interface Lock {

    String LEADER_ELECTION_META_KEY = "object-storage.election.leader";

    /**
     * Returns the current {@link LeaderElectionRecord} or null if none.
     *
     * @return the current LeaderElectionRecord or null if none
     */
    LeaderElectionRecord get();

    /**
     * Attempt to create a new {@link LeaderElectionRecord}.
     *
     * @param leaderElectionRecord to update
     */
    void create(LeaderElectionRecord leaderElectionRecord);

    /**
     * Attempts to update the current {@link LeaderElectionRecord}.
     *
     * @param leaderElectionRecord to update
     */
    void update(LeaderElectionRecord leaderElectionRecord);

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
