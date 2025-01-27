package org.cloud.objectstore.consensus.exceptions;


/**
 * Exception thrown when there is a conflict writing the leader election record.
 */
public class LeaderConflictWriteException extends LeaderElectionException {

    public LeaderConflictWriteException(String message) {
        super(message);
    }

    public LeaderConflictWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
