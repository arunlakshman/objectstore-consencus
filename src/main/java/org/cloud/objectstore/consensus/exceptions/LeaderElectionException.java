package org.cloud.objectstore.consensus.exceptions;

/**
 * Base Exception for leader election related exceptions
 */
public class LeaderElectionException extends RuntimeException {

    public LeaderElectionException(String message) {
        super(message);
    }

    public LeaderElectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
