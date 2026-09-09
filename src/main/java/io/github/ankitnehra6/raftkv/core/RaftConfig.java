package io.github.ankitnehra6.raftkv.core;

/**
 * Timing parameters, expressed in ticks rather than milliseconds.
 *
 * <p>A tick is whatever the driver decides it is: the simulation advances one per loop
 * iteration, a real server advances one every few milliseconds. Keeping the algorithm in
 * logical time is what lets a test compress an hour of cluster behaviour into a
 * millisecond, and it removes every {@code Thread.sleep} from the test suite.
 *
 * @param electionTimeoutMinTicks lower bound of the randomised election timeout
 * @param electionTimeoutMaxTicks upper bound. The spread is essential, not cosmetic: with
 *     a fixed timeout every follower would call an election at the same instant, split the
 *     vote, and repeat — potentially forever.
 * @param heartbeatIntervalTicks how often a leader sends heartbeats. Must be comfortably
 *     below the election timeout or followers will time out on a healthy leader.
 */
public record RaftConfig(
        int electionTimeoutMinTicks, int electionTimeoutMaxTicks, int heartbeatIntervalTicks) {

    public RaftConfig {
        if (electionTimeoutMinTicks < 1) {
            throw new IllegalArgumentException("election timeout must be at least 1 tick");
        }
        if (electionTimeoutMaxTicks < electionTimeoutMinTicks) {
            throw new IllegalArgumentException("election timeout max must be >= min");
        }
        if (heartbeatIntervalTicks < 1) {
            throw new IllegalArgumentException("heartbeat interval must be at least 1 tick");
        }
        if (heartbeatIntervalTicks >= electionTimeoutMinTicks) {
            throw new IllegalArgumentException(
                    "heartbeat interval (%d) must be below the minimum election timeout (%d), "
                            .formatted(heartbeatIntervalTicks, electionTimeoutMinTicks)
                            + "or followers will time out on a healthy leader");
        }
    }

    /** A sane default: heartbeat every 3 ticks, elections between 10 and 20. */
    public static RaftConfig defaults() {
        return new RaftConfig(10, 20, 3);
    }
}
