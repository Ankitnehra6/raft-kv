package io.github.ankitnehra6.raftkv.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The log in isolation.
 *
 * <p>Every off-by-one in a Raft implementation lives at the 1-based index boundary, so the
 * boundary is tested directly rather than only through the algorithm.
 */
class RaftLogTest {

    private final RaftLog log = new RaftLog();

    private static LogEntry entry(long term, long index) {
        return new LogEntry(term, index, ("t" + term + "i" + index).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void anEmptyLogReportsZeroes() {
        assertThat(log.isEmpty()).isTrue();
        assertThat(log.lastIndex()).isZero();
        assertThat(log.lastTerm()).isZero();
        assertThat(log.entryAt(1)).isEmpty();
    }

    /** Index 0 is the sentinel that lets a leader replicate into an empty log. */
    @Test
    void indexZeroAlwaysMatchesTermZero() {
        assertThat(log.matches(0, 0)).isTrue();
        assertThat(log.matches(0, 1)).isFalse();
        assertThat(log.termAt(0)).isZero();
    }

    @Test
    void appendsMustBeContiguous() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));

        assertThat(log.lastIndex()).isEqualTo(2);
        assertThatThrownBy(() -> log.append(entry(1, 4)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contiguous");
    }

    @Test
    void rejectsAZeroOrNegativeIndex() {
        assertThatThrownBy(() -> new LogEntry(1, 0, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1-based");
    }

    @Test
    void matchesOnlyOnTheRightTerm() {
        log.append(entry(3, 1));

        assertThat(log.matches(1, 3)).isTrue();
        assertThat(log.matches(1, 2)).as("same index, wrong term").isFalse();
        assertThat(log.matches(2, 3)).as("index past the end").isFalse();
    }

    @Test
    void readsASuffix() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(2, 3));

        assertThat(log.from(2)).hasSize(2).extracting(LogEntry::index).containsExactly(2L, 3L);
        assertThat(log.from(4)).as("past the end is empty, not an error").isEmpty();
        assertThat(log.from(1)).hasSize(3);
    }

    @Test
    void truncatesFromAnIndex() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(1, 3));

        log.truncateFrom(2);

        assertThat(log.lastIndex()).isEqualTo(1);
        assertThat(log.entryAt(2)).isEmpty();
    }

    /**
     * A delayed or duplicated AppendEntries legitimately carries entries the follower
     * already has. Truncating on those would discard entries this node has already
     * acknowledged — which, if they were committed, loses data.
     */
    @Test
    void appendingEntriesItAlreadyHasChangesNothing() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(1, 3));

        long last = log.appendFrom(0, List.of(entry(1, 1), entry(1, 2)));

        assertThat(last).isEqualTo(2);
        assertThat(log.lastIndex()).as("the tail must survive a duplicate prefix").isEqualTo(3);
    }

    @Test
    void aConflictingTermTruncatesFromThatPoint() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(1, 3));

        // The leader's entry at index 2 is from a different term, so index 2 onwards is
        // discarded and replaced.
        long last = log.appendFrom(1, List.of(entry(5, 2)));

        assertThat(last).isEqualTo(2);
        assertThat(log.lastIndex()).isEqualTo(2);
        assertThat(log.termAt(2)).isEqualTo(5);
        assertThat(log.entryAt(3)).as("the divergent suffix is gone").isEmpty();
    }

    @Test
    void appendsNewEntriesPastTheEnd() {
        log.append(entry(1, 1));

        long last = log.appendFrom(1, List.of(entry(2, 2), entry(2, 3)));

        assertThat(last).isEqualTo(3);
        assertThat(log.lastIndex()).isEqualTo(3);
        assertThat(log.lastTerm()).isEqualTo(2);
    }

    @Test
    void anEmptyBatchIsAHeartbeatAndChangesNothing() {
        log.append(entry(1, 1));

        long last = log.appendFrom(1, List.of());

        assertThat(last).as("returns the prev index when there is nothing to add").isEqualTo(1);
        assertThat(log.lastIndex()).isEqualTo(1);
    }

    /** Records compare arrays by reference; log comparisons need value semantics. */
    @Test
    void entriesCompareByValue() {
        LogEntry a = new LogEntry(1, 1, "same".getBytes(StandardCharsets.UTF_8));
        LogEntry b = new LogEntry(1, 1, "same".getBytes(StandardCharsets.UTF_8));

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    }

    @Test
    void commandsAreDefensivelyCopied() {
        byte[] mutable = "original".getBytes(StandardCharsets.UTF_8);
        LogEntry stored = new LogEntry(1, 1, mutable);

        mutable[0] = 'X';
        stored.command()[1] = 'Y';

        assertThat(new String(stored.command(), StandardCharsets.UTF_8))
                .as("neither the source array nor a returned copy may mutate the entry")
                .isEqualTo("original");
    }

    @Test
    void termAtPastTheEndIsAnError() {
        log.append(entry(1, 1));

        assertThatThrownBy(() -> log.termAt(9)).isInstanceOf(IndexOutOfBoundsException.class);
    }
}
