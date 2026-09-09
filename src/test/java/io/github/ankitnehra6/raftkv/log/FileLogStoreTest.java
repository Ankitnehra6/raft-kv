package io.github.ankitnehra6.raftkv.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.NodeId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Durability and crash recovery.
 *
 * <p>A crash is simulated by closing the store and reopening it, and torn writes by
 * truncating or corrupting the file directly. That is the honest way to test this: waiting
 * for a real power failure is not a test strategy, and a store that has never been reopened
 * over a damaged file has not been shown to recover from one.
 */
class FileLogStoreTest {

    @TempDir Path directory;

    private static LogEntry entry(long term, long index, String command) {
        return new LogEntry(term, index, command.getBytes(StandardCharsets.UTF_8));
    }

    private static String commandOf(LogEntry entry) {
        return new String(entry.command(), StandardCharsets.UTF_8);
    }

    private Path logFile() {
        return directory.resolve("raft.log");
    }

    @Test
    void survivesACloseAndReopen() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "alpha"), entry(1, 2, "beta"), entry(2, 3, "gamma")));
        }

        try (FileLogStore reopened = new FileLogStore(directory)) {
            assertThat(reopened.lastIndex()).isEqualTo(3);
            assertThat(reopened.readAll()).extracting(FileLogStoreTest::commandOf)
                    .containsExactly("alpha", "beta", "gamma");
        }
    }

    @Test
    void anEmptyDirectoryStartsAnEmptyLog() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            assertThat(store.lastIndex()).isZero();
            assertThat(store.readAll()).isEmpty();
            assertThat(store.loadState()).isEqualTo(PersistentState.INITIAL);
        }
    }

    @Test
    void persistsTermAndVote() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.saveState(new PersistentState(7, NodeId.of("n3")));
        }

        try (FileLogStore reopened = new FileLogStore(directory)) {
            PersistentState state = reopened.loadState();
            assertThat(state.currentTerm()).isEqualTo(7);
            assertThat(state.votedForId()).contains(NodeId.of("n3"));
        }
    }

    @Test
    void persistsATermWithNoVote() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.saveState(new PersistentState(4, null));
        }

        try (FileLogStore reopened = new FileLogStore(directory)) {
            assertThat(reopened.loadState().currentTerm()).isEqualTo(4);
            assertThat(reopened.loadState().votedForId()).isEmpty();
        }
    }

    @Test
    void rejectsANonContiguousAppend() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "one")));

            assertThatThrownBy(() -> store.append(List.of(entry(1, 5, "five"))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("contiguous");
        }
    }

    @Test
    void truncationSurvivesAReopen() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "keep"), entry(1, 2, "drop"), entry(1, 3, "drop")));
            store.truncateFrom(2);
            assertThat(store.lastIndex()).isEqualTo(1);
        }

        try (FileLogStore reopened = new FileLogStore(directory)) {
            assertThat(reopened.readAll()).extracting(FileLogStoreTest::commandOf).containsExactly("keep");
        }
    }

    @Test
    void canAppendAfterTruncating() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "a"), entry(1, 2, "b")));
            store.truncateFrom(2);
            store.append(List.of(entry(5, 2, "replacement")));

            assertThat(store.readAll()).extracting(FileLogStoreTest::commandOf)
                    .containsExactly("a", "replacement");
            assertThat(store.readAll().getLast().term()).isEqualTo(5);
        }
    }

    // --- torn writes ---------------------------------------------------------------

    /**
     * A crash mid-append leaves a partial record. Recovery must keep everything before it
     * and discard the fragment — the entry was never acknowledged, so no client was told it
     * succeeded.
     */
    @Test
    void recoversFromATornTrailingRecord() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "durable"), entry(1, 2, "durable-too"), entry(1, 3, "lost")));
        }

        // Chop the last few bytes off, as a power failure mid-write would.
        long size = Files.size(logFile());
        try (var channel =
                java.nio.channels.FileChannel.open(logFile(), StandardOpenOption.WRITE)) {
            channel.truncate(size - 6);
        }

        try (FileLogStore recovered = new FileLogStore(directory)) {
            assertThat(recovered.readAll()).extracting(FileLogStoreTest::commandOf)
                    .containsExactly("durable", "durable-too");
            assertThat(recovered.lastIndex()).isEqualTo(2);
        }
    }

    /** A record whose bytes were mangled must fail its checksum, not be read as garbage. */
    @Test
    void detectsACorruptedRecordByChecksum() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "good"), entry(1, 2, "corrupted")));
        }

        byte[] bytes = Files.readAllBytes(logFile());
        // Flip a bit inside the second record's payload.
        bytes[bytes.length - 8] ^= 0x40;
        Files.write(logFile(), bytes);

        try (FileLogStore recovered = new FileLogStore(directory)) {
            assertThat(recovered.readAll())
                    .as("the corrupted record must be discarded, not silently accepted")
                    .extracting(FileLogStoreTest::commandOf)
                    .containsExactly("good");
        }
    }

    /** After recovering from damage, the log must be writable again. */
    @Test
    void canAppendAfterRecoveringFromATornTail() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "survivor"), entry(1, 2, "torn")));
        }

        long size = Files.size(logFile());
        try (var channel =
                java.nio.channels.FileChannel.open(logFile(), StandardOpenOption.WRITE)) {
            channel.truncate(size - 4);
        }

        try (FileLogStore recovered = new FileLogStore(directory)) {
            assertThat(recovered.lastIndex()).isEqualTo(1);
            recovered.append(List.of(entry(2, 2, "written-after-recovery")));
        }

        try (FileLogStore reopened = new FileLogStore(directory)) {
            assertThat(reopened.readAll()).extracting(FileLogStoreTest::commandOf)
                    .containsExactly("survivor", "written-after-recovery");
        }
    }

    /** A garbage length prefix must be rejected rather than used to size an allocation. */
    @Test
    void rejectsAnImplausibleRecordLength() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, "fine")));
        }

        // Append a record claiming to be 2GB long.
        try (var channel =
                java.nio.channels.FileChannel.open(logFile(), StandardOpenOption.APPEND)) {
            channel.write(java.nio.ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).flip());
        }

        try (FileLogStore recovered = new FileLogStore(directory)) {
            assertThat(recovered.readAll()).hasSize(1);
        }
    }

    @Test
    void handlesAnEmptyCommand() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(LogEntry.noop(3, 1)));
        }

        try (FileLogStore reopened = new FileLogStore(directory)) {
            assertThat(reopened.readAll()).hasSize(1);
            assertThat(reopened.readAll().getFirst().isNoop()).isTrue();
            assertThat(reopened.readAll().getFirst().term()).isEqualTo(3);
        }
    }

    @Test
    void handlesLargeCommands() throws IOException {
        String large = "x".repeat(200_000);

        try (FileLogStore store = new FileLogStore(directory)) {
            store.append(List.of(entry(1, 1, large)));
        }

        try (FileLogStore reopened = new FileLogStore(directory)) {
            assertThat(commandOf(reopened.readAll().getFirst())).hasSize(200_000);
        }
    }

    /** A truncated state file must not be read as a bogus term. */
    @Test
    void ignoresATruncatedStateFile() throws IOException {
        try (FileLogStore store = new FileLogStore(directory)) {
            store.saveState(new PersistentState(9, NodeId.of("n1")));
        }

        Path stateFile = directory.resolve("raft.state");
        Files.write(stateFile, new byte[] {1, 2, 3});

        try (FileLogStore reopened = new FileLogStore(directory)) {
            assertThat(reopened.loadState())
                    .as("a state file too short to parse must fall back to initial")
                    .isEqualTo(PersistentState.INITIAL);
        }
    }
}
