package io.github.ankitnehra6.raftkv.log;

import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.NodeId;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.zip.CRC32;

/**
 * An append-only, crash-safe log on disk.
 *
 * <p>Each record is length-prefixed and CRC-checked:
 *
 * <pre>
 *   payloadLength (4) | term (8) | index (8) | commandLength (4) | command | crc32 (4)
 * </pre>
 *
 * <p>The design assumes a crash can happen at any byte. A record half-written when the
 * power failed will fail either its length check or its CRC, and recovery stops there,
 * keeping every record before it. That is safe precisely because Raft never treats an entry
 * as committed until a majority has acknowledged it — an entry lost to a torn write was, by
 * definition, never acknowledged by this node, so no client was ever told it succeeded.
 *
 * <p>Truncation is the one operation that is not append-only, and it happens when a leader
 * overwrites a follower's divergent suffix. It is implemented by rewriting the file: slower
 * than seeking, and far easier to reason about, since a crash mid-rewrite leaves either the
 * old file or the new one rather than a spliced hybrid.
 */
public class FileLogStore implements LogStore {

    // System.Logger rather than a logging framework: this project has no runtime
    // dependencies and a storage layer is not a good reason to acquire the first one.
    private static final Logger log = System.getLogger(FileLogStore.class.getName());

    /** term + index + commandLength. */
    private static final int HEADER_BYTES = 8 + 8 + 4;

    private static final int LENGTH_PREFIX_BYTES = 4;
    private static final int CRC_BYTES = 4;

    /** Refuses absurd lengths from a corrupt file rather than trying to allocate them. */
    private static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;

    private final Path logFile;
    private final Path stateFile;
    private final FileChannel channel;
    private final boolean syncOnWrite;

    private final List<LogEntry> cached = new ArrayList<>();

    public FileLogStore(Path directory) {
        this(directory, true);
    }

    /**
     * @param syncOnWrite whether to fsync each append. Only ever false in benchmarks: with
     *     it off the store is fast and offers no crash guarantee at all.
     */
    public FileLogStore(Path directory, boolean syncOnWrite) {
        this.syncOnWrite = syncOnWrite;
        try {
            Files.createDirectories(directory);
            this.logFile = directory.resolve("raft.log");
            this.stateFile = directory.resolve("raft.state");
            this.channel =
                    FileChannel.open(
                            logFile,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
            recover();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open log store in " + directory, e);
        }
    }

    /**
     * Reads the log back, stopping at the first record that is incomplete or fails its
     * checksum.
     *
     * <p>The channel is then truncated to the last good record, so the next append does not
     * write past a torn tail and leave a permanently unreadable file.
     */
    private void recover() throws IOException {
        long position = 0;
        long size = channel.size();

        while (position < size) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(LENGTH_PREFIX_BYTES);
            if (channel.read(lengthBuffer, position) < LENGTH_PREFIX_BYTES) {
                break; // torn: not even a length
            }
            int payloadLength = lengthBuffer.flip().getInt();

            if (payloadLength < HEADER_BYTES || payloadLength > MAX_RECORD_BYTES) {
                log.log(Level.WARNING, "log record at offset %d has an implausible length %d"
                                .formatted(position, payloadLength));
                break;
            }

            long recordEnd = position + LENGTH_PREFIX_BYTES + payloadLength + CRC_BYTES;
            if (recordEnd > size) {
                break; // torn: the record was cut short
            }

            ByteBuffer body = ByteBuffer.allocate(payloadLength + CRC_BYTES);
            channel.read(body, position + LENGTH_PREFIX_BYTES);
            body.flip();

            byte[] payload = new byte[payloadLength];
            body.get(payload);
            int storedCrc = body.getInt();

            CRC32 crc = new CRC32();
            crc.update(payload);
            if ((int) crc.getValue() != storedCrc) {
                log.log(Level.WARNING, "log record at offset %d failed its checksum; truncating here"
                                .formatted(position));
                break;
            }

            cached.add(decode(payload));
            position = recordEnd;
        }

        if (position < size) {
            log.log(Level.WARNING, "truncating %d bytes of damaged tail from %s"
                            .formatted(size - position, logFile));
            channel.truncate(position);
            channel.force(true);
        }
    }

    private static LogEntry decode(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        long term = buffer.getLong();
        long index = buffer.getLong();
        int commandLength = buffer.getInt();
        byte[] command = new byte[commandLength];
        buffer.get(command);
        return new LogEntry(term, index, command);
    }

    private static byte[] encode(LogEntry entry) {
        byte[] command = entry.command();

        // Built payload-first so the checksum is computed over exactly the bytes recovery
        // will verify, with no offset arithmetic to get wrong.
        byte[] payload =
                ByteBuffer.allocate(HEADER_BYTES + command.length)
                        .putLong(entry.term())
                        .putLong(entry.index())
                        .putInt(command.length)
                        .put(command)
                        .array();

        CRC32 crc = new CRC32();
        crc.update(payload);

        return ByteBuffer.allocate(LENGTH_PREFIX_BYTES + payload.length + CRC_BYTES)
                .putInt(payload.length)
                .put(payload)
                .putInt((int) crc.getValue())
                .array();
    }

    @Override
    public void append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        try {
            for (LogEntry entry : entries) {
                long expected = lastIndex() + 1;
                if (entry.index() != expected) {
                    throw new IllegalArgumentException(
                            "log must be contiguous: expected %d, got %d"
                                    .formatted(expected, entry.index()));
                }
                channel.position(channel.size());
                channel.write(ByteBuffer.wrap(encode(entry)));
                cached.add(entry);
            }
            if (syncOnWrite) {
                // The entry is not durable until this returns, and Raft must not
                // acknowledge it before then.
                channel.force(false);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append to " + logFile, e);
        }
    }

    @Override
    public void truncateFrom(long index) {
        if (index < 1 || index > lastIndex()) {
            return;
        }
        List<LogEntry> keep = new ArrayList<>(cached.subList(0, (int) (index - 1)));
        rewrite(keep);
    }

    /**
     * Rewrites the log to a temporary file and moves it into place.
     *
     * <p>An atomic move means a crash leaves either the whole old log or the whole new one.
     * Truncating in place would leave a window in which the file is neither.
     */
    private void rewrite(List<LogEntry> entries) {
        Path temporary = logFile.resolveSibling("raft.log.rewrite");
        try {
            try (FileChannel out =
                    FileChannel.open(
                            temporary,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE)) {
                for (LogEntry entry : entries) {
                    out.write(ByteBuffer.wrap(encode(entry)));
                }
                out.force(true);
            }

            Files.move(temporary, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            cached.clear();
            cached.addAll(entries);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to rewrite " + logFile, e);
        }
    }

    @Override
    public List<LogEntry> readAll() {
        return List.copyOf(cached);
    }

    @Override
    public long lastIndex() {
        return cached.isEmpty() ? 0 : cached.getLast().index();
    }

    @Override
    public void saveState(PersistentState state) {
        // Written to a temporary file and moved into place, so a crash mid-write cannot
        // leave a half-updated term — which would be worse than either the old or new one.
        Path temporary = stateFile.resolveSibling("raft.state.tmp");
        byte[] votedFor =
                state.votedFor() == null
                        ? new byte[0]
                        : state.votedFor().value().getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer =
                ByteBuffer.allocate(8 + 4 + votedFor.length)
                        .putLong(state.currentTerm())
                        .putInt(votedFor.length)
                        .put(votedFor);

        try {
            try (FileChannel out =
                    FileChannel.open(
                            temporary,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE)) {
                out.write(buffer.flip());
                out.force(true);
            }
            Files.move(temporary, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to save state to " + stateFile, e);
        }
    }

    @Override
    public PersistentState loadState() {
        if (!Files.exists(stateFile)) {
            return PersistentState.INITIAL;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(stateFile));
            if (buffer.remaining() < 12) {
                log.log(Level.WARNING, "state file %s is too short to be valid; starting fresh".formatted(stateFile));
                return PersistentState.INITIAL;
            }
            long term = buffer.getLong();
            int length = buffer.getInt();
            if (length < 0 || length > buffer.remaining()) {
                log.log(Level.WARNING,
                        "state file %s has an implausible vote length; keeping the term only"
                                .formatted(stateFile));
                return new PersistentState(term, null);
            }
            if (length == 0) {
                return new PersistentState(term, null);
            }
            byte[] votedFor = new byte[length];
            buffer.get(votedFor);
            return new PersistentState(term, NodeId.of(new String(votedFor, StandardCharsets.UTF_8)));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load state from " + stateFile, e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
