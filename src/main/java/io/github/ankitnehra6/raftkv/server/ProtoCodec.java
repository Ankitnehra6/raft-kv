package io.github.ankitnehra6.raftkv.server;

import com.google.protobuf.ByteString;
import io.github.ankitnehra6.raftkv.core.LogEntry;
import io.github.ankitnehra6.raftkv.core.Message;
import io.github.ankitnehra6.raftkv.core.NodeId;
import io.github.ankitnehra6.raftkv.core.Snapshot;
import io.github.ankitnehra6.raftkv.grpc.AppendEntriesReply;
import io.github.ankitnehra6.raftkv.grpc.AppendEntriesRequest;
import io.github.ankitnehra6.raftkv.grpc.InstallSnapshotReply;
import io.github.ankitnehra6.raftkv.grpc.InstallSnapshotRequest;
import io.github.ankitnehra6.raftkv.grpc.LogEntryProto;
import io.github.ankitnehra6.raftkv.grpc.RequestVoteReply;
import io.github.ankitnehra6.raftkv.grpc.RequestVoteRequest;
import java.util.List;

/**
 * Translates between the domain messages and the wire schema.
 *
 * <p>The two are kept separate on purpose. The wire format is a compatibility contract that
 * has to evolve independently of the algorithm; serialising the domain types directly would
 * make an ordinary refactor of the core a silent, breaking protocol change.
 *
 * <p>All conversion lives here rather than being spread through the server, so there is
 * exactly one place to look when a field is added.
 */
final class ProtoCodec {

    private ProtoCodec() {}

    // --- outgoing ------------------------------------------------------------------

    static RequestVoteRequest toProto(Message.RequestVote m) {
        return RequestVoteRequest.newBuilder()
                .setFrom(m.from().value())
                .setTerm(m.term())
                .setLastLogIndex(m.lastLogIndex())
                .setLastLogTerm(m.lastLogTerm())
                .build();
    }

    static AppendEntriesRequest toProto(Message.AppendEntries m) {
        AppendEntriesRequest.Builder builder =
                AppendEntriesRequest.newBuilder()
                        .setFrom(m.from().value())
                        .setTerm(m.term())
                        .setPrevLogIndex(m.prevLogIndex())
                        .setPrevLogTerm(m.prevLogTerm())
                        .setLeaderCommit(m.leaderCommit());

        for (LogEntry entry : m.entries()) {
            builder.addEntries(toProto(entry));
        }
        return builder.build();
    }

    static InstallSnapshotRequest toProto(Message.InstallSnapshot m) {
        Snapshot snapshot = m.snapshot();
        return InstallSnapshotRequest.newBuilder()
                .setFrom(m.from().value())
                .setTerm(m.term())
                .setLastIncludedIndex(snapshot.lastIncludedIndex())
                .setLastIncludedTerm(snapshot.lastIncludedTerm())
                .setData(ByteString.copyFrom(snapshot.data()))
                .build();
    }

    static LogEntryProto toProto(LogEntry entry) {
        return LogEntryProto.newBuilder()
                .setTerm(entry.term())
                .setIndex(entry.index())
                .setType(entry.type().ordinal())
                .setCommand(ByteString.copyFrom(entry.command()))
                .build();
    }

    static RequestVoteReply toProto(Message.RequestVoteResponse m) {
        return RequestVoteReply.newBuilder()
                .setFrom(m.from().value())
                .setTerm(m.term())
                .setVoteGranted(m.voteGranted())
                .build();
    }

    static AppendEntriesReply toProto(Message.AppendEntriesResponse m) {
        return AppendEntriesReply.newBuilder()
                .setFrom(m.from().value())
                .setTerm(m.term())
                .setSuccess(m.success())
                .setMatchIndex(m.matchIndex())
                .setConflictIndex(m.conflictIndex())
                .build();
    }

    static InstallSnapshotReply toProto(Message.InstallSnapshotResponse m) {
        return InstallSnapshotReply.newBuilder()
                .setFrom(m.from().value())
                .setTerm(m.term())
                .setMatchIndex(m.matchIndex())
                .build();
    }

    // --- incoming ------------------------------------------------------------------

    static Message.RequestVote fromProto(RequestVoteRequest request, NodeId to) {
        return new Message.RequestVote(
                NodeId.of(request.getFrom()),
                to,
                request.getTerm(),
                request.getLastLogIndex(),
                request.getLastLogTerm());
    }

    static Message.AppendEntries fromProto(AppendEntriesRequest request, NodeId to) {
        List<LogEntry> entries = request.getEntriesList().stream().map(ProtoCodec::fromProto).toList();

        return new Message.AppendEntries(
                NodeId.of(request.getFrom()),
                to,
                request.getTerm(),
                request.getPrevLogIndex(),
                request.getPrevLogTerm(),
                entries,
                request.getLeaderCommit());
    }

    static Message.InstallSnapshot fromProto(InstallSnapshotRequest request, NodeId to) {
        return new Message.InstallSnapshot(
                NodeId.of(request.getFrom()),
                to,
                request.getTerm(),
                new Snapshot(
                        request.getLastIncludedIndex(),
                        request.getLastIncludedTerm(),
                        request.getData().toByteArray()));
    }

    static LogEntry fromProto(LogEntryProto proto) {
        LogEntry.Type[] types = LogEntry.Type.values();
        int ordinal = proto.getType();
        if (ordinal < 0 || ordinal >= types.length) {
            // A peer running a newer version sent an entry this node cannot interpret.
            // Applying it as something else would diverge this replica from the rest, so
            // refusing is the only safe response.
            throw new IllegalArgumentException("unknown log entry type " + ordinal);
        }
        return new LogEntry(
                proto.getTerm(), proto.getIndex(), types[ordinal], proto.getCommand().toByteArray());
    }

    static Message.RequestVoteResponse fromProto(RequestVoteReply reply, NodeId to) {
        return new Message.RequestVoteResponse(
                NodeId.of(reply.getFrom()), to, reply.getTerm(), reply.getVoteGranted());
    }

    static Message.AppendEntriesResponse fromProto(AppendEntriesReply reply, NodeId to) {
        return new Message.AppendEntriesResponse(
                NodeId.of(reply.getFrom()),
                to,
                reply.getTerm(),
                reply.getSuccess(),
                reply.getMatchIndex(),
                reply.getConflictIndex());
    }

    static Message.InstallSnapshotResponse fromProto(InstallSnapshotReply reply, NodeId to) {
        return new Message.InstallSnapshotResponse(
                NodeId.of(reply.getFrom()), to, reply.getTerm(), reply.getMatchIndex());
    }
}
