package io.github.ankitnehra6.raftkv.server;

import io.github.ankitnehra6.raftkv.grpc.DeleteRequest;
import io.github.ankitnehra6.raftkv.grpc.GetReply;
import io.github.ankitnehra6.raftkv.grpc.GetRequest;
import io.github.ankitnehra6.raftkv.grpc.KeyValueGrpc;
import io.github.ankitnehra6.raftkv.grpc.PutRequest;
import io.github.ankitnehra6.raftkv.grpc.StatusReply;
import io.github.ankitnehra6.raftkv.grpc.StatusRequest;
import io.github.ankitnehra6.raftkv.grpc.WriteReply;
import io.github.ankitnehra6.raftkv.statemachine.Command;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.CompletionException;

/**
 * The client-facing service.
 *
 * <p>Every operation, reads included, goes through the log. A read served from the local
 * state machine would be faster and wrong: a leader deposed without knowing it yet would
 * answer from a stale map, and the client would see a value that had already been
 * overwritten.
 *
 * <p>A request to a non-leader is answered with {@code ok=false} and a leader hint rather
 * than an error. Losing leadership is routine — it happens on every election — and a client
 * that has to parse an exception to learn where to go next is a client that will get it
 * wrong.
 */
class KeyValueService extends KeyValueGrpc.KeyValueImplBase {

    private final RaftServer server;

    KeyValueService(RaftServer server) {
        this.server = server;
    }

    @Override
    public void put(PutRequest request, StreamObserver<WriteReply> observer) {
        write(observer, Command.encode(new Command.Put(request.getKey(), request.getValue())));
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<WriteReply> observer) {
        write(observer, Command.encode(new Command.Delete(request.getKey())));
    }

    private void write(StreamObserver<WriteReply> observer, byte[] command) {
        try {
            server.propose(command).join();
            observer.onNext(WriteReply.newBuilder().setOk(true).build());
        } catch (CompletionException | NotLeaderException e) {
            observer.onNext(
                    WriteReply.newBuilder().setOk(false).setLeaderHint(hintFrom(e)).build());
        }
        observer.onCompleted();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetReply> observer) {
        try {
            Optional<String> value =
                    server.propose(Command.encode(new Command.Get(request.getKey()))).join();

            GetReply.Builder reply = GetReply.newBuilder().setOk(true).setFound(value.isPresent());
            value.ifPresent(reply::setValue);
            observer.onNext(reply.build());

        } catch (CompletionException | NotLeaderException e) {
            observer.onNext(GetReply.newBuilder().setOk(false).setLeaderHint(hintFrom(e)).build());
        }
        observer.onCompleted();
    }

    @Override
    public void status(StatusRequest request, StreamObserver<StatusReply> observer) {
        RaftServer.Status status = server.status();
        observer.onNext(
                StatusReply.newBuilder()
                        .setNodeId(status.nodeId())
                        .setRole(status.role().name())
                        .setTerm(status.term())
                        .setCommitIndex(status.commitIndex())
                        .setLeaderId(status.leaderId())
                        .addAllMembers(status.members())
                        .build());
        observer.onCompleted();
    }

    /** Digs the leader hint out of whatever wrapper the future threw. */
    private static String hintFrom(Throwable e) {
        Throwable cause = e instanceof CompletionException ? e.getCause() : e;
        while (cause != null) {
            if (cause instanceof NotLeaderException notLeader) {
                return notLeader.leaderHint();
            }
            cause = cause.getCause();
        }
        return "";
    }
}
