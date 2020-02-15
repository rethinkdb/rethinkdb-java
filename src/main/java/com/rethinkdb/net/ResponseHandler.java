package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.ast.Query;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlRuntimeError;
import com.rethinkdb.gen.proto.ResponseType;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class ResponseHandler<T> implements Consumer<FluxSink<T>> {
    private final Connection connection;
    private final Query query;
    private final Response firstRes;
    private final TypeReference<T> typeRef;
    private final Converter.FormatOptions fmt;

    // This gets used if it's a partial request.
    private final Semaphore requesting = new Semaphore(1);
    private final Semaphore emitting = new Semaphore(1);
    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicReference<Response> currentResponse = new AtomicReference<>();
    private final AtomicReference<FluxSink<T>> sink = new AtomicReference<>();

    public ResponseHandler(Connection connection, Query query, Response firstRes, TypeReference<T> typeRef) {
        this.connection = connection;
        this.query = query;
        this.firstRes = firstRes;
        this.typeRef = typeRef;
        fmt = new Converter.FormatOptions(query.globalOptions);
        currentResponse.set(firstRes);
    }

    @Override
    public void accept(final FluxSink<T> sink) {
        if (firstRes.isWaitComplete()) {
            sink.complete();
            return;
        }

        if (firstRes.isAtom() || firstRes.isSequence()) {
            try {
                emitData(sink);
            } catch (IndexOutOfBoundsException ex) {
                throw new ReqlDriverError("Atom response was empty!", ex);
            }
            sink.complete();
            return;
        }

        if (firstRes.isPartial()) {
            // Welcome to the code documentation of partial sequences, please take a seat.

            // First of all, we emit all of this request. Reactor's buffer should handle this.
            emitData(sink);

            // It is a partial response, so connection should be able to kill us if needed, and clients should be able to stop us.
            this.sink.set(sink);
            sink.onCancel(() -> {
                connection.loseTrackOf(this);
                connection.stop(firstRes.token);
            });
            sink.onDispose(() -> connection.loseTrackOf(this));
            connection.keepTrackOf(this);

            // We can't simply overflow buffers, so we gotta do small batches.
            sink.onRequest(amount -> onRequest(sink, amount));
            return;
        }

        sink.error(firstRes.makeError(query));
    }

    private void onRequest(FluxSink<T> sink, long amount) {
        final Response lastRes = currentResponse.get();
        if (lastRes.isPartial() && requestCount.addAndGet(amount) > 0 && requesting.tryAcquire()) {
            // great, we should make a CONTINUE request.

            // TODO isolate this into methods
            Mono.fromFuture(connection.continueResponse(lastRes.token)).subscribe(
                nextRes -> { // Okay, let's process this response.
                    boolean shouldContinue = currentResponse.compareAndSet(lastRes, nextRes);
                    if (nextRes.isSequence()) {
                        try {
                            emitting.acquire();
                            emitData(sink);
                            emitting.release();
                            sink.complete(); // Completed. This means it's over.
                        } catch (InterruptedException e) {
                            sink.error(e); // It errored. This means it's over.
                        }
                    } else if (nextRes.isPartial()) {
                        // Okay, we got another partial response, so there's more.

                        requesting.release(); // Request's over, release this for later.
                        try {
                            emitting.acquire();
                            int count = emitData(sink);
                            emitting.release();
                            if (shouldContinue) {
                                onRequest(sink, -count); //Recursion!
                            }
                        } catch (InterruptedException e) {
                            sink.error(e); // It errored. This means it's over.
                        }
                    } else {
                        sink.error(nextRes.makeError(query)); // It errored. This means it's over.
                    }
                }, sink::error // It errored. This means it's over.
            );
        }
    }

    void onConnectionClosed() throws InterruptedException {
        // This will spin wait for a bit until it is not null
        while (sink.compareAndSet(null, null)) Thread.yield();
        FluxSink<T> sink = this.sink.get();
        currentResponse.set(Response.make(query.token, ResponseType.SUCCESS_SEQUENCE).build());
        try {
            emitting.acquire();
        } finally {
            sink.error(new ReqlRuntimeError("Connection is closed."));
        }
    }

    @SuppressWarnings("unchecked")
    private int emitData(final FluxSink<T> sink) {
        List<Object> objects = (List<Object>) Converter.convertPseudotypes(firstRes.data, fmt);
        for (Object each : objects) {
            if (firstRes.isAtom() && each instanceof List) {
                for (Object o : ((List<Object>) each)) {
                    sink.next(Util.convertToPojo(o, typeRef));
                }
            } else {
                sink.next(Util.convertToPojo(each, typeRef));
            }
        }
        return objects.size();
    }
}
