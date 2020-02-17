package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.Query;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlRuntimeError;
import com.rethinkdb.gen.proto.ResponseType;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Result<T> implements Iterator<T>, Iterable<T>, Closeable {
    /**
     * The fetch mode to use on partial sequences.
     */
    public enum FetchMode {
        /**
         * Fetches all parts of the sequence as fast as possible.<br>
         * <b>WARNING:</b> This can end up throwing {@link OutOfMemoryError}s in case of giant sequences.
         */
        AGGRESSIVE,
        /**
         * Fetches the next part of the sequence once the buffer reaches half of the original size.
         */
        PREEMPTIVE_HALF,
        /**
         * Fetches the next part of the sequence once the buffer reaches a third of the original size.
         */
        PREEMPTIVE_THIRD,
        /**
         * Fetches the next part of the sequence once the buffer reaches a fourth of the original size.
         */
        PREEMPTIVE_FOURTH,
        /**
         * Fetches the next part of the sequence once the buffer reaches a fifth of the original size.
         */
        PREEMPTIVE_FIFTH,
        /**
         * Fetches the next part of the sequence once the buffer reaches a sixth of the original size.
         */
        PREEMPTIVE_SIXTH,
        /**
         * Fetches the next part of the sequence once the buffer reaches a seventh of the original size.
         */
        PREEMPTIVE_SEVENTH,
        /**
         * Fetches the next part of the sequence once the buffer reaches an eight of the original size.
         */
        PREEMPTIVE_EIGHTH,
        /**
         * Fetches the next part of the sequence once the buffer becomes empty.
         */
        LAZY
    }

    protected final Connection connection;
    protected final Query query;
    protected final Response firstRes;
    protected final FetchMode fetchMode;
    protected final TypeReference<T> typeRef;
    protected final Converter.FormatOptions fmt;

    protected final BlockingQueue<Object> rawQueue = new LinkedBlockingQueue<>();
    // completes with false if cancelled, otherwise with true. exceptionally completes if error.
    protected final CompletableFuture<Boolean> completed = new CompletableFuture<>();

    // This gets used if it's a partial request.
    protected final Semaphore requesting = new Semaphore(1);
    protected final Semaphore emitting = new Semaphore(1);
    protected final AtomicLong lastRequestCount = new AtomicLong();
    protected final AtomicReference<Response> currentResponse = new AtomicReference<>();

    public Result(Connection connection,
                  Query query,
                  Response firstRes,
                  FetchMode fetchMode,
                  TypeReference<T> typeRef) {
        this.connection = connection;
        this.query = query;
        this.firstRes = firstRes;
        this.fetchMode = fetchMode;
        this.typeRef = typeRef;
        fmt = new Converter.FormatOptions(query.globalOptions);
        currentResponse.set(firstRes);

        //todo change later
        CompletableFuture.runAsync(this::handleFirstResponse);
    }

    public long connectionToken() {
        return query.token;
    }

    public int bufferedCount() {
        return rawQueue.size();
    }

    public boolean isFeed() {
        return firstRes.isFeed();
    }

    @Override
    public void close() {
        completed.complete(false);
    }

    public List<T> toList() {
        return collect(Collectors.toList());
    }

    public <R, A> R collect(Collector<? super T, A, R> collector) {
        A container = collector.supplier().get();
        BiConsumer<A, ? super T> accumulator = collector.accumulator();
        forEachRemaining(next -> accumulator.accept(container, next));
        this.close();
        return collector.finisher().apply(container);
    }

    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false).onClose(this::close);
    }

    public Stream<T> parallelStream() {
        return StreamSupport.stream(spliterator(), true).onClose(this::close);
    }

    @Override
    public boolean hasNext() {
        return !rawQueue.isEmpty() || !completed.isDone();
    }

    public T next(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            Object next = rawQueue.poll(timeout, unit);
            onStateUpdate();
            return Util.convertToPojo(next, typeRef);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        try {
            Object next = rawQueue.take();
            onStateUpdate();
            return Util.convertToPojo(next, typeRef);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return this;
    }

    protected void handleFirstResponse() {
        if (firstRes.isWaitComplete()) {
            completed.complete(true);
            return;
        }

        if (firstRes.isAtom() || firstRes.isSequence()) {
            try {
                emitData(firstRes);
            } catch (IndexOutOfBoundsException ex) {
                throw new ReqlDriverError("Atom response was empty!", ex);
            }
            completed.complete(true);
            return;
        }

        if (firstRes.isPartial()) {
            // Welcome to the code documentation of partial sequences, please take a seat.

            // First of all, we emit all of this request. Reactor's buffer should handle this.
            emitData(firstRes);

            // It is a partial response, so connection should be able to kill us if needed,
            // and clients should be able to stop the Result.
            completed.thenAccept(finished -> {
                if (!finished) {
                    connection.sendStop(firstRes.token);
                }
                connection.loseTrackOf(this);
            });
            connection.keepTrackOf(this);

            // We can't simply overflow buffers, so we gotta do small batches.
            onStateUpdate();
            return;
        }

        completed.completeExceptionally(firstRes.makeError(query));
    }

    /**
     * This function is called on next()
     */
    protected void onStateUpdate() {
        final Response lastRes = currentResponse.get();
        if (lastRes.isPartial() && shouldContinue() && requesting.tryAcquire()) {
            // great, we should make a CONTINUE request.
            connection.sendContinue(lastRes.token).whenComplete((nextRes, t) -> {
                if (t != null) { // It errored. This means it's over.
                    completed.completeExceptionally(t);
                } else { // Okay, let's process this response.
                    currentResponse.set(nextRes);
                    if (nextRes.isSequence()) {
                        try {
                            emitting.acquire();
                            emitData(nextRes);
                            emitting.release();
                            completed.complete(true); // Completed. This means it's over.
                        } catch (InterruptedException e) {
                            completed.completeExceptionally(e); // It errored. This means it's over.
                        }
                    } else if (nextRes.isPartial()) {
                        // Okay, we got another partial response, so there's more.

                        requesting.release(); // Request's over, release this for later.
                        try {
                            emitting.acquire();
                            emitData(nextRes);
                            emitting.release();
                            onStateUpdate(); //Recursion!
                        } catch (InterruptedException e) {
                            completed.completeExceptionally(e); // It errored. This means it's over.
                        }
                    } else {
                        completed.completeExceptionally(firstRes.makeError(query)); // It errored. This means it's over.
                    }
                }
            });
        }
    }

    protected boolean shouldContinue() {
        if (!firstRes.isPartial()) {
            return false;
        }
        switch (fetchMode) {
            case PREEMPTIVE_HALF: {
                return rawQueue.size() * 2 < lastRequestCount.get();
            }
            case PREEMPTIVE_THIRD: {
                return rawQueue.size() * 3 < lastRequestCount.get();
            }
            case PREEMPTIVE_FOURTH: {
                return rawQueue.size() * 4 < lastRequestCount.get();
            }
            case PREEMPTIVE_FIFTH: {
                return rawQueue.size() * 5 < lastRequestCount.get();
            }
            case PREEMPTIVE_SIXTH: {
                return rawQueue.size() * 6 < lastRequestCount.get();
            }
            case PREEMPTIVE_SEVENTH: {
                return rawQueue.size() * 7 < lastRequestCount.get();
            }
            case PREEMPTIVE_EIGHTH: {
                return rawQueue.size() * 8 < lastRequestCount.get();
            }
            case LAZY: {
                return rawQueue.isEmpty();
            }
            case AGGRESSIVE:
            default: {
                return true;
            }
        }
    }

    protected void onConnectionClosed() throws InterruptedException {
        currentResponse.set(Response.make(query.token, ResponseType.SUCCESS_SEQUENCE).build());
        completed.completeExceptionally(new ReqlRuntimeError("Connection is closed."));
    }

    @SuppressWarnings("unchecked")
    protected void emitData(Response res) {
        if (completed.isDone()) {
            if (completed.join()) {
                throw new RuntimeException("The Response already completed successfully.");
            } else {
                throw new RuntimeException("The Response was cancelled.");
            }
        }
        lastRequestCount.set(0);
        List<Object> objects = (List<Object>) Converter.convertPseudotypes(res.data, fmt);
        for (Object each : objects) {
            if (connection.unwrapLists && firstRes.isAtom() && each instanceof List) {
                for (Object o : ((List<Object>) each)) {
                    rawQueue.offer(o);
                    lastRequestCount.incrementAndGet();
                }
            } else {
                rawQueue.offer(each);
                lastRequestCount.incrementAndGet();
            }
        }
    }
}
