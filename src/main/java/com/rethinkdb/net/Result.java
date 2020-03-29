package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.ast.Query;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlRuntimeError;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.model.Profile;
import com.rethinkdb.utils.Internals;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A ReQL query result.
 *
 * @param <T> the type of the result.
 */
public class Result<T> implements Iterator<T>, Iterable<T>, Closeable {
    /**
     * The object which represents {@code null} inside the BlockingQueue.
     */
    private static final Object NIL = new Object();

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
        LAZY;

        @Nullable
        public static FetchMode fromString(String s) {
            try {
                return valueOf(s.toUpperCase());
            } catch (RuntimeException ignored) {
                return null;
            }
        }
    }

    protected final Connection connection;
    protected final Query query;
    protected final Response firstRes;
    protected final TypeReference<T> typeRef;
    protected final Internals.FormatOptions fmt;
    // can be altered depending on the operation
    protected FetchMode fetchMode;

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
        fmt = new Internals.FormatOptions(query.globalOptions);
        currentResponse.set(firstRes);
        handleFirstResponse();
    }

    /**
     * Gets the connection token of this result.
     *
     * @return the connection token.
     */
    public long connectionToken() {
        return query.token;
    }

    /**
     * Gets the number of objects buffered and readily available.
     *
     * @return the buffered objects queue size.
     */
    public int bufferedCount() {
        return rawQueue.size();
    }

    /**
     * Gets if this Result is a feed. Feeds may never end at all.
     *
     * @return true if this Result is a feed.
     */
    public boolean isFeed() {
        return firstRes.isFeed();
    }

    /**
     * Closes this Result.
     */
    @Override
    public void close() {
        completed.complete(false);
    }

    /**
     * Collect all the results, fetching from the server if necessary, to a list and closes the Result.<br><br>
     * <b>WARNING: If {@link Result#isFeed()} is true, this may never return. This method changes the {@code fetchMode}
     * of this Result to {@link FetchMode#AGGRESSIVE} to complete this as fast as possible.</b>
     *
     * @return the list
     */
    public @NotNull List<T> toList() {
        return collect(Collectors.toList());
    }

    /**
     * Collect all the results, fetching from the server if necessary, using a {@link Collector} and closes the Result.
     * <br><br>
     * <b>WARNING: If {@link Result#isFeed()} is true, this may never return. This method changes the {@code fetchMode}
     * of this Result to {@link FetchMode#AGGRESSIVE} to complete this as fast as possible.</b>
     *
     * @param collector the collector
     * @param <A>       the mutable accumulation type of the reduction operation (often hidden as an implementation detail)
     * @param <R>       the result type of the reduction operation
     * @return the final result
     */
    public <R, A> R collect(@NotNull Collector<? super T, A, R> collector) {
        try {
            fetchMode = FetchMode.AGGRESSIVE;
            onStateUpdate();
            A container = collector.supplier().get();
            BiConsumer<A, ? super T> accumulator = collector.accumulator();
            forEachRemaining(next -> accumulator.accept(container, next));
            return collector.finisher().apply(container);
        } finally {
            close();
        }
    }

    /**
     * Creates a new sequential {@code Stream} from the results, which closes this Result on completion.
     * <br><br>
     * <b>WARNING: If {@link Result#isFeed()} is true, this may never return. This method changes the {@code fetchMode}
     * of this Result to {@link FetchMode#AGGRESSIVE} to complete this as fast as possible.</b>
     *
     * @return the newly created stream.
     */
    public @NotNull Stream<T> stream() {
        fetchMode = FetchMode.AGGRESSIVE;
        onStateUpdate();
        return StreamSupport.stream(spliterator(), false).onClose(this::close);
    }

    /**
     * Creates a new parallel {@code Stream} from the results, which closes this Result on completion.
     * <br><br>
     * <b>WARNING: If {@link Result#isFeed()} is true, this may never return. This method changes the {@code fetchMode}
     * of this Result to {@link FetchMode#AGGRESSIVE} to complete this as fast as possible.</b>
     *
     * @return the newly created stream.
     */
    public @NotNull Stream<T> parallelStream() {
        fetchMode = FetchMode.AGGRESSIVE;
        onStateUpdate();
        return StreamSupport.stream(spliterator(), true).onClose(this::close);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return !rawQueue.isEmpty() || !completed.isDone();
    }

    /**
     * Returns the next element in the iteration, with a defined timeout.
     *
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit    a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     * @throws TimeoutException       if the poll operation times out
     */
    public @Nullable T next(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            if (!hasNext()) {
                throwOnCompleted();
            }
            Object next = rawQueue.poll(timeout, unit);
            if (next == null) {
                throw new TimeoutException("The poll operation timed out.");
            }
            onStateUpdate();
            if (next == NIL) {
                return null;
            }
            return Internals.toPojo(next, typeRef);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @Nullable T next() {
        try {
            if (!hasNext()) {
                throwOnCompleted();
            }
            Object next = rawQueue.take();
            onStateUpdate();
            if (next == NIL) {
                return null;
            }
            return Internals.toPojo(next, typeRef);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the first object and closes the Result, discarding all other results.
     * Throws if there's this result has no elements.
     *
     * @return the first result available.
     */
    public @Nullable T first() {
        try {
            if (!hasNext()) {
                throwOnCompleted();
            }
            Object next = rawQueue.take();
            if (next == NIL) {
                return null;
            }
            return Internals.toPojo(next, typeRef);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            rawQueue.clear();
            close();
        }
    }

    /**
     * Returns the first object and closes the Result, discarding all other results.
     * Throws if there's this result has no elements or more than one element.
     *
     * @return the first result available.
     */
    public @Nullable T single() {
        try {
            if (!hasNext()) {
                throwOnCompleted();
            }
            Object next = rawQueue.take();
            if (hasNext()) {
                throw new IllegalStateException("More than one result.");
            }
            if (next == NIL) {
                return null;
            }
            return Internals.toPojo(next, typeRef);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            rawQueue.clear();
            close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @NotNull Iterator<T> iterator() {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forEach(Consumer<? super T> action) {
        try {
            Objects.requireNonNull(action);
            fetchMode = FetchMode.AGGRESSIVE;
            while (hasNext()) {
                action.accept(next());
            }
        } finally {
            close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        forEach(action);
    }

    /**
     * Gets the current's response Profile, if any.
     *
     * @return the Profile from the current response, or null
     */
    public @Nullable Profile profile() {
        return currentResponse.get().profile;
    }

    /**
     * Returns the current responses' type.
     *
     * @return the {@link ResponseType} of the current response.
     */
    public @NotNull ResponseType responseType() {
        return currentResponse.get().type;
    }

    /**
     * Overrides the fetch mode for this Result.
     *
     * @param fetchMode the new fetch mode.
     * @return itself.
     */
    public @NotNull Result<T> overrideFetchMode(FetchMode fetchMode) {
        this.fetchMode = fetchMode;
        onStateUpdate();
        return this;
    }

    @Override
    public String toString() {
        return "Result{" +
            "connection=" + connection +
            ", query=" + query +
            ", firstRes=" + firstRes +
            ", completed=" + completed +
            ", currentResponse=" + currentResponse +
            '}';
    }

    // protected methods

    /**
     * Function called on the first response.
     */
    protected void handleFirstResponse() {
        try {
            ResponseType type = firstRes.type;
            if (type.equals(ResponseType.WAIT_COMPLETE)) {
                completed.complete(true);
                return;
            }

            if (type.equals(ResponseType.SUCCESS_ATOM) || type.equals(ResponseType.SUCCESS_SEQUENCE)) {
                try {
                    emitData(firstRes);
                } catch (IndexOutOfBoundsException ex) {
                    throw new ReqlDriverError("Atom response was empty!", ex);
                }
                completed.complete(true);
                return;
            }

            if (type.equals(ResponseType.SUCCESS_PARTIAL)) {
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

            throw firstRes.makeError(query);
        } catch (Exception e) {
            completed.completeExceptionally(e);
            throw e;
        }
    }

    protected void throwOnCompleted() {
        if (completed.isDone()) {
            try {
                if (completed.join()) {
                    throw new NoSuchElementException("No more elements.");
                } else {
                    throw new NoSuchElementException("Result was cancelled.");
                }
            } catch (CompletionException e) {
                if (e.getCause() instanceof ReqlError) {
                    throw ((ReqlError) e.getCause());
                }
                throw e;
            }
        }
    }

    /**
     * This function is called on next()
     */
    protected void onStateUpdate() {
        final Response lastRes = currentResponse.get();
        if (shouldContinue(lastRes) && requesting.tryAcquire()) {
            // great, we should make a CONTINUE request.
            connection.sendContinue(lastRes.token).whenComplete((nextRes, t) -> {
                if (t != null) { // It errored. This means it's over.
                    completed.completeExceptionally(t);
                } else { // Okay, let's process this response.
                    currentResponse.set(nextRes);
                    if (nextRes.type.equals(ResponseType.SUCCESS_SEQUENCE)) {
                        try {
                            emitting.acquire();
                            emitData(nextRes);
                            emitting.release();
                            completed.complete(true); // Completed. This means it's over.
                        } catch (Exception e) {
                            completed.completeExceptionally(e); // It errored. This means it's over.
                        }
                    } else if (nextRes.type.equals(ResponseType.SUCCESS_PARTIAL)) {
                        // Okay, we got another partial response, so there's more.

                        requesting.release(); // Request's over, release this for later.
                        try {
                            emitting.acquire();
                            emitData(nextRes);
                            emitting.release();
                            onStateUpdate(); //Recursion!
                        } catch (Exception e) {
                            completed.completeExceptionally(e); // It errored. This means it's over.
                        }
                    } else {
                        completed.completeExceptionally(firstRes.makeError(query)); // It errored. This means it's over.
                    }
                }
            });
        }
    }

    protected boolean shouldContinue(Response res) {
        if (completed.isDone() || !res.type.equals(ResponseType.SUCCESS_PARTIAL)) {
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

    protected void onConnectionClosed() {
        currentResponse.set(new Response(query.token, ResponseType.SUCCESS_SEQUENCE));
        completed.completeExceptionally(new ReqlRuntimeError("Connection is closed."));
    }

    protected void emitData(Response res) {
        if (completed.isDone()) {
            if (completed.join()) {
                throw new RuntimeException("The Response already completed successfully.");
            } else {
                throw new RuntimeException("The Response was cancelled.");
            }
        }
        lastRequestCount.set(0);
        for (Object each : (List<?>) Internals.convertPseudotypes(res.data, fmt)) {
            if (connection.unwrapLists && firstRes.type.equals(ResponseType.SUCCESS_ATOM) && each instanceof List) {
                for (Object o : ((List<?>) each)) {
                    rawQueue.offer(o == null ? NIL : o);
                    lastRequestCount.incrementAndGet();
                }
            } else {
                rawQueue.offer(each == null ? NIL : each);
                lastRequestCount.incrementAndGet();
            }
        }
    }
}
