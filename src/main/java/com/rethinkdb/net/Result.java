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
import java.util.concurrent.atomic.AtomicBoolean;
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
     * The object which represents the completed signal inside the BlockingQueue.
     * Used only by partial sequences to unlock threads. Multiple ENDs might be emitted.
     */
    private static final Object END = new Object();

    protected final Connection connection;
    protected final Query query;
    protected final Response sourceResponse;
    protected final TypeReference<T> typeRef;
    protected final Internals.FormatOptions fmt;
    protected final boolean unwrapLists;
    // can be altered depending on the operation
    protected FetchMode fetchMode;

    protected final BlockingQueue<Object> rawQueue = new LinkedBlockingQueue<>();
    // completes with false if cancelled, otherwise with true. exceptionally completes if error.
    protected final CompletableFuture<Boolean> completed = new CompletableFuture<>();

    // Used by Partial Responses.
    private final AtomicReference<PartialSequence> currentPartial = new AtomicReference<>();

    public Result(Connection connection,
                  Query query,
                  Response sourceResponse,
                  FetchMode fetchMode,
                  boolean unwrapLists,
                  TypeReference<T> typeRef) {
        this.connection = connection;
        this.query = query;
        this.sourceResponse = sourceResponse;
        this.fetchMode = fetchMode;
        this.typeRef = typeRef;
        this.fmt = Internals.parseFormatOptions(query.globalOptions);
        this.unwrapLists = unwrapLists;
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
        return sourceResponse.isFeed();
    }

    /**
     * Closes this Result.
     */
    @Override
    public void close() {
        completed.complete(false);
    }

    /**
     * Collect all remaining results to a list, fetching from the server if necessary, and closes the Result.<br><br>
     * <b>WARNING: If {@link Result#isFeed()} is true, this may never return. This method changes the {@code fetchMode}
     * of this Result to {@link FetchMode#AGGRESSIVE} to complete this as fast as possible.</b>
     *
     * @return the list
     */
    public @NotNull List<T> toList() {
        return collect(Collectors.toList());
    }

    /**
     * Collect all remaining results using the provided {@link Collector}, fetching from the server if necessary, and
     * closes the Result.<br><br>
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
            overrideFetchMode(FetchMode.AGGRESSIVE);
            A container = collector.supplier().get();
            BiConsumer<A, ? super T> accumulator = collector.accumulator();
            forEach(next -> accumulator.accept(container, next));
            return collector.finisher().apply(container);
        } finally {
            close();
        }
    }

    /**
     * Creates a new sequential {@code Stream} from the results, which closes this Result on completion.
     * <br><br>
     * <b>WARNING: If {@link Result#isFeed()} is true, this stream is possibly infinite. This method changes the
     * {@code fetchMode} of this Result to {@link FetchMode#AGGRESSIVE} to complete this as fast as possible.</b>
     *
     * @return the newly created stream.
     */
    public @NotNull Stream<T> stream() {
        return StreamSupport.stream(overrideFetchMode(FetchMode.AGGRESSIVE).spliterator(), false).onClose(this::close);
    }

    /**
     * Creates a new parallel {@code Stream} from the results, which closes this Result on completion.
     * <br><br>
     * <b>WARNING: If {@link Result#isFeed()} is true, this stream is possibly infinite. This method changes the
     * {@code fetchMode} of this Result to {@link FetchMode#AGGRESSIVE} to complete this as fast as possible.</b>
     *
     * @return the newly created stream.
     */
    public @NotNull Stream<T> parallelStream() {
        return StreamSupport.stream(overrideFetchMode(FetchMode.AGGRESSIVE).spliterator(), true).onClose(this::close);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return !rawQueue.isEmpty() && rawQueue.peek() != END || !completed.isDone();
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
            maybeContinue();
            Object next = rawQueue.poll(timeout, unit);
            maybeContinue();
            if (next == null) {
                throw new TimeoutException("The poll operation timed out.");
            }
            if (next == END) {
                rawQueue.offer(END);
                throwOnCompleted();
                throw new ReqlDriverError("END reached, but wasn't completed. Please contact devs!");
            }
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
            maybeContinue();
            Object next = rawQueue.take(); // This method shouldn't block forever.
            maybeContinue();
            if (next == END) {
                rawQueue.offer(END);
                throwOnCompleted();
                throw new ReqlDriverError("END reached, but wasn't completed. Please contact devs!");
            }
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
        // This method should never call "maybeNextBatch".
        try {
            if (!hasNext()) {
                throwOnCompleted();
            }
            Object next = rawQueue.take();
            if (next == END) {
                rawQueue.offer(END);
                throwOnCompleted();
                throw new ReqlDriverError("END reached, but wasn't completed. Please contact devs!");
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
     * Returns the first object and closes the Result, discarding all other results.
     * Throws if there's this result has no elements or more than one element.
     *
     * @return the first result available.
     */
    public @Nullable T single() {
        // This method should never call "maybeNextBatch".
        try {
            if (!hasNext()) {
                throwOnCompleted();
            }
            Object next = rawQueue.take();
            if (hasNext()) {
                throw new IllegalStateException("More than one result.");
            }
            if (next == END) {
                rawQueue.offer(END);
                throwOnCompleted();
                throw new ReqlDriverError("END reached, but wasn't completed. Please contact devs!");
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
            overrideFetchMode(FetchMode.AGGRESSIVE);
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
        return currentResponse().profile;
    }

    /**
     * Returns the current responses' type.
     *
     * @return the {@link ResponseType} of the current response.
     */
    public @NotNull ResponseType responseType() {
        return currentResponse().type;
    }

    /**
     * Overrides the fetch mode for this Result.
     *
     * @param fetchMode the new fetch mode.
     * @return itself.
     */
    public @NotNull Result<T> overrideFetchMode(FetchMode fetchMode) {
        this.fetchMode = fetchMode;
        maybeContinue();
        return this;
    }

    @Override
    public String toString() {
        return "Result{" +
            "connection=" + connection +
            ", query=" + query +
            ", firstRes=" + sourceResponse +
            ", completed=" + completed +
            ", currentResponse=" + currentResponse() +
            '}';
    }

    // protected methods

    protected Response currentResponse() {
        PartialSequence batch = currentPartial.get();
        if (batch != null) {
            return batch.response;
        }
        return sourceResponse;
    }

    /**
     * Function called on the first response.
     */
    protected void handleFirstResponse() {
        try {
            ResponseType type = sourceResponse.type;
            if (type.equals(ResponseType.WAIT_COMPLETE)) {
                completed.complete(true);
                return;
            }

            if (type.equals(ResponseType.SUCCESS_ATOM) || type.equals(ResponseType.SUCCESS_SEQUENCE)) {
                try {
                    emitData(sourceResponse);
                } catch (IndexOutOfBoundsException ex) {
                    throw new ReqlDriverError("Atom response was empty!", ex);
                }
                completed.complete(true);
                return;
            }

            if (type.equals(ResponseType.SUCCESS_PARTIAL)) {
                currentPartial.set(new PartialSequence(sourceResponse));

                // It is a partial response, so connection should be able to kill us if needed,
                // and clients should be able to stop the Result.
                completed.thenAccept(finished -> {
                    if (!finished) {
                        connection.sendStop(sourceResponse.token);
                    }
                    connection.loseTrackOf(Result.this);
                });
                connection.keepTrackOf(Result.this);

                // We can't simply overflow buffers, so we gotta do small batches.
                maybeContinue();
                return;
            }

            throw sourceResponse.makeError(query);
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

    protected void maybeContinue() {
        PartialSequence partial = currentPartial.get();
        if (partial != null) {
            partial.maybeContinue();
        }
    }

    protected void onConnectionClosed() {
        currentPartial.set(new PartialSequence(new Response(query.token, ResponseType.SUCCESS_SEQUENCE)));
        completed.completeExceptionally(new ReqlRuntimeError("Connection is closed."));
    }

    protected int emitData(Response res) {
        throwOnCompleted();
        int count = 0;
        for (Object each : (List<?>) Internals.convertPseudotypes(res.data, fmt)) {
            if (unwrapLists && res.type.equals(ResponseType.SUCCESS_ATOM) && each instanceof List) {
                for (Object o : ((List<?>) each)) {
                    rawQueue.offer(o == null ? NIL : o);
                    count++;
                }
            } else {
                rawQueue.offer(each == null ? NIL : each);
                count++;
            }
        }
        return count;
    }

    private class PartialSequence {
        protected final Response response;
        protected final int emitted;
        protected final boolean last;
        protected final AtomicBoolean continued = new AtomicBoolean();

        private PartialSequence(Response response) {
            this.response = response;
            if (response.type.equals(ResponseType.SUCCESS_PARTIAL)) {
                this.last = false;
                // Okay, we got another partial response, so there's more.
                this.emitted = tryEmitData();
                maybeContinue(); //Recursion!
            } else if (response.type.equals(ResponseType.SUCCESS_SEQUENCE)) {
                this.last = true;
                // Last response.
                this.emitted = tryEmitData();
                completed.complete(true);
                rawQueue.offer(END);
            } else {
                this.last = true;
                completed.completeExceptionally(response.makeError(query)); // It errored. This means it's over.
                rawQueue.offer(END);
                this.emitted = 0;
            }
        }

        protected void maybeContinue() {
            final int remaining = rawQueue.size();

            if (!completed.isDone() && !last && fetchMode.shouldContinue(remaining, emitted)) {
                continueResponse();
            }
        }

        private int tryEmitData() {
            try {
                return emitData(response);
            } catch (Exception e) {
                completed.completeExceptionally(e); // It errored. This means it's over.
                rawQueue.offer(END);
            }
            return 0;
        }

        private void continueResponse() {
            if (!continued.getAndSet(true)) {
                connection.sendContinue(response.token).whenComplete((continued, t) -> {
                    if (t == null) { // Okay, let's process this response.
                        currentPartial.set(new PartialSequence(continued));
                    } else { // It errored. This means it's over.
                        completed.completeExceptionally(t);
                        rawQueue.offer(END);
                    }
                });
            }
        }
    }

    /**
     * The fetch mode to use on partial sequences.
     */
    public enum FetchMode {
        /**
         * Fetches all parts of the sequence as fast as possible.<br>
         * <b>WARNING:</b> This can end up throwing {@link OutOfMemoryError}s in case of giant sequences.
         */
        AGGRESSIVE {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return true;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer reaches half of the original size.
         */
        PREEMPTIVE_HALF {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size <= requestSize / 2;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer reaches a third of the original size.
         */
        PREEMPTIVE_THIRD {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size <= requestSize / 3;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer reaches a fourth of the original size.
         */
        PREEMPTIVE_FOURTH {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size <= requestSize / 4;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer reaches a fifth of the original size.
         */
        PREEMPTIVE_FIFTH {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size <= requestSize / 5;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer reaches a sixth of the original size.
         */
        PREEMPTIVE_SIXTH {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size <= requestSize / 6;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer reaches a seventh of the original size.
         */
        PREEMPTIVE_SEVENTH {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size <= requestSize / 7;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer reaches an eight of the original size.
         */
        PREEMPTIVE_EIGHTH {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size <= requestSize / 8;
            }
        },
        /**
         * Fetches the next part of the sequence once the buffer becomes empty.
         */
        LAZY {
            @Override
            public boolean shouldContinue(int size, int requestSize) {
                return size == 0;
            }
        };

        public abstract boolean shouldContinue(int size, int requestSize);

        @Nullable
        public static FetchMode fromString(String s) {
            try {
                return valueOf(s.toUpperCase());
            } catch (RuntimeException ignored) {
                return null;
            }
        }
    }
}
