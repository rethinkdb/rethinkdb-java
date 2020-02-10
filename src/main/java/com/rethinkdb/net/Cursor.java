package com.rethinkdb.net;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface Cursor<T> extends Iterator<T>, Iterable<T>, Closeable {
    long connectionToken();

    Converter.FormatOptions formatOptions();

    @Override
    void close();

    boolean isFeed();

    int bufferedSize();

    T next(long timeout) throws TimeoutException;

    default List<T> toList() {
        return collect(Collectors.toList());
    }

    default <R, A> R collect(Collector<? super T, A, R> collector) {
        A container = collector.supplier().get();
        BiConsumer<A, ? super T> accumulator = collector.accumulator();
        forEachRemaining(next -> accumulator.accept(container, next));
        return collector.finisher().apply(container);
    }

    default Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    default Stream<T> parallelStream() {
        return StreamSupport.stream(spliterator(), true);
    }
}
