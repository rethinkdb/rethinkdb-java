package com.rethinkdb.net;

import com.rethinkdb.ast.Query;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.exc.ReqlRuntimeError;
import com.rethinkdb.gen.proto.ResponseType;
import org.jetbrains.annotations.Nullable;

import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class CursorImpl<T> implements Cursor<T> {
    private final Connection connection;
    private final Query query;
    private final long token;
    private final boolean feed;
    @Nullable
    private final Class<T> pojoClass;
    private final Converter.FormatOptions fmt;
    private final Deque<Object> bufferedItems = new LinkedBlockingDeque<>();

    // mutable members
    private int outstandingRequests = 0;
    private int threshold = 1;
    @Nullable
    private RuntimeException error = null;
    private boolean alreadyIterated = false;
    private Future<Response> awaitingContinue = null;

    //constructor
    CursorImpl(Connection connection, Query query, Response firstResponse, @Nullable Class<T> pojoClass) {
        this.connection = connection;
        this.query = query;
        this.token = query.token;
        this.feed = firstResponse.isFeed();
        this.pojoClass = pojoClass;
        this.fmt = new Converter.FormatOptions(query.globalOptions);
        connection.addToCache(query.token, this);
        maybeSendContinue();
        extendInternal(firstResponse);
    }

    //region Cursor<T> implementation
    @Override
    public long connectionToken() {
        return token;
    }

    @Override
    public Converter.FormatOptions formatOptions() {
        return fmt;
    }

    @Override
    public boolean isFeed() {
        return feed;
    }

    @Override
    public int bufferedSize() {
        return bufferedItems.size();
    }

    @Override
    public T next(long timeout) throws TimeoutException {
        while (bufferedItems.size() == 0) {
            maybeSendContinue();
            waitOnCursorItems(timeout);

            if (bufferedItems.size() != 0) {
                break;
            }

            if (error != null) {
                throw error;
            }
        }

        return Util.convertToPojo(Converter.convertPseudotypes(bufferedItems.pop(), fmt), pojoClass);
    }

    @Override
    public void close() {
        connection.removeFromCache(this.token);
        if (error == null) {
            error = new NoSuchElementException();
            if (connection.isOpen()) {
                outstandingRequests += 1;
                connection.stop(this);
            }
        }
    }

    @Override
    public Iterator<T> iterator() {
        if (!alreadyIterated) {
            alreadyIterated = true;
            return this;
        }
        throw new ReqlDriverError("The results of this query have already been consumed.");
    }

    @Override
    public boolean hasNext() {
        if (bufferedItems.size() > 0) {
            return true;
        }
        if (error != null) {
            return false;
        }
        if (feed) {
            return true;
        }

        maybeSendContinue();
        waitOnCursorItems();

        return bufferedItems.size() > 0;
    }

    @Override
    public T next() {
        while (bufferedItems.size() == 0) {
            maybeSendContinue();
            waitOnCursorItems();

            if (bufferedItems.size() != 0) {
                break;
            }

            if (error != null) {
                throw error;
            }
        }

        return Util.convertToPojo(Converter.convertPseudotypes(bufferedItems.pop(), fmt), pojoClass);
    }

    //end

    //region internals
    private void maybeSendContinue() {
        if (error == null && bufferedItems.size() < threshold && outstandingRequests == 0) {
            outstandingRequests += 1;
            this.awaitingContinue = connection.continue_(this);
        }
    }

    private void waitOnCursorItems() {
        Response res;
        try {
            res = this.awaitingContinue.get();
        } catch (Exception e) {
            throw new ReqlDriverError(e);
        }
        extend(res);
    }

    private void waitOnCursorItems(long timeout) throws TimeoutException {
        Response res;
        try {
            res = this.awaitingContinue.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException exc) {
            throw exc;
        } catch (Exception e) {
            throw new ReqlDriverError(e);
        }
        extend(res);
    }

    private void extend(Response response) {
        outstandingRequests -= 1;
        maybeSendContinue();
        extendInternal(response);
    }

    private void extendInternal(Response response) {
        threshold = response.data.size();
        if (error == null) {
            if (response.isPartial()) {
                bufferedItems.addAll(response.data);
            } else if (response.isSequence()) {
                bufferedItems.addAll(response.data);
                error = new NoSuchElementException();
            } else {
                error = response.makeError(query);
            }
        }
        if (outstandingRequests == 0 && error != null) {
            connection.removeFromCache(response.token);
        }
    }

    void setError(String errMsg) {
        if (error == null) {
            error = new ReqlRuntimeError(errMsg);
            Response dummyResponse = Response
                    .make(query.token, ResponseType.SUCCESS_SEQUENCE)
                    .build();
            extendInternal(dummyResponse);
        }
    }
    //end
}
