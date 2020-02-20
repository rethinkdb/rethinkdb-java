package com.rethinkdb.net;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/**
 * A pump that read {@link Response}s using {@link Response#readFromSocket(ConnectionSocket)}.
 */
public interface ResponsePump {
    /**
     * A factory of response pumps.
     */
    interface Factory {
        /**
         * Creates a new response pump using the provided connection socket.
         * @param socket the {@link ConnectionSocket} to pump response pumps from
         * @return a new {@link ResponsePump}.
         */
        @NotNull ResponsePump newPump(@NotNull ConnectionSocket socket);
    }

    /**
     * Creates a response awaiter for a query token.
     * @param token the query token
     * @return a {@link CompletableFuture} that completes with a {@link Response} that matches the token.
     */
    @NotNull CompletableFuture<Response> await(long token);


    /**
     * Checks if the response pump is alive.
     * @return true if the response pump is alive, false otherwise.
     */
    boolean isAlive();

    /**
     * Shutdowns the response pump.
     */
    void shutdownPump();
}
