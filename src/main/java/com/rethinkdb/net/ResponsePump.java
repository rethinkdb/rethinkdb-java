package com.rethinkdb.net;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

public interface ResponsePump {
    interface Factory {
        ResponsePump newPump(@NotNull ConnectionSocket socket);
    }

    CompletableFuture<QueryResponse> await(long token);

    void shutdownPump();
}
