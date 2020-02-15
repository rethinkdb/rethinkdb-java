package com.rethinkdb.net;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface ResponsePump {
    interface Factory {
        ResponsePump newPump(@NotNull ConnectionSocket socket);
    }

    Mono<Response> await(long token);

    void shutdownPump();
}
