package com.rethinkdb.net;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface ConnectionSocket extends Closeable {
    interface Factory {
        ConnectionSocket newSocket(@NotNull String hostname,
                                   int port,
                                   @Nullable SSLContext sslContext,
                                   @Nullable Long timeoutMs);
    }

    boolean isOpen();

    void close();

    void write(@NotNull ByteBuffer buffer);

    @NotNull ByteBuffer read(int size);

    /**
     * Reads a null-terminated string, under a timeout. If time runs out, it throws instead.
     *
     * @param timeoutMs the timeout, in milliseconds
     * @return the string.
     */
    @NotNull String readCString(@Nullable Long timeoutMs);
}
