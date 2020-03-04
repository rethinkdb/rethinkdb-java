package com.rethinkdb.net;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A connection socket into the server.
 */
public interface ConnectionSocket extends Closeable {
    /**
     * A factory of sockets.
     */
    interface Factory {
        /**
         * Creates a new connection socket into the server.
         * @param hostname the hostname
         * @param port the post
         * @param sslContext an {@link SSLContext}, if any
         * @param timeoutMs a timeout, in milliseconds, if any
         * @return a new {@link ConnectionSocket}.
         */
        @NotNull ConnectionSocket newSocket(@NotNull String hostname,
                                   int port,
                                   @Nullable SSLContext sslContext,
                                   @Nullable Long timeoutMs);
    }

    /**
     * Checks if the connection socket is open.
     * @return true if the connection socket is open, false otherwise.
     */
    boolean isOpen();

    /**
     * Closes the connection socket.
     */
    void close();

    /**
     * Writes the contents of the buffer into the socket.
     * @param buffer the contents to write.
     */
    void write(@NotNull ByteBuffer buffer);

    /**
     * Reads a defined amount of bytes, and wraps it in a {@link ByteBuffer}.
     * @param length the length of bytes to read.
     * @return a {@link ByteBuffer} with the read contents.
     */
    @NotNull ByteBuffer read(int length);

    /**
     * Reads a null-terminated string, under a timeout. If time runs out, it throws instead.
     *
     * @param timeoutMs the timeout, in milliseconds
     * @return the string.
     */
    @NotNull String readCString(@Nullable Long timeoutMs);
}
