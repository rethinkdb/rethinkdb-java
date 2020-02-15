package com.rethinkdb.net;

import com.rethinkdb.gen.exc.ReqlDriverError;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class DefaultConnectionFactory implements ConnectionSocket.Factory, ResponsePump.Factory {
    public static final DefaultConnectionFactory INSTANCE = new DefaultConnectionFactory();

    private DefaultConnectionFactory() {
    }

    @Override
    public ConnectionSocket newSocket(@NotNull String hostname, int port, SSLContext sslContext, Long timeoutMs) {
        SocketWrapper s = new SocketWrapper(hostname, port, sslContext, timeoutMs);
        s.connect();
        return s;
    }

    @Override
    public ResponsePump newPump(@NotNull ConnectionSocket socket) {
        return new ThreadResponsePump(socket);
    }

    private static class SocketWrapper implements ConnectionSocket {
        // networking stuff
        private Socket socket;
        private SocketFactory socketFactory = SocketFactory.getDefault();
        private SSLSocket sslSocket;
        private OutputStream writeStream;
        private DataInputStream readStream;

        // options
        private SSLContext sslContext;
        private Long timeoutMs;
        private final String hostname;
        private final int port;

        SocketWrapper(String hostname,
                      int port,
                      SSLContext sslContext,
                      Long timeoutMs) {
            this.hostname = hostname;
            this.port = port;
            this.sslContext = sslContext;
            this.timeoutMs = timeoutMs;
        }

        void connect() {
            Long deadline = timeoutMs == null ? null : System.currentTimeMillis() + timeoutMs;
            try {
                // establish connection
                final InetSocketAddress addr = new InetSocketAddress(hostname, port);
                socket = socketFactory.createSocket();
                socket.connect(addr, timeoutMs == null ? 0 : timeoutMs.intValue());
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);

                // should we secure the connection?
                if (sslContext != null) {
                    socketFactory = sslContext.getSocketFactory();
                    SSLSocketFactory sslSf = (SSLSocketFactory) socketFactory;
                    sslSocket = (SSLSocket) sslSf.createSocket(socket,
                        socket.getInetAddress().getHostAddress(),
                        socket.getPort(),
                        true);

                    // replace input/output streams
                    readStream = new DataInputStream(sslSocket.getInputStream());
                    writeStream = sslSocket.getOutputStream();

                    // execute SSL handshake
                    sslSocket.startHandshake();
                } else {
                    writeStream = socket.getOutputStream();
                    readStream = new DataInputStream(socket.getInputStream());
                }
            } catch (IOException e) {
                throw new ReqlDriverError("Connection timed out.", e);
            }
        }

        @Override
        public void write(ByteBuffer buffer) {
            try {
                buffer.flip();
                writeStream.write(buffer.array());
            } catch (IOException e) {
                throw new ReqlDriverError(e);
            }
        }

        @NotNull
        @Override
        public String readCString(@Nullable Long deadline) {
            try {
                final StringBuilder sb = new StringBuilder();
                char c;
                while ((c = (char) this.readStream.readByte()) != '\0') {
                    // is there a deadline?
                    if (deadline != null) {
                        // have we timed-out?
                        if (deadline < System.currentTimeMillis()) { // reached time-out
                            throw new ReqlDriverError("Connection timed out.");
                        }
                    }
                    sb.append(c);
                }

                return sb.toString();
            } catch (IOException e) {
                throw new ReqlDriverError(e);
            }
        }

        @NotNull
        @Override
        public ByteBuffer read(int bufsize) {
            try {
                byte[] buf = new byte[bufsize];
                int bytesRead = 0;
                while (bytesRead < bufsize) {
                    final int res = this.readStream.read(buf, bytesRead, bufsize - bytesRead);
                    if (res == -1) {
                        throw new ReqlDriverError("Reached the end of the read stream.");
                    } else {
                        bytesRead += res;
                    }
                }
                return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            } catch (IOException e) {
                throw new ReqlDriverError(e);
            }
        }

        @Nullable
        Integer clientPort() {
            if (socket != null) {
                return socket.getLocalPort();
            }
            return null;
        }

        @Nullable
        SocketAddress clientAddress() {
            if (socket != null) {
                return socket.getLocalSocketAddress();
            }
            return null;
        }

        @Override
        public boolean isOpen() {
            return socket != null && (socket.isConnected() && !socket.isClosed());
        }

        @Override
        public void close() {
            // if needed, disconnect from server
            if (socket != null && isOpen()) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new ReqlDriverError(e);
                }
            }
        }

        @Override
        public String toString() {
            return "ConnectionSocket(" + hostname + ':' + port + ')';
        }
    }

    private static class ThreadResponsePump implements ResponsePump {
        private final Thread thread;
        private Map<Long, CompletableFuture<Response>> awaiting = new ConcurrentHashMap<>();

        public ThreadResponsePump(ConnectionSocket socket) {
            this.thread = new Thread(() -> {
                // pump responses until interrupted
                while (true) {
                    // validate socket is open
                    if (!socket.isOpen()) {
                        shutdown(new IOException("Socket closed, exiting response pump."));
                        return;
                    }

                    if (awaiting == null) {
                        return;
                    }

                    // read response and send it to whoever is waiting, if anyone
                    try {
                        final Response response = Response.readFrom(socket);
                        final CompletableFuture<Response> awaiter = awaiting.remove(response.token);
                        if (awaiter != null) {
                            awaiter.complete(response);
                        }
                    } catch (Exception e) {
                        shutdown(e);
                        return;
                    }
                }
            }, "RethinkDB-" + socket + "-ResponsePump");

        }

        @Override
        public Mono<Response> await(long token) {
            CompletableFuture<Response> future = new CompletableFuture<>();
            if (awaiting == null) {
                throw new ReqlDriverError("Response pump closed.");
            }
            awaiting.put(token, future);
            return Mono.fromFuture(future);
        }

        private void shutdown(Exception e) {
            Map<Long, CompletableFuture<Response>> awaiting = this.awaiting;
            this.awaiting = null;
            thread.interrupt();
            awaiting.forEach((token, future) -> {
                future.completeExceptionally(e);
            });
        }

        @Override
        public void shutdownPump() {
            shutdown(new ReqlDriverError("Response pump closed."));
        }
    }
}
