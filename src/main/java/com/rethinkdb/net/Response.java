package com.rethinkdb.net;

import com.rethinkdb.ErrorBuilder;
import com.rethinkdb.ast.Query;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.proto.ErrorType;
import com.rethinkdb.gen.proto.ResponseNote;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.model.Backtrace;
import com.rethinkdb.model.Profile;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Response {
    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    public final long token;
    public final ResponseType type;
    public final List<ResponseNote> notes;

    public final List<Object> data;
    public final @Nullable Profile profile;
    public final @Nullable Backtrace backtrace;
    public final @Nullable ErrorType errorType;

    public Response(long token,
                    ResponseType responseType,
                    List<Object> data,
                    List<ResponseNote> responseNotes,
                    @Nullable Profile profile,
                    @Nullable Backtrace backtrace,
                    @Nullable ErrorType errorType
    ) {
        this.token = token;
        this.type = responseType;
        this.data = data;
        this.notes = responseNotes;
        this.profile = profile;
        this.backtrace = backtrace;
        this.errorType = errorType;
    }

    public Response(long token, ResponseType responseType) {
        this(token, responseType, Collections.emptyList(), Collections.emptyList(), null, null, null);
    }

    public ReqlError makeError(Query query) {
        String msg = data.size() > 0 ?
            (String) data.get(0)
            : "Unknown error message";
        return new ErrorBuilder(msg, type)
            .setBacktrace(backtrace)
            .setErrorType(errorType)
            .setTerm(query)
            .build();
    }

    public boolean isFeed() {
        return notes.stream().anyMatch(ResponseNote::isFeed);
    }

    @Override
    public String toString() {
        return "Response{" +
            "token=" + token +
            ", type=" + type +
            ", notes=" + notes +
            ", data=" + data +
            ", profile=" + profile +
            ", backtrace=" + backtrace +
            '}';
    }

    @SuppressWarnings("unchecked")
    public static Response readFromSocket(ConnectionSocket socket) {
        final ByteBuffer header = socket.read(12);
        final long token = header.getLong();
        final int responseLength = header.getInt();
        final ByteBuffer buffer = socket.read(responseLength).order(ByteOrder.LITTLE_ENDIAN);

        if (Response.LOGGER.isTraceEnabled()) {
            Response.LOGGER.trace(
                "JSON Recv: Token: {} {}", token, new String(
                    buffer.array(),
                    buffer.arrayOffset() + buffer.position(),
                    buffer.remaining(),
                    StandardCharsets.UTF_8
                )
            );
        }

        Map<String, Object> json = Util.readJSON(buffer);
        return new Response(
            token,
            ResponseType.fromValue(((Long) json.get("t")).intValue()),
            (List<Object>) json.getOrDefault("r", Collections.emptyList()),
            ((List<Long>) json.getOrDefault("n", Collections.emptyList()))
                .stream()
                .map(Long::intValue)
                .map(ResponseNote::maybeFromValue)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()),
            Profile.fromList((List<Object>) json.get("p")),
            Backtrace.fromList((List<Object>) json.getOrDefault("b", null)),
            json.containsKey("e") ? ErrorType.maybeFromValue(((Long) json.get("e")).intValue()) : null
        );
    }
}
