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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class Response {
    private static final Logger logger = LoggerFactory.getLogger(Query.class);

    public final long token;
    public final ResponseType type;
    public final List<ResponseNote> notes;

    public final List<Object> data;
    public final @Nullable Profile profile;
    public final @Nullable Backtrace backtrace;
    public final @Nullable ErrorType errorType;

    private Response(long token,
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

    public static Response parseFrom(long token, ByteBuffer buf) {
        if (Response.logger.isDebugEnabled()) {
            Response.logger.debug(
                "JSON Recv: Token: {} {}", token, Util.bufferToString(buf));
        }
        Map<String, Object> jsonResp = Util.toJSON(buf);
        ResponseType responseType = ResponseType.fromValue(((Long) jsonResp.get("t")).intValue());
        List<Long> responseNoteVals = new ArrayList<>();
        jsonResp.put("n", responseNoteVals);
        List<ResponseNote> responseNotes = responseNoteVals
            .stream()
            .map(Long::intValue)
            .map(ResponseNote::maybeFromValue)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        Builder res = new Builder(token, responseType);
        if (jsonResp.containsKey("e")) {
            res.setErrorType(((Long) jsonResp.get("e")).intValue());
        }
        return res.setNotes(responseNotes)
            .setProfile((List<Object>) jsonResp.getOrDefault("p", null))
            .setBacktrace((List<Object>) jsonResp.getOrDefault("b", null))
            .setData((List<Object>) jsonResp.getOrDefault("r", new ArrayList<>()))
            .build();
    }

    static Builder make(long token, ResponseType type) {
        return new Builder(token, type);
    }

    ReqlError makeError(Query query) {
        String msg = data.size() > 0 ?
            (String) data.get(0)
            : "Unknown error message";
        return new ErrorBuilder(msg, type)
            .setBacktrace(backtrace)
            .setErrorType(errorType)
            .setTerm(query)
            .build();
    }

    boolean isWaitComplete() {
        return type == ResponseType.WAIT_COMPLETE;
    }

    /* Whether the response is any kind of feed */
    boolean isFeed() {
        return notes.stream().anyMatch(ResponseNote::isFeed);
    }

    /* Whether the response is any kind of error */
    boolean isError() {
        return type.isError();
    }

    /* What type of success the response contains */
    boolean isAtom() {
        return type == ResponseType.SUCCESS_ATOM;
    }

    boolean isSequence() {
        return type == ResponseType.SUCCESS_SEQUENCE;
    }

    boolean isPartial() {
        return type == ResponseType.SUCCESS_PARTIAL;
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

    static class Builder {
        long token;
        ResponseType responseType;
        List<ResponseNote> notes = new ArrayList<>();
        List<Object> data = new ArrayList<>();
        @Nullable Profile profile;
        @Nullable Backtrace backtrace;
        @Nullable ErrorType errorType;

        Builder(long token, ResponseType responseType) {
            this.token = token;
            this.responseType = responseType;
        }

        Builder setNotes(List<ResponseNote> notes) {
            this.notes.addAll(notes);
            return this;
        }

        Builder setData(List<Object> data) {
            if (data != null) {
                this.data = data;
            }
            return this;
        }

        Builder setProfile(List<Object> profile) {
            this.profile = Profile.fromList(profile);
            return this;
        }

        Builder setBacktrace(List<Object> backtrace) {
            this.backtrace = Backtrace.fromList(backtrace);
            return this;
        }

        Builder setErrorType(int value) {
            this.errorType = ErrorType.fromValue(value);
            return this;
        }

        Response build() {
            return new Response(
                token,
                responseType,
                data,
                notes,
                profile,
                backtrace,
                errorType
            );
        }
    }
}
