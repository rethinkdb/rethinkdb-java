package com.rethinkdb.ast;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlRuntimeError;
import com.rethinkdb.gen.proto.QueryType;
import com.rethinkdb.model.OptArgs;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A query object that can be send to the server, and serializes itself to a [{@link ByteBuffer}.
 */
public class Query {
    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    public final QueryType type;
    public final long token;
    public final OptArgs globalOptions;

    public final @Nullable ReqlAst term;

    public Query(QueryType type, long token, @Nullable ReqlAst term, OptArgs globalOptions) {
        this.type = type;
        this.token = token;
        this.term = term;
        this.globalOptions = globalOptions;
    }

    public Query(QueryType type, long token) {
        this(type, token, null, new OptArgs());
    }

    public ByteBuffer serialize() {
        try {
            // set initial capacity to max size possible, avoids resizing
            List<Object> list = new ArrayList<>(3);
            list.add(type.value);
            if (term != null) {
                list.add(term.build());
            }
            if (!globalOptions.isEmpty()) {
                list.add(ReqlAst.buildOptarg(globalOptions));
            }
            String json = RethinkDB.getInternalMapper().writeValueAsString(list);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + bytes.length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(token)
                .putInt(bytes.length)
                .put(bytes);
            LOGGER.trace("JSON Send: Token: {} {}", token, json);
            return buffer;
        } catch (IOException e) {
            throw new ReqlRuntimeError(e);
        }
    }

    public static Query createStart(long token, ReqlAst term, OptArgs globalOptions) {
        return new Query(QueryType.START, token, term, globalOptions);
    }

    public static Query createContinue(long token) {
        return new Query(QueryType.CONTINUE, token);
    }

    public static Query createStop(long token) {
        return new Query(QueryType.STOP, token);
    }

    public static Query createNoreplyWait(long token) {
        return new Query(QueryType.NOREPLY_WAIT, token);
    }

    public static Query createServerInfo(long token) {
        return new Query(QueryType.SERVER_INFO, token);
    }
}
