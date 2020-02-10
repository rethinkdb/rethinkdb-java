package com.rethinkdb.ast;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlRuntimeError;
import com.rethinkdb.gen.proto.QueryType;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Util;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/*
 * An instance for a query that has been sent to the server.
 * Keeps track of its token, the args to .run() it was called with,
 * and its query type.
 */
public class Query {
    private static final Logger logger = LoggerFactory.getLogger(Query.class);

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

    public static Query stop(long token) {
        return new Query(QueryType.STOP, token, null, new OptArgs());
    }

    public static Query continue_(long token) {
        return new Query(QueryType.CONTINUE, token, null, new OptArgs());
    }

    public static Query start(long token, ReqlAst term, OptArgs globalOptions) {
        return new Query(QueryType.START, token, term, globalOptions);
    }

    public static Query noreplyWait(long token) {
        return new Query(QueryType.NOREPLY_WAIT, token, null, new OptArgs());
    }

    public ByteBuffer serialize() {
        try {
            List<Object> queryArr = new ArrayList<>();
            queryArr.add(type.value);
            if (term != null) {
                queryArr.add(term.build());
            }
            if (!globalOptions.isEmpty()) {
                queryArr.add(ReqlAst.buildOptarg(globalOptions));
            }
            String queryJson = RethinkDB.getInternalMapper().writeValueAsString(queryArr);
            byte[] queryBytes = queryJson.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = Util.leByteBuffer(Long.BYTES + Integer.BYTES + queryBytes.length)
                .putLong(token)
                .putInt(queryBytes.length)
                .put(queryBytes);
            logger.trace("JSON Send: Token: {} {}", token, queryJson);
            return bb;
        } catch (IOException e) {
            throw new ReqlRuntimeError(e);
        }
    }
}
