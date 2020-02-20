package com.rethinkdb.ast;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.gen.ast.Binary;
import com.rethinkdb.gen.ast.Datum;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.proto.TermType;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;

import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Base class for all ReQL queries.
 */
public class ReqlAst {
    protected final TermType termType;
    protected final Arguments args;
    protected final OptArgs optargs;

    protected ReqlAst(TermType termType, Arguments args, OptArgs optargs) {
        if (termType == null) {
            throw new ReqlDriverError("termType can't be null!");
        }
        this.termType = termType;
        this.args = args != null ? args : new Arguments();
        this.optargs = optargs != null ? optargs : new OptArgs();
    }

    public static Map<String, Object> buildOptarg(OptArgs opts) {
        Map<String, Object> result = new LinkedHashMap<>(opts.size());
        opts.forEach((name, arg) -> result.put(name, arg.build()));
        return result;
    }

    protected Object build() {
        // Create a JSON object from the Ast
        List<Object> list = new ArrayList<>();
        list.add(termType.value);
        if (args.size() > 0) {
            list.add(args.stream().map(ReqlAst::build).collect(Collectors.toList()));
        } else {
            list.add(Collections.emptyList());
        }
        if (optargs.size() > 0) {
            list.add(buildOptarg(optargs));
        }
        return list;
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result.
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public Result<Object> run(Connection conn) {
        return conn.run(this, new OptArgs(), null, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public Result<Object> run(Connection conn, OptArgs runOpts) {
        return conn.run(this, runOpts, null, null);
    }

    /**
     * Runs this query via connection {@code conn} with  the specified {@code fetchMode}
     * and returns the result.
     *
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public Result<Object> run(Connection conn, Result.FetchMode fetchMode) {
        return conn.run(this, new OptArgs(), fetchMode, null);
    }


    /**
     * Runs this query via connection {@code conn} with default options and returns the result, with the values
     * converted to the type of {@code Class<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, Class<T> typeRef) {
        return conn.run(this, new OptArgs(), null, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result, with the values
     * converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> Result<T> run(Connection conn, TypeReference<T> typeRef) {
        return conn.run(this, new OptArgs(), null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode} and
     * returns the result.
     *
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public Result<Object> run(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode) {
        return conn.run(this, runOpts, fetchMode, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result, with the values
     * converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, OptArgs runOpts, Class<T> typeRef) {
        return conn.run(this, runOpts, null, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result, with the values
     * converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, OptArgs runOpts, TypeReference<T> typeRef) {
        return conn.run(this, runOpts, null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result, with
     * the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.run(this, new OptArgs(), fetchMode, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result, with
     * the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.run(this, new OptArgs(), fetchMode, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result, with the values converted to the type of {@code Class<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.run(this, runOpts, fetchMode, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result, with the values converted to the type of {@code TypeReference<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.run(this, runOpts, fetchMode, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result asynchronously.
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runAsync(Connection conn) {
        return conn.runAsync(this, new OptArgs(), null, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result asynchronously.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runAsync(Connection conn, OptArgs runOpts) {
        return conn.runAsync(this, runOpts, null, null);
    }

    /**
     * Runs this query via connection {@code conn} with  the specified {@code fetchMode}
     * and returns the result asynchronously.
     *
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runAsync(Connection conn, Result.FetchMode fetchMode) {
        return conn.runAsync(this, new OptArgs(), fetchMode, null);
    }


    /**
     * Runs this query via connection {@code conn} with default options and returns the result asynchronously, with the
     * values converted to the type of {@code Class<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, Class<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), null, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result asynchronously, with the
     * values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, TypeReference<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode} and
     * returns the result asynchronously.
     *
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode) {
        return conn.runAsync(this, runOpts, fetchMode, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result asynchronously,
     * with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, OptArgs runOpts, Class<T> typeRef) {
        return conn.runAsync(this, runOpts, null, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result asynchronously,
     * with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, OptArgs runOpts, TypeReference<T> typeRef) {
        return conn.runAsync(this, runOpts, null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result
     * asynchronously, with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result
     * asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result asynchronously, with the values converted to the type of {@code Class<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result asynchronously, with the values converted to the type of {@code TypeReference<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with default options without waiting.
     *
     * @param conn The connection to run this query
     */
    public void runNoReply(Connection conn) {
        conn.runNoReply(this, new OptArgs());
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} without waiting.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     */
    public void runNoReply(Connection conn, OptArgs runOpts) {
        conn.runNoReply(this, runOpts);
    }

    /**
     * Returns the AST representation of this {@link ReqlAst}.
     *
     * @return a pretty-printed representation of this abstract syntax tree.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("<root>\n");
        astToString(builder, null, "", true);
        return builder.toString();
    }

    private void astToString(StringBuilder builder, String name, String indent, boolean tail) {
        builder.append(indent).append(tail ? "└── " : "├── ");

        if (name != null) {
            builder.append(name).append("=");
        }
        builder.append(getClass().getSimpleName()).append(':');

        if (this instanceof Datum) {
            Object datum = ((Datum) this).datum;
            builder.append(' ').append(datum).append(" (").append(datum.getClass().getSimpleName()).append(")");
        } else if (this instanceof Binary) {
            builder.append(' ');
            byte[] binaryData = ((Binary) this).binaryData;
            int length = binaryData != null ? binaryData.length : 0;
            builder.append('(').append(length).append(length != 1 ? " bytes" : " byte").append(")");
        }
        builder.append('\n');
        Iterator<ReqlAst> argsIterator = args.iterator();
        while (argsIterator.hasNext()) {
            ReqlAst arg = argsIterator.next();
            arg.astToString(builder, null,
                indent + (tail ? "    " : "│   "),
                !argsIterator.hasNext() && optargs.isEmpty());
        }

        if (!optargs.isEmpty()) {
            builder.append(indent).append(tail ? "    " : "│   ").append("└── ").append("<optArgs>: \n");
            Iterator<Entry<String, ReqlAst>> optIterator = optargs.entrySet().iterator();
            while (optIterator.hasNext()) {
                Entry<String, ReqlAst> entry = optIterator.next();
                entry.getValue().astToString(builder, entry.getKey(),
                    indent + (tail ? "    " : "│   ") + "    ",
                    !optIterator.hasNext());
            }
        }
    }

    /**
     * A TypeReference that accepts an class instead of compiler type information.
     *
     * @param <T> the type referred to.
     */
    private static class ClassReference<T> extends TypeReference<T> {
        private Class<T> c;

        ClassReference(Class<T> c) {
            this.c = c;
        }

        @Override
        public Type getType() {
            return c;
        }
    }
}