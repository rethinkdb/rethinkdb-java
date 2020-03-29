package com.rethinkdb.ast;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.gen.ast.Binary;
import com.rethinkdb.gen.ast.Datum;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.gen.proto.TermType;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.GroupedResult;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;
import com.rethinkdb.utils.Types;

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
        // set initial capacity to max size possible, avoids resizing
        List<Object> list = new ArrayList<>(3);
        list.add(termType.value);
        list.add(args.isEmpty() ? Collections.emptyList() : args.stream().map(ReqlAst::build).collect(Collectors.toList()));
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
        return conn.run(this, new OptArgs(), null, null, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public Result<Object> run(Connection conn, OptArgs runOpts) {
        return conn.run(this, runOpts, null, null, null);
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
        return conn.run(this, new OptArgs(), fetchMode, null, null);
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
        return conn.run(this, new OptArgs(), null, null, Types.of(typeRef));
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
        return conn.run(this, new OptArgs(), null, null, typeRef);
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
        return conn.run(this, runOpts, fetchMode, null, null);
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
        return conn.run(this, runOpts, null, null, Types.of(typeRef));
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
        return conn.run(this, runOpts, null, null, typeRef);
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
        return conn.run(this, new OptArgs(), fetchMode, null, Types.of(typeRef));
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
        return conn.run(this, new OptArgs(), fetchMode, null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result, with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.run(this, runOpts, fetchMode, null, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> run(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.run(this, runOpts, fetchMode, null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result asynchronously.
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runAsync(Connection conn) {
        return conn.runAsync(this, new OptArgs(), null, null, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result asynchronously.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runAsync(Connection conn, OptArgs runOpts) {
        return conn.runAsync(this, runOpts, null, null, null);
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
        return conn.runAsync(this, new OptArgs(), fetchMode, null, null);
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
        return conn.runAsync(this, new OptArgs(), null, null, Types.of(typeRef));
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
        return conn.runAsync(this, new OptArgs(), null, null, typeRef);
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
        return conn.runAsync(this, runOpts, fetchMode, null, null);
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
        return conn.runAsync(this, runOpts, null, null, Types.of(typeRef));
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
        return conn.runAsync(this, runOpts, null, null, typeRef);
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
        return conn.runAsync(this, new OptArgs(), fetchMode, null, Types.of(typeRef));
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
        return conn.runAsync(this, new OptArgs(), fetchMode, null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result asynchronously, with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, null, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, null, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped atom.
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public Object runAtom(Connection conn) {
        return handleAtom(conn.run(this, new OptArgs(), null, null, null));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped atom.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public Object runAtom(Connection conn, OptArgs runOpts) {
        return handleAtom(conn.run(this, runOpts, null, null, null));
    }

    /**
     * Runs this query via connection {@code conn} with  the specified {@code fetchMode}
     * and returns the unwrapped atom.
     *
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public Object runAtom(Connection conn, Result.FetchMode fetchMode) {
        return handleAtom(conn.run(this, new OptArgs(), fetchMode, null, null));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped atom, with the values
     * converted to the type of {@code Class<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> T runAtom(Connection conn, Class<T> typeRef) {
        return handleAtom(conn.run(this, new OptArgs(), null, null, Types.of(typeRef)));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped atom, with the values
     * converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> T runAtom(Connection conn, TypeReference<T> typeRef) {
        return handleAtom(conn.run(this, new OptArgs(), null, null, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode} and
     * returns the unwrapped atom.
     *
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public Object runAtom(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode) {
        return handleAtom(conn.run(this, runOpts, fetchMode, null, null));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped atom,
     * with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> T runAtom(Connection conn, OptArgs runOpts, Class<T> typeRef) {
        return handleAtom(conn.run(this, runOpts, null, null, Types.of(typeRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped atom,
     * with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> T runAtom(Connection conn, OptArgs runOpts, TypeReference<T> typeRef) {
        return handleAtom(conn.run(this, runOpts, null, null, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped atom,
     * with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> T runAtom(Connection conn, Result.FetchMode fetchMode, Class<T> typeRef) {
        return handleAtom(conn.run(this, new OptArgs(), fetchMode, null, Types.of(typeRef)));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped atom,
     * with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> T runAtom(Connection conn, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return handleAtom(conn.run(this, new OptArgs(), fetchMode, null, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped atom, with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> T runAtom(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return handleAtom(conn.run(this, runOpts, fetchMode, null, Types.of(typeRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped atom, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> T runAtom(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return handleAtom(conn.run(this, runOpts, fetchMode, null, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped atom asynchronously.
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public CompletableFuture<Object> runAtomAsync(Connection conn) {
        return conn.runAsync(this, new OptArgs(), null, null, null).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped atom
     * asynchronously.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public CompletableFuture<Object> runAtomAsync(Connection conn, OptArgs runOpts) {
        return conn.runAsync(this, runOpts, null, null, null).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with  the specified {@code fetchMode}
     * and returns the unwrapped atom asynchronously.
     *
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public CompletableFuture<Object> runAtomAsync(Connection conn, Result.FetchMode fetchMode) {
        return conn.runAsync(this, new OptArgs(), fetchMode, null, null).thenApplyAsync(ReqlAst::handleAtom);
    }


    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped atom asynchronously,
     * with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, Class<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), null, null, Types.of(typeRef)).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped atom asynchronously,
     * with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, TypeReference<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), null, null, typeRef).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode} and
     * returns the unwrapped atom asynchronously.
     *
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public CompletableFuture<Object> runAtomAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode) {
        return conn.runAsync(this, runOpts, fetchMode, null, null).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped atom
     * asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, OptArgs runOpts, Class<T> typeRef) {
        return conn.runAsync(this, runOpts, null, null, Types.of(typeRef)).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped atom
     * asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, OptArgs runOpts, TypeReference<T> typeRef) {
        return conn.runAsync(this, runOpts, null, null, typeRef).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped atom
     * asynchronously, with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, null, Types.of(typeRef)).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped atom
     * asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, null, typeRef).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped atom asynchronously, with the values converted to the type of {@code Class<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, null, Types.of(typeRef)).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped atom asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<T> runAtomAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, null, typeRef).thenApplyAsync(ReqlAst::handleAtom);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result, with
     * the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, Class<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result, with
     * the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, TypeReference<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result, with
     * the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, Class<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result, with
     * the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result, with
     * the values converted to the defined grouping and value types.
     *
     * @param <K>     The grouping type
     * @param <V>     The value type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, TypeReference<GroupedResult<K, V>> typeRef) {
        return handleGrouping(conn.run(this, new OptArgs(), null, true, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, Class<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, TypeReference<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, Class<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>     The grouping type
     * @param <V>     The value type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, TypeReference<GroupedResult<K, V>> typeRef) {
        return handleGrouping(conn.run(this, runOpts, null, true, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, Result.FetchMode fetchMode, Class<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, Result.FetchMode fetchMode, TypeReference<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, Result.FetchMode fetchMode, Class<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, Result.FetchMode fetchMode, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result,
     * with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param typeRef   The type to convert to
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, Result.FetchMode fetchMode, TypeReference<GroupedResult<K, V>> typeRef) {
        return handleGrouping(conn.run(this, new OptArgs(), fetchMode, true, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<K> keyRef, Class<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return handleGrouping(conn.run(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <K, V> Map<K, Set<V>> runGrouping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<GroupedResult<K, V>> typeRef) {
        return handleGrouping(conn.run(this, runOpts, fetchMode, true, typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, Class<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, TypeReference<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, Class<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>     The grouping type
     * @param <V>     The value type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, TypeReference<GroupedResult<K, V>> typeRef) {
        return conn.runAsync(this, new OptArgs(), null, true, typeRef).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, Class<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, TypeReference<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, Class<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>      The grouping type
     * @param <V>      The value type
     * @param conn     The connection to run this query
     * @param runOpts  The options to run this query with
     * @param keyRef   The grouping type to convert to
     * @param valueRef The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, runOpts, null, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>     The grouping type
     * @param <V>     The value type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, TypeReference<GroupedResult<K, V>> typeRef) {
        return conn.runAsync(this, runOpts, null, true, typeRef).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, Result.FetchMode fetchMode, Class<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, Result.FetchMode fetchMode, TypeReference<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, Result.FetchMode fetchMode, Class<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, Result.FetchMode fetchMode, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the unwrapped grouping result
     * asynchronously, with the values converted to the defined grouping and value types.
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, Result.FetchMode fetchMode, TypeReference<GroupedResult<K, V>> typeRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, typeRef).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result asynchronously, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result asynchronously, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<K> keyRef, Class<V> valueRef) {
        return conn.runAsync(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result asynchronously, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result asynchronously, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param keyRef    The grouping type to convert to
     * @param valueRef  The value type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<K> keyRef, TypeReference<V> valueRef) {
        return conn.runAsync(this, runOpts, fetchMode, true, Types.groupOf(keyRef, valueRef)).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the unwrapped grouping result asynchronously, with the values converted to the defined grouping and value types
     *
     * @param <K>       The grouping type
     * @param <V>       The value type
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <K, V> CompletableFuture<Map<K, Set<V>>> runGroupingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<GroupedResult<K, V>> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, true, typeRef).thenApplyAsync(ReqlAst::handleGrouping);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence.
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public Result<Object> runUnwrapping(Connection conn) {
        return conn.run(this, new OptArgs(), null, true, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public Result<Object> runUnwrapping(Connection conn, OptArgs runOpts) {
        return conn.run(this, runOpts, null, true, null);
    }

    /**
     * Runs this query via connection {@code conn} with  the specified {@code fetchMode}
     * and returns the result.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence.
     *
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public Result<Object> runUnwrapping(Connection conn, Result.FetchMode fetchMode) {
        return conn.run(this, new OptArgs(), fetchMode, true, null);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result, with the values
     * converted to the type of {@code Class<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> runUnwrapping(Connection conn, Class<T> typeRef) {
        return conn.run(this, new OptArgs(), null, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result, with the values
     * converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> Result<T> runUnwrapping(Connection conn, TypeReference<T> typeRef) {
        return conn.run(this, new OptArgs(), null, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode} and
     * returns the result.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence.
     *
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public Result<Object> runUnwrapping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode) {
        return conn.run(this, runOpts, fetchMode, true, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result, with the values
     * converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> runUnwrapping(Connection conn, OptArgs runOpts, Class<T> typeRef) {
        return conn.run(this, runOpts, null, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result, with the values
     * converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> runUnwrapping(Connection conn, OptArgs runOpts, TypeReference<T> typeRef) {
        return conn.run(this, runOpts, null, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result, with
     * the values converted to the type of {@code Class<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> runUnwrapping(Connection conn, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.run(this, new OptArgs(), fetchMode, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result, with
     * the values converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> runUnwrapping(Connection conn, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.run(this, new OptArgs(), fetchMode, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result, with the values converted to the type of {@code Class<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> runUnwrapping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.run(this, runOpts, fetchMode, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result, with the values converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> Result<T> runUnwrapping(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.run(this, runOpts, fetchMode, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result asynchronously.
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runUnwrappingAsync(Connection conn) {
        return conn.runAsync(this, new OptArgs(), null, true, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result asynchronously.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence.
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runUnwrappingAsync(Connection conn, OptArgs runOpts) {
        return conn.runAsync(this, runOpts, null, true, null);
    }

    /**
     * Runs this query via connection {@code conn} with  the specified {@code fetchMode}
     * and returns the result asynchronously.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence.
     *
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runUnwrappingAsync(Connection conn, Result.FetchMode fetchMode) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, null);
    }


    /**
     * Runs this query via connection {@code conn} with default options and returns the result asynchronously, with the
     * values converted to the type of {@code Class<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, Class<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), null, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns the result asynchronously, with the
     * values converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param typeRef The type to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, TypeReference<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), null, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode} and
     * returns the result asynchronously.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence.
     *
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @return The result of this query
     */
    public CompletableFuture<Result<Object>> runUnwrappingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode) {
        return conn.runAsync(this, runOpts, fetchMode, true, null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result asynchronously,
     * with the values converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, OptArgs runOpts, Class<T> typeRef) {
        return conn.runAsync(this, runOpts, null, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns the result asynchronously,
     * with the values converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>     The result type
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @param typeRef The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, OptArgs runOpts, TypeReference<T> typeRef) {
        return conn.runAsync(this, runOpts, null, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result
     * asynchronously, with the values converted to the type of {@code Class<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with the specified {@code fetchMode} and returns the result
     * asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.runAsync(this, new OptArgs(), fetchMode, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result asynchronously, with the values converted to the type of {@code Class<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, Class<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, true, Types.of(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts}, the specified {@code fetchMode}
     * and returns the result asynchronously, with the values converted to the type of {@code TypeReference<T>}.
     * If the query returns an atom which is an array, it'll unwrap the atom as if it were a completed sequence,
     * and the type conversion will be applied to each element of the array instead on the array as a whole.
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param fetchMode The fetch mode to use in partial sequences
     * @param typeRef   The type to convert to
     * @return The result of this query
     */
    public <T> CompletableFuture<Result<T>> runUnwrappingAsync(Connection conn, OptArgs runOpts, Result.FetchMode fetchMode, TypeReference<T> typeRef) {
        return conn.runAsync(this, runOpts, fetchMode, true, typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with default options without awaiting the response.
     *
     * @param conn The connection to run this query
     */
    public void runNoReply(Connection conn) {
        conn.runNoReply(this, new OptArgs());
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} without awaiting the response.
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

    private static <T> T handleAtom(Result<T> result) {
        if (!result.responseType().equals(ResponseType.SUCCESS_ATOM)) {
            throw new IllegalStateException("result is not an atom.");
        }

        return result.single();
    }

    private static <K, V> Map<K, Set<V>> handleGrouping(Result<GroupedResult<K, V>> result) {
        return GroupedResult.toMap(result.toList());
    }
}