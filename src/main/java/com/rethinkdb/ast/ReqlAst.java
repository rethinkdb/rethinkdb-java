package com.rethinkdb.ast;

import com.fasterxml.jackson.core.type.TypeReference;
import com.rethinkdb.gen.ast.Binary;
import com.rethinkdb.gen.ast.Datum;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.gen.proto.TermType;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import reactor.core.publisher.Flux;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Base class for all reql queries.
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
     * Runs this query via connection {@code conn} with default options and returns an atom result
     * or a sequence result as a cursor. The atom result either has a primitive type (e.g., {@code Integer})
     * or represents a JSON object as a {@code Map<String, Object>}. The cursor is a {@code com.rethinkdb.net.Cursor}
     * which may be iterated to get a sequence of atom results
     *
     * @param conn The connection to run this query
     * @return The result of this query
     */
    public Flux<Object> run(Connection conn) {
        return conn.run(this, new OptArgs(), null);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns an atom result
     * or a sequence result as a cursor. The atom result either has a primitive type (e.g., {@code Integer})
     * or represents a JSON object as a {@code Map<String, Object>}. The cursor is a {@code com.rethinkdb.net.Cursor}
     * which may be iterated to get a sequence of atom results
     *
     * @param conn    The connection to run this query
     * @param runOpts The options to run this query with
     * @return The result of this query
     */
    public Flux<Object> run(Connection conn, OptArgs runOpts) {
        return conn.run(this, runOpts, null);
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns an atom result
     * or a sequence result as a cursor. The atom result representing a JSON object is converted
     * to an object of type {@code Class<T>} specified with {@code typeRef}. The cursor
     * is a {@code com.rethinkdb.net.Cursor} which may be iterated to get a sequence of atom results
     * of type {@code Class<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param typeRef The class of POJO to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> Flux<T> run(Connection conn, Class<T> typeRef) {
        return conn.run(this, new OptArgs(), new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns an atom result
     * or a sequence result as a cursor. The atom result representing a JSON object is converted
     * to an object of type {@code Class<T>} specified with {@code typeRef}. The cursor
     * is a {@code com.rethinkdb.net.Cursor} which may be iterated to get a sequence of atom results
     * of type {@code Class<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param typeRef The class of POJO to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> Flux<T> run(Connection conn, OptArgs runOpts, Class<T> typeRef) {
        return conn.run(this, runOpts, new ClassReference<>(typeRef));
    }

    /**
     * Runs this query via connection {@code conn} with default options and returns an atom result
     * or a sequence result as a cursor. The atom result representing a JSON object is converted
     * to an object of type {@code Class<T>} specified with {@code typeRef}. The cursor
     * is a {@code com.rethinkdb.net.Cursor} which may be iterated to get a sequence of atom results
     * of type {@code Class<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param typeRef The class of POJO to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> Flux<T> run(Connection conn, TypeReference<T> typeRef) {
        return conn.run(this, new OptArgs(), typeRef);
    }

    /**
     * Runs this query via connection {@code conn} with options {@code runOpts} and returns an atom result
     * or a sequence result as a cursor. The atom result representing a JSON object is converted
     * to an object of type {@code Class<T>} specified with {@code typeRef}. The cursor
     * is a {@code com.rethinkdb.net.Cursor} which may be iterated to get a sequence of atom results
     * of type {@code Class<T>}
     *
     * @param <T>       The type of result
     * @param conn      The connection to run this query
     * @param runOpts   The options to run this query with
     * @param typeRef The class of POJO to convert to
     * @return The result of this query (either a {@code P or a Cursor<P>}
     */
    public <T> Flux<T> run(Connection conn, OptArgs runOpts, TypeReference<T> typeRef) {
        return conn.run(this, runOpts, typeRef);
    }

    public void runNoReply(Connection conn) {
        conn.runNoReply(this, new OptArgs());
    }

    public void runNoReply(Connection conn, OptArgs globalOpts) {
        conn.runNoReply(this, globalOpts);
    }

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
            @Nullable byte[] binaryData = ((Binary) this).binaryData;
            int length = (int) (binaryData != null ? binaryData.length : 0);
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

    static class ClassReference<T> extends TypeReference<T> {
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