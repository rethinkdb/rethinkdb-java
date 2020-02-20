package com.rethinkdb.ast;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.*;
import com.rethinkdb.gen.exc.ReqlDriverCompileError;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.ReqlLambda;

import java.lang.reflect.Array;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;


public class Util {
    public static int startingDepth = 100;

    private Util() {
    }

    /**
     * Convert an object to {@link ReqlAst}
     *
     * @param val the original object.
     * @return ReqlAst
     */
    public static ReqlAst toReqlAst(Object val) {
        return toReqlAst(val, startingDepth);
    }

    /**
     * Convert an object to {@link ReqlExpr}
     *
     * @param val the original object.
     * @return ReqlAst
     */
    public static ReqlExpr toReqlExpr(Object val) {
        ReqlAst converted = toReqlAst(val);
        if (converted instanceof ReqlExpr) {
            return (ReqlExpr) converted;
        } else {
            throw new ReqlDriverError("Cannot convert %s to ReqlExpr", val);
        }
    }

    private static ReqlAst toReqlAst(Object val, int remainingDepth) {
        if (remainingDepth <= 0) {
            throw new ReqlDriverCompileError("Recursion limit reached converting to ReqlAst");
        }
        if (val instanceof ReqlAst) {
            return (ReqlAst) val;
        }

        if (val instanceof List) {
            Arguments innerValues = new Arguments();
            for (Object innerValue : (List<?>) val) {
                innerValues.add(toReqlAst(innerValue, remainingDepth - 1));
            }
            return new MakeArray(innerValues, null);
        }

        if (val instanceof Map) {
            Map<String, ReqlAst> obj = new MapObject<>();
            ((Map<?, ?>) val).forEach((key, value) -> {
                if (key.getClass().isEnum()) {
                    obj.put(((Enum<?>) key).name(), toReqlAst(value, remainingDepth - 1));
                } else if (key instanceof String) {
                    obj.put((String) key, toReqlAst(value, remainingDepth - 1));
                } else {
                    throw new ReqlDriverCompileError("Object keys can only be strings");
                }
            });
            return MakeObj.fromMap(obj);
        }

        if (val instanceof ReqlLambda) {
            return Func.fromLambda((ReqlLambda) val);
        }

        final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

        if (val instanceof LocalDateTime) {
            ZoneId zid = ZoneId.systemDefault();
            DateTimeFormatter fmt2 = fmt.withZone(zid);
            return Iso8601.fromString(((LocalDateTime) val).format(fmt2));
        }
        if (val instanceof ZonedDateTime) {
            return Iso8601.fromString(((ZonedDateTime) val).format(fmt));
        }
        if (val instanceof OffsetDateTime) {
            return Iso8601.fromString(((OffsetDateTime) val).format(fmt));
        }

        if (val instanceof Integer) {
            return new Datum(val);
        }

        if (val instanceof Number) {
            return new Datum(val);
        }

        if (val instanceof Boolean) {
            return new Datum(val);
        }

        if (val instanceof String) {
            return new Datum(val);
        }

        if (val == null) {
            return new Datum(null);
        }

        Class<?> valClass = val.getClass();
        if (valClass.isEnum()) {
            return new Datum(((Enum<?>) val).name());
        }

        if (valClass.isArray()) {
            if (val instanceof byte[]) {
                return new Binary(((byte[]) val));
            }
            Arguments innerValues = new Arguments();
            int length = Array.getLength(val);
            for (int i = 0; i < length; i++) {
                innerValues.add(toReqlAst(Array.get(val, i)));
            }
            return new MakeArray(innerValues, null);
        }

        // val is a non-null POJO, let's use jackson
        return toReqlAst(RethinkDB.getResultMapper().convertValue(val, Map.class), remainingDepth - 1);
    }
}
