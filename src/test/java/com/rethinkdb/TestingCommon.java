package com.rethinkdb;

import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Result;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
public final class TestingCommon {
    public static Object AnythingIsFine = new Object() {
        public boolean equals(Object other) {
            return true;
        }

        public String toString() {
            return "AnythingIsFine";
        }
    };

    public static Bag bag(List<?> lst) {
        return new Bag(lst);
    }

    public static PartialLst partial(List<?> lst) {
        return new PartialLst(lst);
    }

    public static PartialDct partial(Map<?, ?> dct) {
        return new PartialDct(dct);
    }

    public static ArrLen arrlen(Long length, Object thing) {
        return new ArrLen(length.intValue(), thing);
    }

    public static UUIDMatch uuid() {
        return new UUIDMatch();
    }

    public static IntCmp int_cmp(Long nbr) {
        return new IntCmp(nbr);
    }

    public static FloatCmp float_cmp(Double nbr) {
        return new FloatCmp(nbr);
    }

    public static Regex regex(String regexString) {
        return new Regex(regexString);
    }

    public static Err err(String classname, String message) {
        return new Err(classname, message);
    }

    @SuppressWarnings("unused")
    public static Err err(String classname, String message, List<?> _unused) {
        return err(classname, message);
    }

    @SuppressWarnings("unused")
    public static ErrRegex err_regex(String classname, String message_rgx, Object _unused) {
        // Some invocations pass a stack frame as a third argument
        return new ErrRegex(classname, message_rgx);
    }

    public static Double float_(Double nbr) {
        return nbr;
    }

    public static ZoneOffset PacificTimeZone() {
        return ZoneOffset.ofHours(-7);
    }

    public static ZoneOffset UTCTimeZone() {
        return ZoneOffset.ofHours(0);
    }

    // Generated-tests specific methods
    public static List<?> fetch(Object resultObj, long limit) throws Exception {
        if (limit < 0) {
            limit = Long.MAX_VALUE;
        }
        Result<?> result = (Result<?>) resultObj;
        List<Object> list = new ArrayList<>((int) limit);
        for (long i = 0; i < limit; i++) {
            if (!result.hasNext()) {
                break;
            }
            list.add(result.next(500, TimeUnit.MILLISECONDS));
        }
        return list;
    }

    public static Object runOrCatch(Object query, OptArgs runopts, Connection conn) {
        if (query == null || query instanceof List) {
            return query;
        }
        try {
            Result<Object> res = ((ReqlAst) query).run(conn, runopts);
            if (res.responseType() == ResponseType.SUCCESS_ATOM || res.responseType().isError()) {
                return res.single();
            } else {
                return res.toList();
            }
        } catch (Exception e) {
            return e;
        }
    }

    public static Object maybeRun(Object query, Connection conn, OptArgs runopts) {
        return query instanceof ReqlAst ? ((ReqlAst) query).run(conn, runopts) : query;
    }

    public static Object maybeRun(Object query, Connection conn) {
        return query instanceof ReqlAst ? ((ReqlAst) query).run(conn) : query;
    }

    // Python emulated methods and interfaces
    public interface Partial {}

    public static class sys {
        public static class floatInfo {
            public static final Double min = Double.MIN_VALUE;
            public static final Double max = Double.MAX_VALUE;
        }
    }

    public static class datetime {
        public static OffsetDateTime fromtimestamp(double seconds, ZoneOffset offset) {
            Instant inst = Instant.ofEpochMilli(Double.valueOf(seconds * 1000).longValue());
            return OffsetDateTime.ofInstant(inst, offset);
        }

        public static OffsetDateTime now() {
            return OffsetDateTime.now();
        }
    }

    public static class ast {
        public static ZoneOffset rqlTzinfo(String offset) {
            if (offset.equals("00:00")) {
                offset = "Z";
            }
            return ZoneOffset.of(offset);
        }
    }

    // Implementations
    public static class Bag {
        final Set<?> bag;

        public Bag(List<?> bag) {
            this.bag = new HashSet<>(bag);
        }

        public boolean equals(Object other) {
            return other instanceof Collection && bag.equals(new HashSet<>(((Collection<?>) other)));
        }

        public String toString() {
            return "Bag(" + bag + ")";
        }

    }

    public static class PartialLst implements Partial {
        final List<?> lst;

        public PartialLst(List<?> lst) {
            this.lst = lst;
        }

        public String toString() {
            return "PartialLst(" + lst + ")";
        }

        public boolean equals(Object other) {
            return other instanceof List && ((List<?>) other).containsAll(lst);
        }

    }

    public static class PartialDct implements Partial {
        final Map<?, ?> dct;

        public PartialDct(Map<?, ?> dct) {
            this.dct = dct;
        }

        public boolean equals(Object other_) {
            if (other_ instanceof Map) {
                Map<?, ?> other = ((Map<?, ?>) other_);
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) dct).entrySet()) {
                    Object key = entry.getKey();
                    Object value = entry.getValue();
                    if (!other.containsKey(key)) {
                        System.out.println("Obtained didn't have key " + key);
                        return false;
                    }
                    Object otherValue = other.get(key);
                    if (value != null || otherValue != null) {
                        if (value == null || otherValue == null) {
                            System.out.println("One was null and the other wasn't for key " + key);
                            return false;
                        } else if (!value.equals(otherValue)) {
                            System.out.println("Weren't equal: " + value + " and " + otherValue);
                            return false;
                        }
                    }
                }
                return true;
            }
            return false;
        }

        public String toString() {
            return "PartialDct(" + dct + ")";
        }
    }

    public static class ArrLen {
        final List<?> list;
        final int length;
        final Object thing;

        public ArrLen(int length, Object thing) {
            this.length = length;
            this.thing = thing;
            this.list = Collections.nCopies(length, thing);
        }

        public String toString() {
            return "ArrLen(length=" + length + " of " + thing + ")";
        }

        public boolean equals(Object other) {
            return other instanceof List && list.equals(other);
        }
    }

    public static class UUIDMatch {
        static final Pattern p = Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

        public boolean equals(Object other) {
            return other instanceof String && p.matcher((String) other).matches();
        }

        public String toString() {
            return "Uuid()";
        }
    }

    public static class IntCmp {
        final Long nbr;

        public IntCmp(Long nbr) {
            this.nbr = nbr;
        }

        public boolean equals(Object other) {
            return nbr.equals(other);
        }
    }

    public static class FloatCmp {
        final Double nbr;

        public FloatCmp(Double nbr) {
            this.nbr = nbr;
        }

        public boolean equals(Object other) {
            return nbr.equals(other);
        }
    }

    public static class Regex {
        public final Pattern pattern;

        public Regex(String regexString) {
            this.pattern = Pattern.compile(regexString, Pattern.DOTALL);
        }

        public String toString() {
            return "Regex(" + pattern + ")";
        }

        public boolean equals(Object other) {
            return other instanceof String && pattern.matcher((String) other).matches();
        }
    }

    public static class Err {
        public final Class<?> clazz;
        public final String message;
        public final Pattern inRegex = Pattern.compile("^(?<message>[^\n]*?)(?: in)?:\n.*$", Pattern.DOTALL);
        public final Pattern assertionRegex = Pattern.compile("^(?<message>[^\n]*?)\nFailed assertion:.*$", Pattern.DOTALL);

        public Err(String classname, String message) {
            String clazzname = "com.rethinkdb.gen.exc." + classname;
            try {
                this.clazz = Class.forName(clazzname);
            } catch (ClassNotFoundException cnfe) {
                throw new RuntimeException("Bad exception class: " + clazzname, cnfe);
            }
            this.message = message;
        }

        public boolean equals(Object other) {
            if (!clazz.isInstance(other)) {
                System.out.println("Classes didn't match: " + clazz + " vs. " + other.getClass());
                return false;
            }
            String otherMessage = ((Exception) other).getMessage();
            otherMessage = inRegex.matcher(otherMessage).replaceFirst("${message}:");
            otherMessage = assertionRegex.matcher(otherMessage).replaceFirst("${message}");
            return message.equals(otherMessage);
        }

        public String toString() {
            return "Err(" + clazz + ": " + message + ")";
        }

    }

    public static class ErrRegex {
        public final Class<?> clazz;
        public final Pattern message_rgx;

        public ErrRegex(String classname, String message_rgx) {
            String clazzname = "com.rethinkdb.gen.exc." + classname;
            try {
                this.clazz = Class.forName(clazzname);
            } catch (ClassNotFoundException cnfe) {
                throw new RuntimeException("Bad exception class: " + clazzname, cnfe);
            }
            this.message_rgx = Pattern.compile(message_rgx);
        }

        public boolean equals(Object other) {
            return clazz.isInstance(other) && message_rgx.matcher(((Exception) other).getMessage()).matches();
        }
    }

    /*
    // UNUSED CODE BELOW
    public static ArrLen arrlen(Long length) {
        return new ArrLen(length.intValue(), null);
    }
    public static int len(List<?> array) {
        return array.size();
    }
    public static ErrRegex err_regex(String classname, String message_rgx) {
        return new ErrRegex(classname, message_rgx);
    }
    public static LongStream range(long start, long stop) {
        return LongStream.range(start, stop);
    }
    public static List<?> list(LongStream str) {
        return str.boxed().collect(Collectors.toList());
    }
    public static Object wait_(long length) {
        try {
            Thread.sleep(length * 1000);
        } catch (InterruptedException ie) {
        }
        return null;
    }
    public static List<?> fetch(Result<?> cursor) throws Exception {
        return fetch(cursor, -1);
    }
    public static class Lst {
        final List<?> lst;
        public Lst(List<?> lst) {
            this.lst = lst;
        }

        public boolean equals(Object other) {
            return lst.equals(other);
        }
    }
    public static class Dct {
        final Map dct;
        public Dct(Map dct){
            this.dct = dct;
        }

        public boolean equals(Object other) {
            return dct.equals(other);
        }
    }
 */
}
