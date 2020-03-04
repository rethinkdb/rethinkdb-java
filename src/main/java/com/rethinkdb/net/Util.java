package com.rethinkdb.net;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlDriverError;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.*;

public class Util {
    private static final TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<Map<String, Object>>() {};

    private Util() {}

    public static Map<String, Object> readJSON(String str) {
        try {
            return RethinkDB.getInternalMapper().readValue(str, mapTypeRef);
        } catch (IOException e) {
            throw new ReqlDriverError(e);
        }
    }

    public static Map<String, Object> readJSON(ByteBuffer buf) {
        try {
            return RethinkDB.getInternalMapper().readValue(
                buf.array(),
                buf.arrayOffset() + buf.position(),
                buf.remaining(),
                mapTypeRef
            );
        } catch (IOException e) {
            throw new ReqlDriverError(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertToPojo(Object value, TypeReference<T> typeRef) {
        if (typeRef != null) {
            JavaType type = RethinkDB.getInternalMapper().getTypeFactory().constructType(typeRef);
            Class<T> rawClass = (Class<T>) type.getRawClass();
            if (rawClass.isEnum()) {
                Enum<?>[] enumConstants = ((Class<Enum<?>>) rawClass).getEnumConstants();
                for (Enum<?> enumConst : enumConstants) {
                    if (enumConst.name().equals(value)) {
                        return (T) enumConst;
                    }
                }
            } else if (rawClass.isAssignableFrom(value.getClass()) && type.containedTypeCount() == 0) {
                // class is assignable from value and has no type parameters.
                // since the only thing that matches those are primitives, strings and dates, it's safe
                return rawClass.cast(value);
            } else {
                try {
                    return RethinkDB.getResultMapper().convertValue(value, typeRef);
                } catch (Exception e) {
                    throw new ReqlDriverError("Tried to convert " + value + ", of type " + value.getClass() + ", to " + typeRef.getType() + ", but got " + e, e);
                }
            }
        }
        return (T) value;
    }
}



