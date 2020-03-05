// Autogenerated by metajava.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../../../../../../../../templates/Enum.java

package com.rethinkdb.gen.proto;

import org.jetbrains.annotations.Nullable;

public enum QueryType {

    START(1),
    CONTINUE(2),
    STOP(3),
    NOREPLY_WAIT(4),
    SERVER_INFO(5);

    public final int value;

    private QueryType(int value) {
        this.value = value;
    }

    public static QueryType fromValue(int value) {
        switch (value) {
            case 1: return QueryType.START;
            case 2: return QueryType.CONTINUE;
            case 3: return QueryType.STOP;
            case 4: return QueryType.NOREPLY_WAIT;
            case 5: return QueryType.SERVER_INFO;
            default:
                throw new IllegalArgumentException(String.format("%s is not a legal value for QueryType", value));
        }
    }

    public static @Nullable QueryType maybeFromValue(int value) {
        try {
            return fromValue(value);
        } catch (IllegalArgumentException iae) {
            return null;
        }
    }

}
