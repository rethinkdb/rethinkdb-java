package com.rethinkdb.model;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public class Backtrace {
    private List<Object> rawBacktrace;

    private Backtrace(List<Object> rawBacktrace) {
        this.rawBacktrace = rawBacktrace;
    }

    public static @Nullable Backtrace fromList(List<Object> rawBacktrace) {
        if (rawBacktrace == null || rawBacktrace.size() == 0) {
            return null;
        }
        return new Backtrace(rawBacktrace);
    }

    public List<Object> getRawBacktrace() {
        return rawBacktrace;
    }
}
