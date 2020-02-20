package com.rethinkdb.model;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public class Backtrace {
    private List<Object> raw;

    private Backtrace(List<Object> raw) {
        this.raw = raw;
    }

    public static @Nullable Backtrace fromList(List<Object> raw) {
        if (raw == null || raw.size() == 0) {
            return null;
        }
        return new Backtrace(raw);
    }

    public List<Object> getRaw() {
        return raw;
    }
}
