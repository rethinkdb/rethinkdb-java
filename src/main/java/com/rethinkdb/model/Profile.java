package com.rethinkdb.model;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public class Profile {
    private List<Object> raw;

    private Profile(List<Object> raw) {
        this.raw = raw;
    }

    public static @Nullable Profile fromList(List<Object> raw) {
        if (raw == null || raw.size() == 0) {
            return null;
        }
        return new Profile(raw);
    }

    public List<Object> getRaw() {
        return raw;
    }
}
