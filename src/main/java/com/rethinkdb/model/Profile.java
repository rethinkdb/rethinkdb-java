package com.rethinkdb.model;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public class Profile {

    private List<Object> values;

    private Profile(List<Object> profileObj) {
        this.values = profileObj;
    }

    public static @Nullable Profile fromList(List<Object> list) {
        if(list == null || list.size() == 0){
            return null;
        }
        return new Profile(list);
    }

    public List<Object> getValues() {
        return values;
    }
}
