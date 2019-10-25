package com.rethinkdb.model;

import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.ast.Util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OptArgs extends LinkedHashMap<String, ReqlAst> {
    public OptArgs with(String key, Object value) {
        if (key != null) {
            put(key, Util.toReqlAst(value));
        }
        return this;
    }

    public OptArgs with(String key, List<Object> value) {
        if (key != null) {
            put(key, Util.toReqlAst(value));
        }
        return this;
    }

    public static OptArgs fromMap(Map<String, ReqlAst> map) {
        OptArgs oa = new OptArgs();
        oa.putAll(map);
        return oa;
    }

    public static OptArgs of(String key, Object val) {
        OptArgs oa = new OptArgs();
        oa.with(key, val);
        return oa;
    }
}
