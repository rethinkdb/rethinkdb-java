package com.rethinkdb.model;


import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.ast.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Arguments extends ArrayList<ReqlAst> {
    public Arguments() {}

    @SuppressWarnings("unchecked")
    public Arguments(Object arg) {
        if (arg instanceof List) {
            coerceAndAddAll((List<Object>) arg);
        } else {
            coerceAndAdd(arg);
        }
    }

    public Arguments(Arguments args) {
        addAll(args);
    }

    public Arguments(ReqlAst arg) {
        add(arg);
    }

    public Arguments(Object[] args) {
        coerceAndAddAll(args);
    }

    public Arguments(List<Object> args) {
        coerceAndAddAll(args);
    }

    public static Arguments make(Object... args) {
        return new Arguments(args);
    }

    public void coerceAndAdd(Object obj) {
        add(Util.toReqlAst(obj));
    }

    public void coerceAndAddAll(Object[] args) {
        for (Object arg : args) {
            coerceAndAdd(arg);
        }
    }

    public void coerceAndAddAll(List<Object> args) {
        args.forEach(this::coerceAndAdd);
    }
}
