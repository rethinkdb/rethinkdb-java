package com.rethinkdb.model;


import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.utils.Internals;

import java.util.ArrayList;
import java.util.List;

public class Arguments extends ArrayList<ReqlAst> {
    public Arguments() {}

    public Arguments(Object arg) {
        if (arg instanceof List) {
            coerceAndAddAll((List<?>) arg);
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

    public Arguments(List<?> args) {
        coerceAndAddAll(args);
    }

    public static Arguments make(Object... args) {
        return new Arguments(args);
    }

    public void coerceAndAdd(Object obj) {
        add(Internals.toReqlAst(obj));
    }

    public void coerceAndAddAll(Object[] args) {
        for (Object arg : args) {
            coerceAndAdd(arg);
        }
    }

    public void coerceAndAddAll(List<?> args) {
        args.forEach(this::coerceAndAdd);
    }
}
