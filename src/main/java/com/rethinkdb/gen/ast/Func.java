// Autogenerated by metajava.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../../../../../../../../templates/ast/Func.java

package com.rethinkdb.gen.ast;

import com.rethinkdb.gen.proto.TermType;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.ast.ReqlAst;


import com.rethinkdb.model.ReqlLambda;
import com.rethinkdb.ast.Util;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Arrays;
import java.util.List;


public class Func extends ReqlExpr {

    private static AtomicInteger varId = new AtomicInteger();


    protected Func(Arguments args){
        super(TermType.FUNC, args, null);
    }

    public static Func fromLambda(ReqlLambda function) {
        if(function instanceof ReqlFunction1){
            ReqlFunction1 func1 = (ReqlFunction1) function;
            int var1 = nextVarId();
            List<Integer> varIds = Arrays.asList(
                var1);
            Object appliedFunction = func1.apply(
                new Var(var1)
            );
            return new Func(Arguments.make(
                  new MakeArray(varIds),
                  Util.toReqlAst(appliedFunction)));
        }
        else if(function instanceof ReqlFunction2){
            ReqlFunction2 func2 = (ReqlFunction2) function;
            int var1 = nextVarId();
            int var2 = nextVarId();
            List<Integer> varIds = Arrays.asList(
                var1, var2);
            Object appliedFunction = func2.apply(
                new Var(var1), new Var(var2)
            );
            return new Func(Arguments.make(
                  new MakeArray(varIds),
                  Util.toReqlAst(appliedFunction)));
        }
        else if(function instanceof ReqlFunction3){
            ReqlFunction3 func3 = (ReqlFunction3) function;
            int var1 = nextVarId();
            int var2 = nextVarId();
            int var3 = nextVarId();
            List<Integer> varIds = Arrays.asList(
                var1, var2, var3);
            Object appliedFunction = func3.apply(
                new Var(var1), new Var(var2), new Var(var3)
            );
            return new Func(Arguments.make(
                  new MakeArray(varIds),
                  Util.toReqlAst(appliedFunction)));
        }
        else if(function instanceof ReqlFunction4){
            ReqlFunction4 func4 = (ReqlFunction4) function;
            int var1 = nextVarId();
            int var2 = nextVarId();
            int var3 = nextVarId();
            int var4 = nextVarId();
            List<Integer> varIds = Arrays.asList(
                var1, var2, var3, var4);
            Object appliedFunction = func4.apply(
                new Var(var1), new Var(var2), new Var(var3), new Var(var4)
            );
            return new Func(Arguments.make(
                  new MakeArray(varIds),
                  Util.toReqlAst(appliedFunction)));
        }
        else {
            throw new ReqlDriverError("Arity of ReqlLambda not recognized!");
        }
    }

    private static int nextVarId(){
        return varId.incrementAndGet();
    }
}
