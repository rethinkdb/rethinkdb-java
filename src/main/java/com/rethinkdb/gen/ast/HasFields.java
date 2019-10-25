// Autogenerated by metajava.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../../../../../../../../templates/AstSubclass.java

package com.rethinkdb.gen.ast;

import com.rethinkdb.gen.proto.TermType;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.ast.ReqlAst;

public class HasFields extends ReqlExpr {

    public HasFields(Object arg) {
        this(new Arguments(arg), null);
    }
    public HasFields(Arguments args){
        this(args, null);
    }
    public HasFields(Arguments args, OptArgs optargs) {
        super(TermType.HAS_FIELDS, args, optargs);
    }

}
