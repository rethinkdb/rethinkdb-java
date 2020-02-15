package com.rethinkdb.gen.exc;

import org.jetbrains.annotations.Nullable;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.model.Backtrace;

public class ${camel(classname)} extends ${camel(superclass)} {

    @Nullable Backtrace backtrace;
    @Nullable ReqlAst term;

    public ${camel(classname)}() {
    }

    public ${camel(classname)}(String message) {
        super(message);
    }

    public ${camel(classname)}(String format, Object... args) {
        super(String.format(format, args));
    }

    public ${camel(classname)}(String message, Throwable cause) {
        super(message, cause);
    }

    public ${camel(classname)}(Throwable cause) {
        super(cause);
    }

    public ${camel(classname)}(String msg, ReqlAst term, Backtrace bt) {
        super(msg);
        this.backtrace = bt;
        this.term = term;
    }

    public @Nullable Backtrace getBacktrace() {
        return backtrace;
    }

    public ${camel(classname)} setBacktrace(Backtrace backtrace) {
        this.backtrace = backtrace;
        return this;
    }

    public @Nullable ReqlAst getTerm() {
        return this.term;
    }

    public ${camel(classname)} setTerm(ReqlAst term) {
        this.term = term;
        return this;
    }
}
