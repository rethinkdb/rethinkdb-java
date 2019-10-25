package com.rethinkdb;

import com.rethinkdb.ast.Query;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.gen.exc.*;
import com.rethinkdb.gen.proto.ErrorType;
import com.rethinkdb.gen.proto.ResponseType;
import com.rethinkdb.model.Backtrace;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

public class ErrorBuilder {
    private final String msg;
    private final ResponseType responseType;
    private @Nullable Backtrace backtrace;
    private @Nullable ErrorType errorType;
    private @Nullable ReqlAst term;

    public ErrorBuilder(String msg, ResponseType responseType) {
        this.msg = msg;
        this.responseType = responseType;
    }

    public ErrorBuilder setBacktrace(@Nullable Backtrace backtrace) {
        this.backtrace = backtrace;
        return this;
    }

    public ErrorBuilder setErrorType(@Nullable ErrorType errorType) {
        this.errorType = errorType;
        return this;
    }

    public ErrorBuilder setTerm(Query query) {
        this.term = query.term;
        return this;
    }

    public ReqlError build() {
        assert (msg != null);
        assert (responseType != null);
        Function<String, ReqlError> con;
        switch (responseType) {
            case CLIENT_ERROR:
                con = ReqlClientError::new;
                break;
            case COMPILE_ERROR:
                con = ReqlServerCompileError::new;
                break;
            case RUNTIME_ERROR: {
                if (errorType == null) {
                    con = ReqlRuntimeError::new;
                } else {
                    switch (errorType) {
                        case INTERNAL:
                            con = ReqlInternalError::new;
                            break;
                        case RESOURCE_LIMIT:
                            con = ReqlResourceLimitError::new;
                            break;
                        case QUERY_LOGIC:
                            con = ReqlQueryLogicError::new;
                            break;
                        case NON_EXISTENCE:
                            con = ReqlNonExistenceError::new;
                            break;
                        case OP_FAILED:
                            con = ReqlOpFailedError::new;
                            break;
                        case OP_INDETERMINATE:
                            con = ReqlOpIndeterminateError::new;
                            break;
                        case USER:
                            con = ReqlUserError::new;
                            break;
                        case PERMISSION_ERROR:
                            con = ReqlPermissionError::new;
                            break;
                        default:
                            con = ReqlRuntimeError::new;
                            break;
                    }
                }
                break;
            }
            default:
                con = ReqlError::new;
        }
        return con.apply(msg).setBacktrace(backtrace).setTerm(term);
    }
}
