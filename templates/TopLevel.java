import com.rethinkdb.ast.Util;
import com.rethinkdb.gen.exc.ReqlDriverError;
import com.rethinkdb.model.Arguments;
import com.rethinkdb.model.MapObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


<%page args="all_terms"/>
	package com.rethinkdb.gen.model;

public class TopLevel {

    public ReqlExpr expr(Object value){
        return Util.toReqlExpr(value);
    }

    public ReqlExpr row(Object... values) {
        throw new ReqlDriverError("r.row is not implemented in the Java driver."+
                                  " Use lambda syntax instead");
    }

    public MapObject<Object, Object> hashMap(Object key, Object val){
        return new MapObject<>().with(key, val);
    }

    public MapObject<Object, Object> hashMap() {
        return new MapObject<>();
    }

    public <T> List<T> array(T val0, T... vals){
        List<T> res = new ArrayList<>();
        res.add(val0);
        Collections.addAll(res, vals);
        return res;
    }

% for type in ["ReqlFunction0", "ReqlFunction1", "ReqlFunction2", "ReqlFunction3", "ReqlFunction4"]:
    public List<Object> array(${type} val0, ${type}... vals){
        List<Object> res = new ArrayList<>();
        res.add(val0);
        Collections.addAll(res, vals);
        return res;
    }
% endfor
    public List<Object> array(){
        return new ArrayList<>();
    }

%for term in all_terms.values():
  %if "TopLevel" in term["include_in"]:
    %for methodname in term['methodnames']:
      %for sig in term['signatures']:
        %if sig['first_arg'] not in ['Db', 'Table']:
    public ${term['classname']} ${methodname}(${
        ', '.join('%s %s' % (arg['type'], arg['var'])
                  for arg in sig['args'])}){
          % if methodname == 'binary':
        <% firstarg = sig['args'][0]['var'] %>
        if(${firstarg} instanceof byte[]){
            return new ${term['classname']}((byte[]) ${firstarg});
        }else{
          %endif
        Arguments args = new Arguments();
          %for arg in sig['args']:
            %if arg['type'] == 'Object...':
        args.coerceAndAddAll(${arg['var']});
            %else:
        args.coerceAndAdd(${arg['var']});
            %endif
          %endfor
        return new ${term['classname']}(args);
          % if methodname == 'binary':
        }
          %endif
    }
        %endif
      %endfor
    %endfor
  %endif
%endfor
}
