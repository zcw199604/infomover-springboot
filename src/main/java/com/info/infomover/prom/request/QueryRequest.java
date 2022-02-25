package com.info.infomover.prom.request;

import com.info.infomover.prom.query.PromQuery;

import javax.ws.rs.FormParam;
import java.io.Serializable;

public class QueryRequest implements Serializable {
    
    @FormParam("query")
    public String query;
    
    @FormParam("time")
    public Long time;
    
    @FormParam("timeout")
    public String timeout;
    
    public static QueryRequest build(PromQuery promQuery, Long time){
        return build(promQuery, time, null);
    }
    
    public static QueryRequest build(PromQuery promQuery, Long time, String timeout) {
        return build(promQuery.build(), time, timeout);
    }
    
    public static QueryRequest build(String query, Long time, String timeout){
        QueryRequest request = new QueryRequest();
        request.query = query;
        request.time = time;
        request.timeout = timeout;
        return request;
    }
    
}
