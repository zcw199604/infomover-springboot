package com.info.infomover.prom.request;

import com.info.infomover.prom.constants.StepDuration;
import com.info.infomover.prom.query.PromQuery;

import javax.ws.rs.FormParam;
import java.io.Serializable;

public class RangeQueryRequest implements Serializable {
    
    @FormParam("query")
    public String query;
    
    @FormParam("start")
    public Long start;
    
    @FormParam("end")
    public Long end;
    
    @FormParam("step")
    public String step;
    
    @FormParam("timeout")
    public String timeout;
    
    public static RangeQueryRequest build(PromQuery promQuery, Long start, Long end, Integer step, StepDuration duration) {
        return build(promQuery, start, end, step+duration.duration);
    }
    
    public static RangeQueryRequest build(PromQuery promQuery, Long start, Long end, String step){
        return build(promQuery.build(), start, end, step, null);
    }
    
    public static RangeQueryRequest build(PromQuery promQuery, Long start, Long end, String step, String timeout){
        return build(promQuery.build(), start, end, step, timeout);
    }
    
    public static RangeQueryRequest build(String query, Long start, Long end, String step, String timeout){
        RangeQueryRequest request = new RangeQueryRequest();
        request.query = query;
        request.start = start;
        request.end = end;
        request.step = step;
        request.timeout = timeout;
        return request;
    }
    
}
