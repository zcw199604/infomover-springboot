package com.info.infomover.prom;

import com.info.infomover.prom.request.QueryRequest;
import com.info.infomover.prom.request.RangeQueryRequest;

import javax.ws.rs.BeanParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/*@Path("/v1")
@RegisterRestClient(configKey = "prometheus")
@Produces(MediaType.APPLICATION_FORM_URLENCODED)
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)*/
public interface PromClient extends AutoCloseable {
    
    @POST
    @Path("/v1/query")
    Response query(@BeanParam QueryRequest request);
    
    
    @POST
    @Path("/v1/query_range")
    Response queryRange(@BeanParam RangeQueryRequest request) ;
    
}
