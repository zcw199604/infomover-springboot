package com.info.infomover.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

//@Provider
public class DefaultExceptionHandler {
    /*private static Logger logger = LoggerFactory.getLogger(DefaultExceptionHandler.class);
    @Override
    public Response toResponse(Exception exception) {
        String msg;
        String detail;
        //TODO 中文翻译
        msg = "Exception: " + exception.getMessage();
        StringWriter stringWriter= new StringWriter();
        PrintWriter writer= new PrintWriter(stringWriter);
        exception.printStackTrace(writer);
        StringBuffer buffer= stringWriter.getBuffer();
        detail = buffer.toString();
        Map<String, String> err = new HashMap<>();
        logger.error("Exception: ",exception);
        err.put("message", msg);
        err.put("detail", detail);
        return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
    }
*/
}
