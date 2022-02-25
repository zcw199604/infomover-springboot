package com.info.infomover.config;


import com.info.infomover.util.UserNotLoginExecption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

@RestControllerAdvice
public class BadRequestExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(BadRequestExceptionHandler.class);

    /**
     *
     *
     * @param exception 错误信息集合
     * @return 错误信息
     */
    @ExceptionHandler(Exception.class)
    public Response validationBodyException(Exception exception) {
        logger.error(exception.getMessage(), exception);
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
        err.put("message", msg);
        err.put("detail", detail);
        return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
    }

    /**
     *
     *
     * @param exception 错误信息集合
     * @return 错误信息
     */
    @ExceptionHandler(UserNotLoginExecption.class)
    public Response validationBodyException(UserNotLoginExecption exception) {
        logger.error(exception.getMessage(), exception);
        String msg;
        //TODO 中文翻译
        msg = "Exception: " + exception.getMessage();
        Map<String, String> err = new HashMap<>();
        err.put("message", msg);
        return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
    }

    /**
     * 参数类型转换错误
     *
     * @param exception 错误
     * @return 错误信息
     */
    @ExceptionHandler(HttpMessageConversionException.class)
    public Response parameterTypeException(HttpMessageConversionException exception) {
        logger.error(exception.getMessage(), exception);
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
        err.put("message", msg);
        err.put("detail", detail);
        return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
    }
    /**
     * 参数类型转换错误
     *
     * @param exception 错误
     * @return 错误信息
     */
    @ExceptionHandler(NoSuchElementException.class)
    public Response parameterTypeException(NoSuchElementException exception) {
        logger.error(exception.getMessage(), exception);
        String msg;
        String detail;
        //TODO 中文翻译
        StringWriter stringWriter= new StringWriter();
        PrintWriter writer= new PrintWriter(stringWriter);
        exception.printStackTrace(writer);
        StringBuffer buffer= stringWriter.getBuffer();
        detail = buffer.toString();
        Map<String, String> err = new HashMap<>();
        err.put("message", "param is error");
        err.put("detail", detail);
        return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
    }

}