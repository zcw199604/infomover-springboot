package com.info.infomover.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import javax.ws.rs.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

@Logged
//@Interceptor
public class LogInterceptor {
    private static Logger logger = LoggerFactory.getLogger(LogInterceptor.class);

    //@AroundInvoke
    public Object logging(InvocationContext ic) throws Exception {
        /*Method method = ic.getMethod();
        Log annotation = ic.getMethod().getAnnotation(Log.class);
        String httpMethod = getHttpMethod(method.getDeclaredAnnotations());
        String path = getPath(method);
        Object name = null;
        if (annotation != null) {
            String action = annotation.action();
            String itemId = annotation.itemId();
            String description = annotation.description();
            SpelUtil spelUtil = new SpelUtil(ic);
            name = spelUtil.cacl("#ctx.getUserPrincipal.name");
            Object itemId_ = null;
            if (StringUtils.isNotBlank(itemId)) {
                itemId_ = spelUtil.cacl(itemId);
            }
            logger.info(" action: {} ,httpMethod: {},username: {},description: {},itemId: {},path: {}", action, httpMethod, name, description, itemId_, path);
        } else {
            logger.info("username:{},httpMethod:{} ,path:{}", name, httpMethod, path);
        }*/
        return ic.proceed();
    }


    private String getPath(Method method) {
        Path path = method.getAnnotation(Path.class);
        Path fPath = method.getDeclaringClass().getAnnotation(Path.class);

        String basePath = "";
        if (path == null) {
            basePath = fPath.value();
        }else {
            basePath = fPath.value() + path.value();
        }

        return basePath;
    }


    private String getHttpMethod(Annotation[] declaredAnnotations) {
        String httpMethodStr = "";
        for (int i = 0; i < declaredAnnotations.length; i++) {
            Annotation declaredAnnotation = declaredAnnotations[i];
            if (declaredAnnotation instanceof GET) {
                httpMethodStr = HttpMethod.GET;
            } else if (declaredAnnotation instanceof POST) {
                httpMethodStr = HttpMethod.POST;
            } else if (declaredAnnotation instanceof PUT) {
                httpMethodStr = HttpMethod.PUT;
            } else if (declaredAnnotation instanceof DELETE) {
                httpMethodStr = HttpMethod.DELETE;
            } else if (declaredAnnotation instanceof PATCH) {
                httpMethodStr = HttpMethod.PATCH;
            } else if (declaredAnnotation instanceof OPTIONS) {
                httpMethodStr = HttpMethod.OPTIONS;
            }
        }
        return httpMethodStr;
    }
}
