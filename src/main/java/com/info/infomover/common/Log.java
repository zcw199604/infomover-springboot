package com.info.infomover.common;

import javax.interceptor.InterceptorBinding;
import java.lang.annotation.*;


/**
 *  日志信息注解
 * @author zcw
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Log {
    // 操作
    String action();

    // 对象类型
    String itemType() default "";

    // 对象标识
    String itemId() default  "";

    // 其他参数
    String param() default "";

    //方法描述
    String description() default "";
}
