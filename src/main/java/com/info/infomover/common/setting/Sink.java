package com.info.infomover.common.setting;


import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(value = ElementType.TYPE)
public @interface Sink {

    /**
     * datasource type
     * sink类型
     */
    String type();


}
