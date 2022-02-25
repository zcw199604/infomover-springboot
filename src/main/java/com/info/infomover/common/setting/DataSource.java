package com.info.infomover.common.setting;


import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(value = ElementType.TYPE)
public @interface DataSource {

    /**
     * datasource type
     * 数据源类型
     */
    String type();


}
