package com.info.infomover.datasource;

import java.lang.annotation.*;


/**
 * 用于标记数据源的作用范围,是作为 source 还是 sink 或者全部
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(value = ElementType.TYPE)
public @interface DataSourceScope {
    DataSourceScopeType[] value();
}
