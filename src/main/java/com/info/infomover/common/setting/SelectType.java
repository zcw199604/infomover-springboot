package com.info.infomover.common.setting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 提示UI选择什么类型的数据
 */
public enum SelectType {

    ID("id"),

    FIELDS("fields"),

    SCHEMA("schema"),

    DATASET("dataset"),

    UDF("udf"),

    NULL(null);

    private String name;

    @JsonCreator
    SelectType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
