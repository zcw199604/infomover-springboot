package com.info.infomover.common.setting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 字段从指定输入选择
 */
public enum Select {

    INPUT("input"),

    LEFT("left"),

    RIGHT("right"),

    DATASET("dataset"),

    SCHEMA("schema"),

    /**
     * 多输入节点，表示从输入节点的某个ID获取数据，如SQL Step
     */
    ID("id"),

    NULL(null);

    private String name;

    @JsonCreator
    Select(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
