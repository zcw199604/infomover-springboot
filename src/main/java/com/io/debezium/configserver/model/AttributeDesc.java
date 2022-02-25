package com.io.debezium.configserver.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: haijun
 * @Date: 2021/11/15 17:06
 */
@Getter
public class AttributeDesc implements Serializable {
    /*
    * 配置属性
    */
    private String name;
    /*
     * 属性中文名
     */
    private String description;
    /*
     * tips中文表达
     */
    private String tips;
    /*
     * 参数取值类型,一律小写
     */
    private String type = "string";
    /*
     * 参数所属类型
     */
    private String category = "connection";
    /*
     * 是否必填
     */
    private boolean required = false;
    /*
     * 是否为高级选项
     */
    private boolean advanced = false;

    private Object defaultValue;
    /*
     * 可选值列表
     */
    private List<String> values;


    private String zh_displayName;
    private String zh_description;

    @JsonIgnore
    private int order;

    public AttributeDesc(String name, String description,String tips, String type, String category,String zh_displayName,String zh_description, boolean required, boolean advanced, Object defaultValue, List<String> values){
        this.name = name;
        this.description = description;
        this.tips = tips;
        this.type = type;
        this.category = category;
        this.required = required;
        this.advanced = advanced;
        this.defaultValue = defaultValue;
        this.values = values;
        this.zh_displayName = zh_displayName;
        this.zh_description = zh_description;
    }

    public AttributeDesc(){
    }
}
