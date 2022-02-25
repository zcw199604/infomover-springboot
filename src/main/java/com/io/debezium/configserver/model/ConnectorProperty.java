/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.model;

import java.util.List;

public class ConnectorProperty {

    public enum Type {
        BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD;
    }

    public enum Category {
        CONNECTION, CONNECTION_ADVANCED, CONNECTION_ADVANCED_SSL, CONNECTION_ADVANCED_REPLICATION, CONNECTION_ADVANCED_PUBLICATION, FILTERS, CONNECTOR, CONNECTOR_SNAPSHOT, CONNECTOR_LOGMINER, CONNECTOR_ADVANCED, ADVANCED, ADVANCED_HEARTBEAT
    }

    public final String name;
    public final String displayName;
    public final String description;
    public String zh_displayName;
    public String zh_description;
    public String scope;
    public final Type type;
    public final Object defaultValue;
    public Object collValue;
    public Object syncValue;
    public final boolean isMandatory;
    public boolean hidden;
    public boolean advanced;
    public boolean noTrim;
    public final Category category;
    public List<String> allowedValues;


    public ConnectorProperty(String name, String displayName, String description, Type type, Object defaultValue, boolean isMandatory, Category category, List<String> allowedValues,String scope) {
        this.name = name;
        this.displayName = displayName;
        this.description = description;
        this.type = type;
        if(defaultValue == null){
            this.defaultValue = null;
        }else {
            this.defaultValue = defaultValue instanceof Class ? ((Class<?>) defaultValue).getName() : defaultValue.toString();
        }
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
        this.scope = scope;
    }

    public ConnectorProperty(String name, String displayName, String description, Type type, Object defaultValue, boolean isMandatory, Category category,
                             List<String> allowedValues,String scope,boolean noTrim) {
        this.name = name;
        this.displayName = displayName;
        this.description = description;
        this.type = type;
        if(defaultValue == null){
            this.defaultValue = null;
        }else {
            this.defaultValue = defaultValue instanceof Class ? ((Class<?>) defaultValue).getName() : defaultValue.toString();
        }
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
        this.scope = scope;
        this.noTrim = noTrim;
    }

    public ConnectorProperty(String name, String displayName, String description, Type type, Object defaultValue, boolean isMandatory, Category category, List<String> allowedValues) {
        this.name = name;
        this.displayName = displayName;
        this.description = description;
        this.type = type;
        if(defaultValue == null){
            this.defaultValue = null;
        }else {
            this.defaultValue = defaultValue instanceof Class ? ((Class<?>) defaultValue).getName() : defaultValue.toString();
        }
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
    }

    public ConnectorProperty(String name, String displayName, String description, Type type, Object defaultValue, boolean isMandatory, Category category, List<String> allowedValues, boolean hidden, boolean advanced) {
        this.name = name;
        this.displayName = displayName;
        this.description = description;
        this.type = type;
        if (defaultValue == null) {
            this.defaultValue = null;
        } else {
            this.defaultValue = defaultValue instanceof Class ? ((Class<?>) defaultValue).getName() : defaultValue.toString();
        }
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
        this.hidden = hidden;
        this.advanced = advanced;
    }


    public AttributeDesc convertToAttribute(){
        String category = this.category.name().toLowerCase();
        String type = convertType(this.type);
        return new AttributeDesc(this.name, this.displayName, this.description, type, category,this.zh_displayName,this.zh_description, this.isMandatory, !category.equals("connection"), this.defaultValue, this.allowedValues);
    }

    public static String convertType(Type type){
        switch (type){
            case BOOLEAN:
                return "Boolean";
            case INT:
            case SHORT:
                return "Int";
            case LONG:
                return "Bigint";
            case DOUBLE:
                return "Double";
            case STRING:
            case LIST:
            case CLASS:
            case PASSWORD:
            default:
                return "String";
        }
    }

}
