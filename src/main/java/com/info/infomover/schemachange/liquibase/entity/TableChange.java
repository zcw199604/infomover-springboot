package com.info.infomover.schemachange.liquibase.entity;

import lombok.Data;

import java.util.Map;

@Data
public class TableChange {

    private String type;

    private String id;

    private Map<String, Object> table;

}
