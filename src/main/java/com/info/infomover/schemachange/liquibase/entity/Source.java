package com.info.infomover.schemachange.liquibase.entity;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;

@Data
public class Source {

    private String server;

    @JsonAlias("business_key")
    private String businessKey;

    @JsonAlias("catalog_key")
    private String catalogKey;

    @JsonAlias("table_key")
    private String tableKey;

}
