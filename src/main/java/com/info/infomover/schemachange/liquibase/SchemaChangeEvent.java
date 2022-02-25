package com.info.infomover.schemachange.liquibase;

import com.info.infomover.schemachange.liquibase.entity.Source;
import com.info.infomover.schemachange.liquibase.entity.TableChange;
import lombok.Data;

@Data
public class SchemaChangeEvent {

    private Source source;

    private String databaseName;

    private String schemaName;

    private TableChange[] tableChanges;

    private String ddl;

}
