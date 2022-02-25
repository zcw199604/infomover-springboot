/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.model;

import com.info.infomover.entity.Connector;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectorStatus {

    private String name;
    private State connectorStatus;
    private String connectorType;
    private String databaseName;
    private String dbServerName;
    private Long clusterId;
    private Connector.Category category;
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastEventTime;
    private final Map<Long, TaskStatus> taskStates = new HashMap<>();

    public enum State {
        UNASSIGNED,
        RUNNING,
        PAUSED,
        FAILED,
        DESTROYED
    }

    public ConnectorStatus(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDbServerName() {
        return dbServerName;
    }

    public void setDbServerName(String dbServerName) {
        this.dbServerName = dbServerName;
    }

    public State getConnectorStatus() {
        return connectorStatus;
    }

    public void setConnectorStatus(State connectorStatus) {
        this.connectorStatus = connectorStatus;
    }

    public Map<Long, TaskStatus> getTaskStates() {
        return taskStates;
    }

    public TaskStatus getTaskState(Long taskNumber) {
        return taskStates.get(taskNumber);
    }

    public void setTaskState(Long taskNumber, State state, List<String> errors) {
        this.taskStates.put(taskNumber, new TaskStatus(state, errors));
    }
    
    public LocalDateTime getLastEventTime() {
        return lastEventTime;
    }
    
    public void setLastEventTime(LocalDateTime lastEventTime) {
        this.lastEventTime = lastEventTime;
    }
    
    public void setConnectorType(String connectorClassName) {
        switch (connectorClassName) {
            case "io.debezium.connector.postgresql.PostgresConnector":
                this.connectorType = "postgres";
                this.databaseName = "PostgreSQL";
                break;
            case "io.debezium.connector.mongodb.MongoDbConnector":
                this.connectorType = "mongodb";
                this.databaseName = "MongoDB";
                break;
            case "io.debezium.connector.mysql.MySqlConnector":
                this.connectorType = "mysql";
                this.databaseName = "MySQL";
                break;
            case "io.debezium.connector.sqlserver.SqlServerConnector":
                this.connectorType = "sql_server";
                this.databaseName = "SQL Server";
                break;
            case "io.debezium.connector.oracle.OracleConnector":
                this.connectorType = "oracle";
                this.databaseName = "Oracle";
                break;
            case "com.info.connect.jdbc.JdbcSinkConnector":
                this.connectorType = "JdbcSink";
                this.databaseName = "JdbcSink";
                break;
            case "com.info.connect.kafka.sink.KafkaSinkConnector":
                this.connectorType = "KafkaSink";
                this.databaseName = "KafkaSink";
                break;
            default:
                this.connectorType = connectorClassName;
                this.databaseName = "unknown";
                break;
        }
    }

    public String getConnectorType() {
        return connectorType;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Long getClusterId() {
        return clusterId;
    }

    public void setClusterId(Long clusterId) {
        this.clusterId = clusterId;
    }

    public Connector.Category getCategory() {
        return category;
    }

    public void setCategory(Connector.Category category) {
        this.category = category;
    }

    @Override
    public String toString() {
        return "ConnectorStatus{" +
                "connectorType='" + connectorType + '\'' +
                ", databaseName=" + databaseName +
                ", connectorState=" + connectorStatus +
                ", taskStates=" + taskStates +
                ", dbServerName=" + dbServerName +
                '}';
    }
}
