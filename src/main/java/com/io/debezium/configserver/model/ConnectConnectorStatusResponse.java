/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ConnectConnectorStatusResponse {

    public enum Type {
        sink,
        source
    }


    @JsonAlias("connector")
    public ConnectConnectorStatus connectorStatus;

    public String name;

    @JsonAlias("tasks")
    public List<ConnectTaskStatus> taskStates;

    @JsonAlias("type")
    public Type connectorType;

    @Override
    public String toString() {
        return "ConnectConnectorStatusResponse{" +
                "name='" + name + '\'' +
                ", connectorStatus=" + connectorStatus +
                ", taskStates=" + taskStates +
                ", connectorType=" + connectorType +
                '}';
    }
}
