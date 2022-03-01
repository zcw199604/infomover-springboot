/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
public class ConnectConnectorStatus {

    @JsonAlias("state")
    public ConnectorStatus.State status;

    @JsonAlias("worker_id")
    public String workerId;

    @Override
    public String toString() {
        return "ConnectConnectorStatus{" +
                "status=" + status +
                ", workerId='" + workerId + '\'' +
                '}';
    }
}
