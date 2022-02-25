/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.model;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.util.Arrays;
import java.util.List;

public class ConnectTaskStatus {

    public Long id;

    @JsonAlias("state")
    public ConnectorStatus.State status;

    @JsonAlias("worker_id")
    public String workerId;

    private List<String> errors;

    @JsonAlias(value = "trace")
    public String getErrors() {
        if (null == errors) {
            return null;
        }
        return String.join("\\n", errors);
    }

    public List<String> getErrorsAsList() {
        return errors;
    }

    @JsonAlias(value = "trace")
    public void setErrors(String errors) {
        if (errors == null) {
            return;
        }
        this.errors = Arrays.asList(errors.split("\\n"));
    }

    @Override
    public String toString() {
        return "ConnectTaskStatus{" +
                "id=" + id +
                ", status=" + status +
                ", workerId='" + workerId + '\'' +
                ", errors='" + errors + '\'' +
                '}';
    }
}
