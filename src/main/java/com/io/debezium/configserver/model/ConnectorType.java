/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.model;

import java.util.List;

public class ConnectorType extends ConnectorDefinition {

    public List<ConnectorProperty> properties;

    public ConnectorType() {
    }


    public ConnectorType(String id, String className, String displayName, String version,String scope, boolean enabled, List<ConnectorProperty> properties) {
        super(id, className, displayName, version,scope, enabled);
        this.properties = properties;
    }
}
