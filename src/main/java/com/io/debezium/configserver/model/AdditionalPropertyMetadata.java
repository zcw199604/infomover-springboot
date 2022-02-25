/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.model;

import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class AdditionalPropertyMetadata {

    public String scope;
    public boolean hidden;
    public final boolean isMandatory;
    public final ConnectorProperty.Category category;
    public final List<String> allowedValues;
    public boolean advanced;

    public AdditionalPropertyMetadata(boolean isMandatory, ConnectorProperty.Category category) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = null;
    }


    public AdditionalPropertyMetadata(boolean isMandatory, boolean advanced, ConnectorProperty.Category category) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = null;
        this.advanced = advanced;
    }

    public AdditionalPropertyMetadata(boolean isMandatory, boolean advanced, ConnectorProperty.Category category,boolean hidden) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = null;
        this.advanced = advanced;
        this.hidden = hidden;
    }

    public AdditionalPropertyMetadata(boolean isMandatory, ConnectorProperty.Category category, List<String> allowedValues) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
    }

    public AdditionalPropertyMetadata(boolean isMandatory, boolean advanced, ConnectorProperty.Category category, List<String> allowedValues) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
        this.advanced = advanced;
    }

    public AdditionalPropertyMetadata(boolean isMandatory, ConnectorProperty.Category category, List<String> allowedValues, String scope) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
    }

    public AdditionalPropertyMetadata(boolean isMandatory, ConnectorProperty.Category category, List<String> allowedValues, boolean hidden) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
        this.hidden = hidden;
    }

    public AdditionalPropertyMetadata(boolean isMandatory, ConnectorProperty.Category category, List<String> allowedValues, boolean hidden, boolean advanced) {
        this.isMandatory = isMandatory;
        this.category = category;
        this.allowedValues = allowedValues;
        this.hidden = hidden;
        this.advanced = advanced;
    }
}
