package com.info.infomover.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = false)
public class ConfigObject extends HashMap<String, Object> {
    private static final long serialVersionUID = -6971430229247641025L;

    public static final String DATASET_ID = "datasetId";
    public static final String DATASET = "dataset";
    public static final String SCHEMA_ID = "schemaId";
    public static final String SCHEMA = "schema";

    public ConfigObject() {
        super();
    }
    
    public static ConfigObject newInstance() {
		return new ConfigObject();
	}

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ConfigObject(Map o) {
        super();
        if (o != null) {
            this.putAll(o);
        }
    }

    public ConfigObject withConfig(String key, Object value) {
        this.put(key, value);
        return this;
    }

	@Override
	public String toString() {
		return "ConfigObject [toString()=" + super.toString() + "]";
	}
}
