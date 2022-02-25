/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.service;

import com.io.debezium.configserver.model.*;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import com.io.debezium.configserver.model.*;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnector;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public abstract class ConnectorIntegratorBase implements ConnectorIntegrator {
    private static final Logger logger = LoggerFactory.getLogger(ConnectorIntegratorBase.class);

    protected abstract ConnectorDescriptor getConnectorDescriptor();

    protected abstract SourceConnector getConnector();

    private static Properties props;

    static {
        logger.info("开始加载properties文件内容.......");
        props = new Properties();
        InputStream in = null;
        try {
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream("zh_connector.properties");
            props.load(new InputStreamReader(in,"UTF-8"));
        } catch (FileNotFoundException e) {
            logger.error("zh_connector.properties文件未找到");
        } catch (IOException e) {
            logger.error("出现IOException");
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
                logger.error("jdbc.properties文件流关闭出现异常");
            }
        }
        logger.info("加载properties文件内容完成...........");
        logger.debug("properties文件内容：" + props);
    }

    public static List<String> enumArrayToList(EnumeratedValue[] input) {
        List<String> result = new ArrayList<>();
        for (EnumeratedValue value : input) {
            result.add(value.getValue());
        }
        return result;
    }

    @Override
    public Map<String, AdditionalPropertyMetadata> allPropertiesWithAdditionalMetadata() {
        AdditionalPropertyMetadata defaultMetadata = new AdditionalPropertyMetadata(false, ConnectorProperty.Category.FILTERS);
        return getConnector().config().configKeys().values()
                .stream().collect(Collectors.toMap(configKey -> configKey.name, configKey -> defaultMetadata));
    }

    @Override
    public ConnectorDefinition getConnectorDefinition() {
        ConnectorDescriptor descriptor = getConnectorDescriptor();
        SourceConnector instance = getConnector();

        return new ConnectorDefinition(
                descriptor.id,
                instance.getClass().getName(),
                descriptor.name,
                instance.version(),
                descriptor.scope,
                descriptor.enabled

        );
    }

    @Override
    public ConnectorType getConnectorType() {
        ConnectorDescriptor descriptor = getConnectorDescriptor();
        SourceConnector instance = getConnector();
        Map<String, ConnectorProperty> properties = instance.config()
                .configKeys()
                .values()
                .stream()
                .filter(configKey -> allPropertiesWithAdditionalMetadata().containsKey(configKey.name))
                .filter(property -> !property.name.startsWith("internal"))
                .map(this::toConnectorProperty)
                .collect(Collectors.toMap(connectorProperty -> connectorProperty.name, connectorProperty -> connectorProperty));

        // apply sorting of properties provided by {@link #allPropertiesWithAdditionalMetadata()}
        ArrayList<ConnectorProperty> sortedProperties = allPropertiesWithAdditionalMetadata().keySet()
                .stream().map(properties::get).collect(Collectors.toCollection(ArrayList::new));
        ConnectorProperty tasksMax = new ConnectorProperty("tasks.max", "tasks.max", "tasks max",
                ConnectorProperty.Type.STRING, "1", false, ConnectorProperty.Category.CONNECTION, null, true, false);
        //添加新字段
        ConnectorProperty valueConverter = new ConnectorProperty("value.converter", "value converter", "value converter",
                ConnectorProperty.Type.CLASS, "org.apache.kafka.connect.json.JsonConverter", false, ConnectorProperty.Category.CONNECTION,
                Arrays.asList(new String[]{"org.apache.kafka.connect.json.JsonConverter"}), false, true);
        ConnectorProperty valueConverterSchemaEnable = new ConnectorProperty("value.converter.schemas.enable", "value schema show", "value.converter.schemas.enable",
                ConnectorProperty.Type.BOOLEAN, true, false, ConnectorProperty.Category.CONNECTION, Arrays.asList(new String[]{"true", "false"}), false, true);
        ConnectorProperty keyConverter = new ConnectorProperty("key.converter", "key converter", "key converter",
                ConnectorProperty.Type.CLASS, "org.apache.kafka.connect.json.JsonConverter", false, ConnectorProperty.Category.CONNECTION,
                Arrays.asList(new String[]{"org.apache.kafka.connect.json.JsonConverter"}), false, true);
        ConnectorProperty keyConverterSchemaEnable = new ConnectorProperty("key.converter.schemas.enable", "key schema show", "key.converter.schemas.enable",
                ConnectorProperty.Type.BOOLEAN, true, false, ConnectorProperty.Category.CONNECTION, Arrays.asList(new String[]{"true", "false"}), false, true);


        ConnectorProperty dataSourceId = new ConnectorProperty("datasource.id", "数据源ID", "数据源ID",
                ConnectorProperty.Type.STRING, null, false, ConnectorProperty.Category.CONNECTION, null,true, false);

        ConnectorProperty dataSourceName = new ConnectorProperty("datasource.name", "数据源名称", "数据源名称",
                ConnectorProperty.Type.STRING, null, false, ConnectorProperty.Category.CONNECTION, null,true, false);

        ConnectorProperty decimalHandling = new ConnectorProperty("decimal.handling.mode", "如何处理decimal", "如何处理decimal",
                ConnectorProperty.Type.STRING, "string", false, ConnectorProperty.Category.CONNECTION, Arrays.asList(new String[]{"string", "precise", "double"}), false, true);

        ConnectorProperty replicationFactor = new ConnectorProperty("topic.creation.default.replication.factor",
                "输出topic副本个数", "输出topic副本个数",
                ConnectorProperty.Type.INT, "1", false, ConnectorProperty.Category.CONNECTION, null,false, true);

        ConnectorProperty partitions = new ConnectorProperty("topic.creation.default.partitions",
                "分区数", "分区数",
                ConnectorProperty.Type.INT, "1", false, ConnectorProperty.Category.CONNECTION, null,false, true);

        ConnectorProperty cleanupPolicy = new ConnectorProperty("topic.creation.default.cleanup.policy",
                "删除策略", "删除策略",
                ConnectorProperty.Type.INT, "delete", false, ConnectorProperty.Category.CONNECTION, Arrays.asList(new String[]{"delete", "compact"}), false, true);

        ConnectorProperty compressionType = new ConnectorProperty("topic.creation.default.compression.type",
                "压缩方式", "压缩方式",
                ConnectorProperty.Type.STRING, "producer", false, ConnectorProperty.Category.CONNECTION, Arrays.asList(new String[]{"producer","uncompressed","gzip", "snappy"}),false, true);

        ConnectorProperty retentionMs = new ConnectorProperty("topic.creation.default.retention.ms",
                "保留时间", "保留时间",
                ConnectorProperty.Type.STRING, "-1", false, ConnectorProperty.Category.CONNECTION, null,false, true);

        ConnectorProperty timePrecisionMode = new ConnectorProperty("time.precision.mode",
                "时间精度模式", "时间精度模式",
                ConnectorProperty.Type.STRING, "connect", false, ConnectorProperty.Category.CONNECTION, Arrays.asList(new String[]{"connect","adaptive", "adaptive_time_microseconds"}), false, true);

        sortedProperties.add(1,decimalHandling);
        sortedProperties.add(2,tasksMax);
        sortedProperties.add(3,valueConverterSchemaEnable);
        sortedProperties.add(valueConverter);
        sortedProperties.add(keyConverter);
        sortedProperties.add(keyConverterSchemaEnable);

        sortedProperties.add(dataSourceId);
        sortedProperties.add(dataSourceName);

        sortedProperties.add(replicationFactor);
        sortedProperties.add(partitions);
        sortedProperties.add(cleanupPolicy);
        sortedProperties.add(compressionType);
        sortedProperties.add(retentionMs);

        sortedProperties.add(timePrecisionMode);

        if (instance instanceof OracleConnector) {
            ConnectorProperty schemaInclude = new ConnectorProperty(OracleConnectorConfig.SCHEMA_INCLUDE_LIST.name(), OracleConnectorConfig.SCHEMA_INCLUDE_LIST.displayName(), "schema.include.list", ConnectorProperty.Type.STRING, null, false, ConnectorProperty.Category.FILTERS, null);
            ConnectorProperty schemaExclude = new ConnectorProperty(OracleConnectorConfig.SCHEMA_EXCLUDE_LIST.name(), OracleConnectorConfig.SCHEMA_EXCLUDE_LIST.displayName(), "schema.exclude.list", ConnectorProperty.Type.STRING, null, false, ConnectorProperty.Category.FILTERS, null);
            List<Integer> indexList = new ArrayList<>();
            for (int i = 0; i < sortedProperties.size(); i++) {
                if (sortedProperties.get(i) == null) {
                    indexList.add(i);
                }
            }
            if (indexList.size() >= 2) {
                sortedProperties.set(indexList.get(0), schemaInclude);
                sortedProperties.set(indexList.get(1), schemaExclude);
            } else {
                sortedProperties.add(schemaInclude);
                sortedProperties.add(schemaExclude);
            }
        }

        String type = null;
        if (instance instanceof OracleConnector) {
            ConnectorProperty oracleConnectorClass = new ConnectorProperty("connector.class", "connector.class", "connector.class",
                    ConnectorProperty.Type.STRING, "io.debezium.connector.oracle.OracleConnector", false, ConnectorProperty.Category.CONNECTION, null,true, false);
            sortedProperties.add(oracleConnectorClass);
            type = "oracle";
        } else if (instance instanceof MySqlConnector) {
            ConnectorProperty mysqlConnectorClass = new ConnectorProperty("connector.class", "connector.class", "connector.class",
                    ConnectorProperty.Type.STRING, "io.debezium.connector.mysql.MySqlConnector", false, ConnectorProperty.Category.CONNECTION, null,true, false);
            sortedProperties.add(mysqlConnectorClass);
            type = "mysql";
        } else if (instance instanceof PostgresConnector) {
            ConnectorProperty postgresConnectorClass = new ConnectorProperty("connector.class", "connector.class", "connector.class",
                    ConnectorProperty.Type.STRING, "io.debezium.connector.postgresql.PostgresConnector", false, ConnectorProperty.Category.CONNECTION, null,true, false);
            sortedProperties.add(postgresConnectorClass);
            type = "postgres";
        }

        for (ConnectorProperty property : sortedProperties) {
            if (property != null) {
                property.zh_description = props.getProperty(type + "."+ property.name + ".description");
                property.zh_displayName = props.getProperty(type + "."+ property.name + ".displayName");
                if(props.containsKey(type + "."+ property.name + ".allowedValues")){
                    property.allowedValues = Arrays.asList(props.getProperty(type + "."+ property.name + ".allowedValues").split(","));
                }
                if(props.containsKey(type + "."+ property.name + ".hidden")){
                    property.hidden = Boolean.getBoolean(props.getProperty(type + "."+ property.name + ".hidden"));
                }
                if(props.containsKey(type + "."+ property.name + ".defaultValue.collect")){
                    property.collValue = props.get(type + "."+ property.name + ".defaultValue.collect");
                }else{
                    property.collValue = property.defaultValue;
                }
                if(props.containsKey(type + "."+ property.name + ".defaultValue.sync")){
                    property.syncValue = props.get(type + "."+ property.name + ".defaultValue.sync");
                }else{
                    property.syncValue = property.defaultValue;
                }
            }
        }

        return new ConnectorType(
                descriptor.id,
                instance.getClass().getName(),
                descriptor.name,
                instance.version(),
                descriptor.scope,
                descriptor.enabled,
                sortedProperties
        );
    }

    @Override
    public ConnectionValidationResult validateConnection(Map<String, String> properties) {
        SourceConnector instance = getConnector();

        try {
            Config result = instance.validate(properties);
            List<PropertyValidationResult> propertyResults = toPropertyValidationResults(result);

            return propertyResults.isEmpty() ? ConnectionValidationResult.valid() : ConnectionValidationResult.invalid(propertyResults);
        } catch (Exception e) {
            return ConnectionValidationResult.invalid(Collections.emptyList(), Collections.singletonList(new GenericValidationResult(e.getMessage(), traceAsString(e))));
        }
    }

    @Override
    public PropertiesValidationResult validateProperties(Map<String, String> properties) {
        List<Field> fields = new ArrayList<>();
        getAllConnectorFields().forEach(field -> {
            if (properties.containsKey(field.name())) {
                fields.add(field);
            }
        });

        Configuration config = Configuration.from(properties);
        Map<String, ConfigValue> results = config.validate(Field.setOf(fields));
        Config result = new Config(new ArrayList<>(results.values()));

        List<PropertyValidationResult> propertyResults = toPropertyValidationResults(result);

        return propertyResults.isEmpty() ? PropertiesValidationResult.valid() : PropertiesValidationResult.invalid(propertyResults);
    }

    private List<PropertyValidationResult> toPropertyValidationResults(Config result) {
        return result.configValues()
                .stream()
                .filter(cv -> !cv.errorMessages().isEmpty())
                .filter(cv -> !cv.errorMessages().get(0).equals(cv.name() + " is referred in the dependents, but not defined."))
                .map(cv -> new PropertyValidationResult(cv.name(), cv.errorMessages().get(0)))
                .collect(Collectors.toList());
    }

    private String traceAsString(Exception e) {
        return e.getStackTrace() != null && e.getStackTrace().length > 0 ? Arrays.toString(e.getStackTrace()) : null;
    }

    private ConnectorProperty toConnectorProperty(ConfigKey configKey) {
        boolean isMandatory = false;
        boolean hidden = false;
        boolean advanced = false;
        ConnectorProperty.Category category = ConnectorProperty.Category.CONNECTOR;
        List<String> allowedValues = null;

        AdditionalPropertyMetadata additionalMetadata = allPropertiesWithAdditionalMetadata().get(configKey.name);
        if (additionalMetadata != null) {
            isMandatory = additionalMetadata.isMandatory;
            category = additionalMetadata.category;
            allowedValues = additionalMetadata.allowedValues;
            hidden = additionalMetadata.hidden;
            advanced = additionalMetadata.advanced;
        }

        return new ConnectorProperty(
                configKey.name,
                configKey.displayName,
                configKey.documentation,
                toConnectorPropertyType(configKey.type()),
                configKey.defaultValue,
                isMandatory,
                category,
                allowedValues,
                hidden,
                advanced
        );
    }

    private ConnectorProperty.Type toConnectorPropertyType(ConfigDef.Type type) {
        switch (type) {
            case BOOLEAN:
                return ConnectorProperty.Type.BOOLEAN;
            case CLASS:
                return ConnectorProperty.Type.CLASS;
            case DOUBLE:
                return ConnectorProperty.Type.DOUBLE;
            case INT:
                return ConnectorProperty.Type.INT;
            case LIST:
                return ConnectorProperty.Type.LIST;
            case LONG:
                return ConnectorProperty.Type.LONG;
            case PASSWORD:
                return ConnectorProperty.Type.PASSWORD;
            case SHORT:
                return ConnectorProperty.Type.SHORT;
            case STRING:
                return ConnectorProperty.Type.STRING;
            default:
                throw new IllegalArgumentException("Unsupported property type: " + type);
        }
    }

    public static class ConnectorDescriptor {
        public String id;
        public String name;
        public boolean enabled;
        public String scope;

        public ConnectorDescriptor(String id, String name,String scope, boolean enabled) {
            this.id = id;
            this.name = name;
            this.scope = scope;
            this.enabled = enabled;
        }
    }
}
