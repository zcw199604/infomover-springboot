/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.rest;

import com.info.infomover.common.Logged;
import com.info.infomover.common.setting.DataSourceLoader;
import com.info.infomover.common.setting.SinkLoader;
import com.info.infomover.common.setting.Transformtions;
import com.info.infomover.common.setting.parser.SettingField;
import com.info.infomover.common.setting.parser.SinkSettingParser;
import com.info.infomover.datasource.AbstractDataSource;
import com.info.infomover.datasource.DataSourceScopeType;
import com.info.infomover.entity.ConfigObject;
import com.info.infomover.entity.Job;
import com.info.infomover.entity.StepDesc;
import com.info.infomover.entity.User;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.sink.KafkaCluster;
import com.info.infomover.util.UserUtil;
import com.io.debezium.configserver.model.*;
import com.io.debezium.configserver.rest.client.KafkaConnectClient;
import com.io.debezium.configserver.rest.client.KafkaConnectClientFactory;
import com.io.debezium.configserver.rest.client.KafkaConnectException;
import com.io.debezium.configserver.rest.model.BadRequestResponse;
import com.io.debezium.configserver.rest.model.ServerError;
import com.io.debezium.configserver.service.ConnectorIntegrator;
import com.io.debezium.configserver.service.StacktraceHelper;
import io.debezium.DebeziumException;
import org.apache.commons.lang3.StringUtils;
import org.jboss.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RequestMapping(ConnectorURIs.API_PREFIX)
@Controller
@Logged
@ResponseBody
public class ConnectorResource {

    private static final Logger LOGGER = Logger.getLogger(ConnectorResource.class);

    private Map<String, ConnectorIntegrator> integrators;

    @Autowired
    private UserRepository userRepository;

    @PostConstruct
    public void init() {

        Map<String, ConnectorIntegrator> integrators = new HashMap<>();
        /*try (ScanResult scanResult = new ClassGraph()
                //.verbose() //If you want to enable logging to stderr
//                .acceptJars("infomover-*.jar")
                .acceptPackages("com.io.debezium")
                //.overrideClassLoaders(Thread.currentThread().getContextClassLoader())
                .scan()) {

            ClassInfoList classInfoList = scanResult.getClassesImplementing(ConnectorIntegrator.class);
            for (ClassInfo classInfo : classInfoList) {
                Class<ConnectorIntegrator> connectorIntegratorClass = classInfo.loadClass(ConnectorIntegrator.class);
                connectorIntegratorClass.
                *//*aClass.get
                integrators.put()*//*
            }
        }
*/
        ServiceLoader<ConnectorIntegrator> load = ServiceLoader.load(ConnectorIntegrator.class);
        for (ConnectorIntegrator connectorIntegrator : load) {
            integrators.put(connectorIntegrator.getConnectorType().id, connectorIntegrator);
        }
        /*ServiceLoader.load(ConnectorIntegrator.class)
                .forEach(integrator -> integrators.put(integrator.getConnectorType().id, integrator));*/

        this.integrators = Collections.unmodifiableMap(integrators);
    }


    @GetMapping(ConnectorURIs.CONNECT_CLUSTERS_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = Map.class)
            ))
    @APIResponse(
            responseCode = "500",
            description = "Exception during Kafka Connect URI validation",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))*/
    public Response getClusters() {
        if (UserUtil.getUserRole().equals((User.Role.User.name()))) {
            User user = userRepository.findByName(UserUtil.getUserName());
            return Response.ok(KafkaConnectClientFactory.getAllKafkaConnectClusters(user.getId())).build();
        } else {
            return Response.ok(KafkaConnectClientFactory.getAllKafkaConnectClusters(null)).build();
        }
    }

    /**
     * 返回所有任务节点的配置
     */
    @GetMapping("/all-connector-types")
    @ResponseBody
    public Response getAllConnectorTypes() {
        List<String> connectorList = new ArrayList<String>(integrators.keySet());
        Map<String, Object> result = new LinkedHashMap<>();

        Map<String, ConnectorType> sources = new HashMap<>();
        for (String type : connectorList) {
            if (StringUtils.isNotEmpty(type)) {
                ConnectorIntegrator integrator = integrators.get(type);
                ConnectorType connectorType = integrator.getConnectorType();
                sources.put(connectorType.id, connectorType);
            }
        }

        result.put("source", sources);
        result.put("transform", Transformtions.propertyList);

        Set<String> allSupportDataSourceType = SinkLoader.getAllSupportSinkType();
        Map<String, Map<String, Object>> settingFields = new HashMap<>();
        for (String type : allSupportDataSourceType) {
            Class<? extends AbstractDataSource> sinkClass = SinkLoader.getSinkClass(type);
            if (sinkClass == null) {
                throw new IllegalArgumentException("Can not found datasource class for type: " + type);
            }
            Map<String, Object> properties = new HashMap<>();
            properties.put("id", type);
            properties.put("scope", "sink");
            properties.put("properties", SinkSettingParser.extractRtcSettingTypeAsList(sinkClass));
            settingFields.put(type, properties);
        }
        result.put("sink", settingFields);

        List<SettingField> kafkaSettingFields = SinkSettingParser.extractRtcSettingTypeAsList(KafkaCluster.class);
        Map<String, Object> kafka = new HashMap<>();

        kafka.put("scope", "cluster");
        kafka.put("id", "kafkaCluster");
        kafka.put("properties", kafkaSettingFields);

        result.put("clusters", new HashMap(){{
            put("cluster", kafka);}
        });

        return Response.ok(result).build();
    }

    /**
     * 根据任务类型返回左边栏图标
     */
    @GetMapping("/collection/all")
    public Response buildModel() {

        Map<String, Map<String, List<StepDesc>>> model = new LinkedHashMap<>();
        model.put(Job.JobType.COLLECT.toString(), this.build(Job.JobType.COLLECT));
        model.put(Job.JobType.SYNC.toString(), this.build(Job.JobType.SYNC));
        return Response.ok(model).build();
    }

    private Map<String, List<StepDesc>> build(Job.JobType jobType) {
        Map<String, List<StepDesc>> model = new LinkedHashMap<>();

        List<StepDesc> source = new ArrayList<>();
        List<String> sourcesScope = DataSourceLoader.getDataSourceScopeType(DataSourceScopeType.Source);
        for (int i = 0; i < sourcesScope.size(); i++) {
            StepDesc stepDesc = new StepDesc();
            stepDesc.setScope("sources");
            stepDesc.setName(sourcesScope.get(i));
            stepDesc.setType(sourcesScope.get(i));
            ConfigObject configObject = new ConfigObject();
            configObject.put("output", new String[]{"output"});
            stepDesc.setUiConfigurations(configObject);
            stepDesc.setOtherConfigurations(new ConfigObject());
            ConfigObject transform = new ConfigObject();
            ConfigObject transform_0 = new ConfigObject();
            transform_0.put("name", "");
            transform_0.put("type", "");
            transform.put("转换_0", transform_0);
            stepDesc.setTransform(transform);

            ConfigObject filter = new ConfigObject();
            ConfigObject filter_0 = new ConfigObject();
            ConfigObject tables = new ConfigObject();
            tables.put("leftFields", new String[]{});
            tables.put("rightFields", new String[]{});

            filter_0.put("database", "");
            filter_0.put("tables", tables);

            filter.put("过滤_0", filter_0);
            stepDesc.setTransform(transform);
            stepDesc.setFilter(filter);

            source.add(stepDesc);
        }
        model.put("数据源", source);

        List<StepDesc> sinks = new ArrayList<>();
        List<String> sinkTypes= null;
        if (Job.JobType.COLLECT == jobType) {
            sinkTypes = Arrays.asList("internal_kafka", "external_kafka");
        } else if (Job.JobType.SYNC == jobType) {
            sinkTypes = DataSourceLoader.getDataSourceScopeType(DataSourceScopeType.Sink);
        }
        for (int i = 0; i < sinkTypes.size(); i++) {
            StepDesc stepDesc = new StepDesc();
            stepDesc.setScope("sinks");
            stepDesc.setName(sinkTypes.get(i));
            stepDesc.setType(Job.JobType.COLLECT == jobType ? "kafka" : sinkTypes.get(i));
            ConfigObject configObject = new ConfigObject();
            configObject.put("input", new String[]{"input"});
            stepDesc.setOtherConfigurations(new ConfigObject());
            stepDesc.setUiConfigurations(configObject);

            ConfigObject transform = new ConfigObject();
            ConfigObject transform_0 = new ConfigObject();
            transform_0.put("name", "");
            transform_0.put("type", "");
            transform.put("转换_0", transform_0);
            stepDesc.setTransform(transform);

            ConfigObject filter = new ConfigObject();
            ConfigObject filter_0 = new ConfigObject();
            filter_0.put("database", "");
            ConfigObject tables = new ConfigObject();
            tables.put("leftFields", new String[]{});
            tables.put("rightFields", new String[]{});

            filter.put("过滤_0", filter_0);
            filter_0.put("tables", tables);
            stepDesc.setTransform(transform);
            stepDesc.setFilter(filter);

            List<ConfigObject> tableMapping = new ArrayList<>();
            stepDesc.setTableMapping(tableMapping);

            sinks.add(stepDesc);
        }
        model.put("输出目标", sinks);
        return model;
    }


    @Path(ConnectorURIs.CONNECTOR_TYPES_ENDPOINT)
    @GET
    @Produces(MediaType.APPLICATION_JSON)

    public List<ConnectorDefinition> getConnectorTypes() {
        return integrators.values()
                .stream()
                .map(ConnectorIntegrator::getConnectorDefinition)
                .collect(Collectors.toList());
    }


    @GetMapping(ConnectorURIs.CONNECTOR_TYPES_ENDPOINT_FOR_CONNECTOR)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ConnectorType.class)
            ))
    @APIResponse(
            responseCode = "400",
            description = "Invalid connector type provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = BadRequestResponse.class)
            ))*/
    public Response getConnectorTypes(@PathVariable("id") String connectorTypeId) {
        if (null == connectorTypeId || "".equals(connectorTypeId)) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("You have to specify a connector type!"))
                    .build();
        }

        ConnectorIntegrator integrator = integrators.get(connectorTypeId);

        if (integrator == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("Unknown connector type: " + connectorTypeId))
                    .build();
        }

        return Response.ok(integrator.getConnectorType()).build();
    }

    private Map<String, String> convertPropertiesToStrings(Map<String, ?> properties) {
        return properties.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue())));
    }


    @PostMapping(ConnectorURIs.CONNECTION_VALIDATION_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ConnectionValidationResult.class)
            ))
    @APIResponse(
            responseCode = "400",
            description = "Invalid connector type provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = BadRequestResponse.class)
            ))*/
    @ResponseBody
    public Response validateConnectionProperties(@PathVariable("id") String connectorTypeId,@RequestBody Map<String, ?> properties) {
        ConnectorIntegrator integrator = integrators.get(connectorTypeId);

        if (integrator == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("Unknown connector type: " + connectorTypeId))
                    .build();
        }

        ConnectionValidationResult validationResult = integrator.validateConnection(convertPropertiesToStrings(properties));

        return Response.ok(validationResult)
                .build();
    }

    @PostMapping(ConnectorURIs.FILTERS_VALIDATION_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = FilterValidationResult.class)
            ))
    @APIResponse(
            responseCode = "400",
            description = "Invalid connector type provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = BadRequestResponse.class)
            ))
    @APIResponse(
            responseCode = "500",
            description = "Exception during validation",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))*/
    @ResponseBody
    public Response validateFilters(@PathVariable("id") String connectorTypeId, @RequestBody Map<String, ?> properties) {
        ConnectorIntegrator integrator = integrators.get(connectorTypeId);

        if (integrator == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("Unknown connector type: " + connectorTypeId))
                    .build();
        }

        try {
            FilterValidationResult validationResult = integrator.validateFilters(convertPropertiesToStrings(properties));

            return Response.ok(validationResult)
                    .build();
        } catch (DebeziumException e) {
            e.printStackTrace();
            return Response.serverError()
                    .entity(new ServerError("Failed to apply table filters", StacktraceHelper.traceAsString(e)))
                    .build();
        }
    }

    @PostMapping(ConnectorURIs.PROPERTIES_VALIDATION_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = PropertiesValidationResult.class)
            ))
    @APIResponse(
            responseCode = "400",
            description = "Invalid connector type provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = BadRequestResponse.class)
            ))*/
    @ResponseBody
    public Response validateConnectorProperties(@PathVariable("id") String connectorTypeId,@RequestBody Map<String, ?> properties) {
        ConnectorIntegrator integrator = integrators.get(connectorTypeId);

        if (integrator == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("Unknown connector type: " + connectorTypeId))
                    .build();
        }

        PropertiesValidationResult validationResult = integrator.validateProperties(convertPropertiesToStrings(properties));

        return Response.ok(validationResult)
                .build();
    }

    @PostMapping(ConnectorURIs.CREATE_CONNECTOR_ENDPOINT)
    @POST
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = FilterValidationResult.class)
            ))
    @APIResponse(
            responseCode = "400",
            description = "Missing or invalid properties or invalid connector type provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = BadRequestResponse.class)
            ))
    @APIResponse(
            responseCode = "500",
            description = "Exception during Kafka Connect URI validation",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))
    @APIResponse(
            responseCode = "503",
            description = "Exception while trying to connect to the selected Kafka Connect cluster",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))*/
    @ResponseBody
    public Response createConnector(
            @PathVariable("cluster") int cluster,
            @PathVariable("connector-type-id") String connectorTypeId,
            @RequestBody ConnectConnectorConfigResponse kafkaConnectConfig
    ) throws KafkaConnectClientException, KafkaConnectException {
        if (kafkaConnectConfig.getConfig() == null || kafkaConnectConfig.getConfig().isEmpty()) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("Connector \"config\" property is not set!"))
                    .build();
        }

        if (null == kafkaConnectConfig.getName() || kafkaConnectConfig.getName().isBlank()) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("Connector \"name\" property is not set!"))
                    .build();
        }

        ConnectorIntegrator integrator = integrators.get(connectorTypeId);
        if (integrator == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(new BadRequestResponse("Unknown connector type: " + connectorTypeId))
                    .build();
        }

        PropertiesValidationResult validationResult = integrator.validateProperties(kafkaConnectConfig.getConfig());

        if (validationResult.status == PropertiesValidationResult.Status.INVALID) {
            return Response.status(Status.BAD_REQUEST).entity(validationResult).build();
        }

        KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);

        kafkaConnectConfig.getConfig().put("connector.class", integrator.getConnectorType().className);

        String result;
        LOGGER.debug("Sending valid connector config: " + kafkaConnectConfig.getConfig());
        try {
            result = kafkaConnectClient.createConnector(kafkaConnectConfig);
        } catch (ProcessingException | IOException e) {
            throw new KafkaConnectClientException(e);
        }
        LOGGER.debug("Kafka Connect response: " + result);

        return Response.ok(result).build();
    }

    @GetMapping(ConnectorURIs.LIST_CONNECTORS_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ConnectorStatus.class, type = SchemaType.ARRAY)
            ))
    @APIResponse(
            responseCode = "500",
            description = "Exception during Kafka Connect URI validation",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))
    @APIResponse(
            responseCode = "503",
            description = "Exception while trying to connect to the selected Kafka Connect cluster",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))*/
    @ResponseBody
    public Response listConnectors(@PathVariable("cluster") int cluster)
            throws KafkaConnectClientException, KafkaConnectException {
        KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);

        List<String> activeConnectors;
        try {
            activeConnectors = kafkaConnectClient.listConnectors();
        } catch (ProcessingException | IOException e) {
            throw new KafkaConnectClientException(e);
        }

        LOGGER.debug("Kafka Connect response: " + activeConnectors);

        List<ConnectorStatus> connectorData = Collections.emptyList();
        if (!activeConnectors.isEmpty()) {
            connectorData = activeConnectors.stream().map(
                    connectorName -> {
                        try {
                            var connectorInfo = kafkaConnectClient.getConnectorInfo(connectorName);
                            String connectorType = connectorInfo.getConfig().get("connector.class");
//                                    if (!connectorType.startsWith("io.debezium")) {
//                                        return null;
//                                    }
                            LOGGER.debug("Kafka Connect connector status details: " + connectorInfo);
                            var connectorStatus = kafkaConnectClient.getConnectorStatus(connectorName);
                            var connectorState = new ConnectorStatus(connectorName);
                            connectorState.setConnectorType(connectorType);
                            connectorState.setConnectorStatus(connectorStatus.connectorStatus.status);
                            connectorState.setDbServerName(connectorInfo.getConfig().get("database.server.name"));
                            connectorStatus.taskStates.forEach(
                                    taskStatus -> connectorState.setTaskState(
                                            taskStatus.id,
                                            taskStatus.status,
                                            (taskStatus.getErrorsAsList() != null
                                                    ? taskStatus.getErrorsAsList()
                                                    : null
                                            )
                                    ));
                            return connectorState;
                        } catch (ProcessingException | IOException e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                        return null;
                    }).collect(Collectors.toList());
        }

        LOGGER.debug("Registered Connectors: " + connectorData);

        return Response.ok(connectorData).build();
    }

    @DeleteMapping(ConnectorURIs.MANAGE_CONNECTORS_ENDPOINT)
    /*@APIResponse(
            responseCode = "204",
            content = @Content(mediaType = MediaType.APPLICATION_JSON))
    @APIResponse(
            responseCode = "404",
            description = "Connector with specified name not found")
    @APIResponse(
            responseCode = "500",
            description = "Exception during Kafka Connect URI validation",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))
    @APIResponse(
            responseCode = "503",
            description = "Exception while trying to connect to the selected Kafka Connect cluster",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ServerError.class)
            ))*/
    @ResponseBody
    public Response deleteConnector(
            @PathVariable("cluster") int cluster,
            @PathVariable("connector-name") String connectorName
    ) throws KafkaConnectClientException, KafkaConnectException {
        KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);

        Response deleteResponse;
        try {
            Response originalKafkaConnectResponse = kafkaConnectClient.deleteConnector(connectorName);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Kafka Connect response: " + originalKafkaConnectResponse.readEntity(String.class));
            }
            deleteResponse = Response.fromResponse(originalKafkaConnectResponse).type(MediaType.APPLICATION_JSON).build();
            originalKafkaConnectResponse.close();
        } catch (ProcessingException | IOException e) {
            throw new KafkaConnectClientException(e);
        }
        return deleteResponse;
    }

}
