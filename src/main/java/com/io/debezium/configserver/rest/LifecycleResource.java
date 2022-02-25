/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.rest;

import com.info.infomover.entity.Connector;
import com.info.infomover.entity.Job;
import com.info.infomover.repository.ConnectorRepository;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.service.ConnectorService;
import com.info.infomover.util.CheckUtil;
import com.io.debezium.configserver.model.ConnectConnectorStatusResponse;
import com.io.debezium.configserver.model.ConnectorStatus;
import com.io.debezium.configserver.rest.client.KafkaConnectClient;
import com.io.debezium.configserver.rest.client.KafkaConnectClientFactory;
import com.io.debezium.configserver.rest.client.KafkaConnectException;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.transaction.Transactional;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

//@Path(ConnectorURIs.API_PREFIX)
@RequestMapping(ConnectorURIs.API_PREFIX)
@ResponseBody
public class LifecycleResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LifecycleResource.class);


    @Autowired
    private JPAQueryFactory queryFactory;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    private ConnectorRepository connectorRepository;

    @PutMapping(ConnectorURIs.CONNECTOR_PAUSE_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ConnectionValidationResult.class)
            ))
    @APIResponse(
            responseCode = "404",
            description = "Invalid connector name provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = NotFoundResponse.class)
            ))
    @APIResponse(
            responseCode = "500",
            description = "Exception during action",
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
    @Transactional
    public Response pauseConnector(
            @PathVariable("cluster") int cluster,
            @PathVariable("connectorname") String connectorName
    ) throws KafkaConnectClientException, KafkaConnectException {
        KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);
        Connector byName = connectorRepository.findByName(connectorName);
        CheckUtil.checkTrue(byName == null,"connector name is error");
        Response result;
        try {
            result = kafkaConnectClient.pauseConnector(connectorName);

            Long jobId = byName.getJobId();
            Connector byName1 = connectorRepository.findByName(connectorName);
            byName1.setConnectorStatus(ConnectorStatus.State.PAUSED);
            connectorRepository.saveAndFlush(byName1);
            //int rows = Connector.update("connectorStatus = ?1 where name = ?2", ConnectorStatus.State.PAUSED, connectorName);
            LOGGER.info("update {} connectorName -> {} rows :{} ", connectorName, ConnectorStatus.State.PAUSED, 1);
            Job byId = jobRepository.findById(jobId).get();
            // 当任务是暂停 需要更新任务状态为已发布
            if (byId.getDeployStatus() == Job.DeployStatus.DEPLOYED) {
                long count = connectorService.countByJobIdAndConnectorStatus(jobId, ConnectorStatus.State.PAUSED);
                // 当所有connect状态都为暂停时,需要修改任务状态为暂停
                if (count == 0) {
                    byId.setDeployStatus(Job.DeployStatus.PAUSED);
                    jobRepository.saveAndFlush(byId);
                    LOGGER.info("job [{}] all connector is paused , update deployStatus -> [{}]", jobId, Job.DeployStatus.PAUSED);
                }
            }
        }
        catch (ProcessingException | IOException e) {
            throw new KafkaConnectClientException(e);
        }
        LOGGER.debug("Kafka Connect response: " + result.readEntity(String.class));

        return result;
    }

    private boolean checkAllConnectStatus(KafkaConnectClient kafkaConnectClient, List<String> connectorNames, int clusterId, ConnectorStatus.State state) {
        for (String connectorName : connectorNames) {
            ConnectorStatus connectorState = new ConnectorStatus(connectorName);
            try {
                ConnectConnectorStatusResponse connectorStatus = kafkaConnectClient.getConnectorStatus(connectorName);
                connectorState.setConnectorStatus(connectorStatus.connectorStatus.status);
                ConnectorStatus.State status = connectorStatus.connectorStatus.status;
                if (status != state) {
                    return false;
                }
            } catch (IOException e) {
                LOGGER.error("getConnecorStatus {} error on cluster {}", connectorName, clusterId, e);
            }
        }
        return true;
    }

    private List<String> parserError(List<String> errorList){
        if(errorList == null || errorList.size() == 0){
            return null;
        }

        List<String> result = errorList.stream().filter(s -> s.startsWith("Caused by:")).collect(Collectors.toList());
        if(result == null || result.size() == 0){
            String exception = errorList.get(0);
            result = new ArrayList<>();
            result.add(exception);
            return result;
        }else{
            return result;
        }
    }

    @PutMapping(ConnectorURIs.CONNECTOR_RESUME_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ConnectionValidationResult.class)
            ))
    @APIResponse(
            responseCode = "404",
            description = "Invalid connector name provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = NotFoundResponse.class)
            ))
    @APIResponse(
            responseCode = "500",
            description = "Exception during action",
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
    @Transactional
    public Response resumeConnector(
            @PathVariable("cluster") int cluster,
            @PathVariable("connectorname") String connectorName
    ) throws KafkaConnectClientException, KafkaConnectException {
        KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);
        Connector byName = connectorRepository.findByName(connectorName);
        CheckUtil.checkTrue(byName == null,"connector name is error");
        Response result;
        try {
            result = kafkaConnectClient.resumeConnector(connectorName);
            Connector byName1 = connectorRepository.findByName(connectorName);
            byName1.setConnectorStatus(ConnectorStatus.State.PAUSED);
            connectorRepository.saveAndFlush(byName1);
            //int rows = Connector.update("connectorStatus = ?1 where name = ?2", ConnectorStatus.State.RUNNING, connectorName);
            LOGGER.info("update {} connectorName -> {} rows :{} ", connectorName, ConnectorStatus.State.RUNNING, 1);
            Long jobId = byName.getJobId();
            Job byId = jobRepository.findById(jobId).get();
            // 当任务是暂停 需要更新任务状态为已发布
            if (byId.getDeployStatus() == Job.DeployStatus.PAUSED) {
                byId.setDeployStatus(Job.DeployStatus.DEPLOYED);
                jobRepository.saveAndFlush(byId);
                LOGGER.info("job [{}] resumeConnector , update deployStatus -> [{}]", jobId, Job.DeployStatus.DEPLOYED);
            }

        }
        catch (ProcessingException | IOException e) {
            throw new KafkaConnectClientException(e);
        }
        LOGGER.debug("Kafka Connect response: " + result.readEntity(String.class));

        return result;
    }

    @PostMapping(ConnectorURIs.CONNECTOR_RESTART_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ConnectionValidationResult.class)
            ))
    @APIResponse(
            responseCode = "404",
            description = "Invalid connector name provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = NotFoundResponse.class)
            ))
    @APIResponse(
            responseCode = "409",
            description = "Could not restart while a rebelancing is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON))
    @APIResponse(
            responseCode = "500",
            description = "Exception during action",
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
    public Response restartConnector(
            @PathVariable("cluster") int cluster,
            @PathVariable("connectorname") String connectorName
    ) throws KafkaConnectClientException, KafkaConnectException {
        KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);

        Response result;
        try {
            result = kafkaConnectClient.restartConnector(connectorName);
        }
        catch (ProcessingException | IOException e) {
            throw new KafkaConnectClientException(e);
        }
        LOGGER.debug("Kafka Connect response: " + result.readEntity(String.class));

        return Response.fromResponse(result).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

    @PostMapping(ConnectorURIs.CONNECTOR_TASK_RESTART_ENDPOINT)
    /*@APIResponse(
            responseCode = "200",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = ConnectionValidationResult.class)
            ))
    @APIResponse(
            responseCode = "404",
            description = "Invalid connector name provided",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = NotFoundResponse.class)
            ))
    @APIResponse(
            responseCode = "409",
            description = "Could not restart while a rebelancing is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON))
    @APIResponse(
            responseCode = "500",
            description = "Exception during action",
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
    public Response restartTask(
            @PathVariable("cluster") int cluster,
            @PathVariable("connectorname") String connectorName,
            @PathVariable("tasknumber") int taskNumber
    ) throws KafkaConnectClientException, KafkaConnectException {
        KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);

        Response result;
        try {
            result = kafkaConnectClient.restartConnectorTask(connectorName, taskNumber);
        }
        catch (ProcessingException | IOException e) {
            throw new KafkaConnectClientException(e);
        }
        LOGGER.debug("Kafka Connect response: " + result.readEntity(String.class));

        return Response.fromResponse(result).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

}
