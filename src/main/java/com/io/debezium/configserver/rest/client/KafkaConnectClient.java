/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.rest.client;

import com.io.debezium.configserver.model.ConnectConnectorConfigResponse;
import com.io.debezium.configserver.model.ConnectConnectorStatusResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Map;


public interface KafkaConnectClient extends AutoCloseable{

    @POST
    @Path("/connectors")
    @Produces({"application/json"})
    String createConnector(ConnectConnectorConfigResponse configuration) throws ProcessingException, IOException;

    @GET
    @Path("/connectors")
    @Produces("application/json")
    List<String> listConnectors() throws ProcessingException, IOException;

    @GET
    @Path("/connectors/{connector-name}")
    @Produces("application/json")
    ConnectConnectorConfigResponse getConnectorInfo(@PathParam("connector-name") String connectorName) throws ProcessingException, IOException;

    @GET
    @Path("/connectors/{connector-name}/status")
    @Produces("application/json")
    ConnectConnectorStatusResponse getConnectorStatus(@PathParam("connector-name") String connectorName) throws ProcessingException, IOException;

    @DELETE
    @Path("/connectors/{connector-name}")
    Response deleteConnector(@PathParam("connector-name") String connectorName) throws ProcessingException, IOException;

    @PUT
    @Path("/connectors/{connector-name}/pause")
    @Produces("application/json")
    Response pauseConnector(@PathParam("connector-name") String connectorName) throws ProcessingException, IOException;

    @PUT
    @Path("/connectors/{connector-name}/resume")
    @Produces("application/json")
    Response resumeConnector(@PathParam("connector-name") String connectorName) throws ProcessingException, IOException;

    @POST
    @Path("/connectors/{connector-name}/restart")
    @Consumes("application/json")
    @Produces("application/json")
    Response restartConnector(@PathParam("connector-name") String connectorName) throws ProcessingException, IOException;

    @POST
    @Path("/connectors/{connector-name}/tasks/{task-number}/restart")
    @Consumes("application/json")
    @Produces("application/json")
    Response restartConnectorTask(@PathParam("connector-name") String connectorName, @PathParam("task-number") int taskNumber) throws ProcessingException, IOException;

    @PUT
    @Path("/connectors/{connector-name}/config")
    @Produces("application/json")
    Response updateConnectorConfig(@PathParam("connector-name") String connectorName,Map<String, String> config) throws ProcessingException, IOException;

    @GET
    @Path("/")
    @Produces("application/json")
    Response getConnectDistributedInfo() throws ProcessingException, IOException;
    
    @GET
    @Path("/connectors")
    @Produces("application/json")
    Response listWithStatus(@QueryParam("expand") @DefaultValue("status") String expand) throws ProcessingException, IOException;
}
