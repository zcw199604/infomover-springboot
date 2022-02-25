/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.io.debezium.configserver.rest.client;

import com.info.infomover.entity.Cluster;
import com.info.infomover.loadbalancer.LoadBalancerFactory;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaConnectClientFactory {

    private static Logger logger = LoggerFactory.getLogger(KafkaConnectClientFactory.class);

    public static Map<Long, String> getAllKafkaConnectClusters(Long creatorId) {
        List<String> query = new ArrayList<>();
        Map<String, Object> params = new HashMap<>();
        if (creatorId != null) {
            query.add("creatorId = :creatorId");
            params.put("creatorId", creatorId);
        }
        query.add("enabled = :enabled");
        params.put("enabled", 1);
        String queryStr = query.stream().collect(Collectors.joining(" AND "));
        //TODO ----
        //List<Cluster> clusterList = Cluster.list(queryStr, Sort.by("updateTime", Sort.Direction.Descending), params);
        List<Cluster> clusterList = new ArrayList<>();
        Map<Long, String> clusterMap = new LinkedHashMap<>();
        for(Cluster cluster : clusterList){
            clusterMap.put(cluster.getId(), cluster.name);
        }
        return clusterMap;
    }

    public static KafkaConnectClient getClient(int cluster) throws KafkaConnectException {
        return getClient((long) cluster);
    }
    
    public static KafkaConnectClient getClient(Long cluster) throws KafkaConnectException {
        URI kafkaConnectURI;
        KafkaConnectClient kafkaConnectClient;
        try {
            kafkaConnectURI = LoadBalancerFactory.chooseConnectorUrl(cluster);
            logger.debug("chose kafkaConnectURI for cluster {} : {}", cluster, kafkaConnectURI);
            kafkaConnectClient = RestClientBuilder.newBuilder()
                    .baseUri(kafkaConnectURI)
                    .build(KafkaConnectClient.class);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new KafkaConnectException(e.getMessage(), e);
        }
        return kafkaConnectClient;
    }

}
