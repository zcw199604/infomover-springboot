package com.info.infomover.loadbalancer;

import com.info.infomover.entity.Cluster;
import com.info.infomover.entity.ConnectDistribute;
import com.info.infomover.quartz.ClusterAlerter;
import com.info.infomover.repository.ClusterRepository;
import com.io.debezium.configserver.rest.client.KafkaConnectClient;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.info.infomover.loadbalancer.AbstractLoadBalancer.clusterMap;

/**
 * create by pengchuan.chen on 2021/10/22
 */
@Component
public class LoadBalancerFactory {
    private static Logger logger = LoggerFactory.getLogger(LoadBalancerFactory.class);

    private static String CONNECT_URI_REBALANCE_RULE = "kafka.connect.uri.rebalance.rule";

    private final String queryStr = "enabled = :enabled";
    private final Map<String, Object> params = new HashMap<String, Object>() {{
        put("enabled", 1);
    }};


    @Autowired
    private ClusterRepository clusterRepository;

    private static AbstractLoadBalancer loadBalancer;
    
    @Autowired
    private ClusterAlerter clusterAlerter;

    @Scheduled(cron = "${connector.cluster.interval.cron}")
    public void checkConnectrStatus() {
        if (clusterMap == null) {
            clusterMap = new ConcurrentHashMap<>();
        }
        List<Cluster> clusterList = clusterRepository.findByEnabled(1);
        if (clusterList == null) {
            return;
        }
        long checkedTime = System.currentTimeMillis();
        for (Cluster cluster : clusterList) {
            String[] urls = cluster.connectorUrl.split(",");
            for (String url : urls) {
                ConnectDistribute connectDistribute = getConnectDistributeStateFromCache(cluster.getId(), url);
                connectDistribute.setCheckedTime(checkedTime);
                try {
                    KafkaConnectClient kafkaConnectClient = RestClientBuilder.newBuilder()
                            .baseUri(new URI(url.trim()))
                            .build(KafkaConnectClient.class);
                    Response response = kafkaConnectClient.getConnectDistributedInfo();
                    if (response.getStatus() == 200) {
                        Map<String, String> responseMap = response.readEntity(Map.class);
                        logger.info("check cluster: {} ConnectDistributed: {} state: {}", cluster.name, url, responseMap);
                        connectDistribute.setStatus(ConnectDistribute.ConnectDistributeStatus.ONLINE);
                        connectDistribute.setKafkaClusterId(responseMap.get("kafka_cluster_id"));
                        connectDistribute.setVersion(responseMap.get("version"));
                        connectDistribute.setCommit(responseMap.get("commit"));
                    } else {
                        logger.warn("can't check cluster: {} ConnectDistributed: {} state: {}", cluster.name, url, response.getStatus());
                        connectDistribute.setStatus(ConnectDistribute.ConnectDistributeStatus.OFFLINE);
                        connectDistribute.setCommit(null);
                        connectDistribute.setVersion(null);
                        connectDistribute.setKafkaClusterId(null);
                    }
                } catch (Exception e) {
                    logger.warn("check cluster: {} ConnectDistributed: {} status failed {}.", cluster.name, url, e.getMessage());
                    connectDistribute.setStatus(ConnectDistribute.ConnectDistributeStatus.OFFLINE);
                    connectDistribute.setCommit(null);
                    connectDistribute.setVersion(null);
                    connectDistribute.setKafkaClusterId(null);
                }
            }
        }
        try {
            //TODO 删除内存中这次没有被检查更新到的connectDistribute信息.
            deleteExpiredUrlConnectDistribute(checkedTime);
            //clusterAlerter.execute();
        } catch (Exception e) {
            logger.warn("delete expired url connectDistribute from cache error.", e);
        }
    }


    /**
     * 从缓存中查找对应clusterId和url的DistributeState信息，如果没找到则新建一个。
     * @param clusterId
     * @param url
     * @return
     */
    private synchronized ConnectDistribute getConnectDistributeStateFromCache(Long clusterId, String url) {
        ConnectDistribute connectDistribute = null;
        if (clusterMap.containsKey(clusterId)) {
            List<ConnectDistribute> distributes = clusterMap.get(clusterId);
            if (distributes == null) {
                distributes = new ArrayList<>();
            }
            for (ConnectDistribute distribute : distributes) {
                if (url.equals(distribute.getUrl())) {
                    connectDistribute = distribute;
                    break;
                }
            }
            if (connectDistribute == null) {
                connectDistribute = new ConnectDistribute(clusterId, url);
                distributes.add(connectDistribute);
            }
        } else {
            connectDistribute = new ConnectDistribute(clusterId, url);
            List<ConnectDistribute> distributes = new ArrayList<>();
            distributes.add(connectDistribute);
            clusterMap.put(clusterId, distributes);
        }
        return connectDistribute;
    }

    /**
     * 从缓存中删除checkedTime小于给定的checkedTime的所有ConnectDistribute信息.
     * @param checkedTime
     */
    private synchronized void deleteExpiredUrlConnectDistribute(Long checkedTime) {
        if (clusterMap == null || clusterMap.size() == 0) {
            return;
        }
        for (Long id : clusterMap.keySet().toArray(new Long[0])) {
            List<ConnectDistribute> needDelete = clusterMap.get(id).stream()
                    .filter(connectDistribute -> connectDistribute.getCheckedTime() < checkedTime)
                    .collect(Collectors.toList());
            if (needDelete != null && needDelete.size() > 0) {
                if (needDelete.size() == clusterMap.get(id).size()) {
                    clusterMap.remove(id);
                } else {
                    clusterMap.get(id).removeAll(needDelete);
                }
                needDelete.forEach(distribute -> {
                    logger.info("removed expired connectDistribute: {} from cache for cluster: {}", distribute.getUrl(), distribute.getCluster());
                });
            }
        }
    }

    public static URI chooseConnectorUrl(Long clusterId) {
        if (loadBalancer == null) {
            String rule = ConfigProvider.getConfig().getConfigValue(CONNECT_URI_REBALANCE_RULE).getValue();
            RebalanceRule rebalanceRule;
            if (StringUtils.isEmpty(rule)) {
                logger.warn("{} is not found in the configuration file," +
                        " random is used here as the default rule of LoadBalancer.", CONNECT_URI_REBALANCE_RULE);
                rebalanceRule = RebalanceRule.RANDOM;
            } else {
                rebalanceRule = RebalanceRule.valueOf(rule.trim().toUpperCase());
            }
            switch (rebalanceRule) {
                case RANDOM:
                    loadBalancer = new RandomLoadBalancer();
                    break;
                case POLLING:
                    loadBalancer = new PollingLoadBalancer();
                    break;
                default:
                    throw new RuntimeException("unsupported rebalanceRule " + rebalanceRule.name());
            }
            logger.info("loaded LoadBalancer: {} succeeded.", loadBalancer.getClass().getCanonicalName());
        }
        logger.debug("using LoadBalancer: {}", loadBalancer.getClass().getCanonicalName());
        try {
            return loadBalancer.choose(clusterId);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<ConnectDistribute> getConnectDistributeForCluster(Long clusterId) {
        if (clusterMap != null) {
            return clusterMap.get(clusterId);
        } else {
            return null;
        }
    }

}
