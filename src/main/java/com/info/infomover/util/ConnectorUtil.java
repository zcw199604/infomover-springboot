package com.info.infomover.util;

import com.info.baymax.common.utils.JsonUtils;
import com.io.debezium.configserver.model.ConnectConnectorStatusResponse;
import com.io.debezium.configserver.model.ConnectTaskStatus;
import com.io.debezium.configserver.model.ConnectorStatus;
import com.io.debezium.configserver.rest.client.KafkaConnectClient;
import com.io.debezium.configserver.rest.client.KafkaConnectClientFactory;
import com.io.debezium.configserver.rest.client.KafkaConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConnectorUtil {
    
    private static final Logger logger = LoggerFactory.getLogger(ConnectorUtil.class);
    
    /**
     * @param cluster   cluster编号
     * @param connector 连接器名称
     * @return 状态：RUNNING-正常运行；FAILED-失败；NOT_EXISTS-不存在
     * @throws IOException           kafka connect error
     */
    public static ConnectorStatus.State connectorStatus(Long cluster, String connector) throws IOException {
        try {
            KafkaConnectClient client = KafkaConnectClientFactory.getClient(cluster);
            ConnectConnectorStatusResponse connectorStatus = client.getConnectorStatus(connector);
            if (connectorStatus.connectorStatus.status.equals(ConnectorStatus.State.RUNNING)) {
                for (ConnectTaskStatus taskState : connectorStatus.taskStates) {
                    if (!taskState.status.equals(ConnectorStatus.State.RUNNING)) {
                        return taskState.status;
                    }
                }
            }
            try {
                client.close();
            } catch (Exception e) {
                logger.error("error when close kafka client :{}", e.getMessage());
            }
            return connectorStatus.connectorStatus.status;
        } catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == 404) {
                logger.warn("ClusterId:{} 404, Connector:{} then state set DESTROYED", cluster, connector);
                return ConnectorStatus.State.DESTROYED;
            } else {
                throw e;
            }
        } catch (KafkaConnectException | IllegalArgumentException e) {
            logger.warn("ClusterId :{} create kafkaConnectClient error then state set DESTROYED msg:{}", cluster, e.getMessage());
            return ConnectorStatus.State.DESTROYED;
        }
    }
    
    public static Map<String, ConnectorStatus.State> listWithStatus(Long cluster) throws IOException {
        Map<String, ConnectorStatus.State> connectorStatus = new HashMap<>();
        boolean flag;
        KafkaConnectClient client = null;
        try {
            client = KafkaConnectClientFactory.getClient(cluster);
            String response = client.listWithStatus("status").readEntity(String.class);
            Map<String, Status> statusMap = JsonUtils.fromJson(response, HashMap.class, String.class, Status.class);
            for (Status statusResponse : statusMap.values()) {
                flag = false;
                String connectorName = statusResponse.status.name;
                if (ConnectorStatus.State.RUNNING.equals(statusResponse.status.connectorStatus.status)) {
                    for (ConnectTaskStatus taskState : statusResponse.status.taskStates) {
                        if (!ConnectorStatus.State.RUNNING.equals(taskState.status)) {
                            connectorStatus.put(connectorName + cluster, taskState.status);
                            flag = true;
                        }
                    }
                }
                if (!flag) {
                    connectorStatus.put(connectorName + cluster, statusResponse.status.connectorStatus.status);
                }
            }
        } catch (IllegalArgumentException illegalArgumentException) {
            // 获取连接失败 跳过
            logger.error("clusterId {} get client error {}", cluster, illegalArgumentException.getMessage());
        } catch (KafkaConnectException e) {
            logger.error("ClusterId:{} create kafkaConnect error : {}", cluster, e.getMessage(), e);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    logger.error("error when close kafka client : {}", e.getMessage());
                }
            }
        }
        
        return connectorStatus;
    }
    
    public static class Status {
        public ConnectConnectorStatusResponse status;
    }

}
