package com.info.infomover.recollect;

import com.info.infomover.datasource.IDatasource;
import com.info.infomover.entity.*;
import com.info.infomover.repository.*;
import com.info.infomover.service.ConnectorService;
import com.info.infomover.service.JobService;
import com.info.infomover.util.*;
import com.io.debezium.configserver.model.ConnectConnectorConfigResponse;
import com.io.debezium.configserver.model.ConnectorStatus;
import com.io.debezium.configserver.rest.client.KafkaConnectClient;
import com.io.debezium.configserver.rest.client.KafkaConnectClientFactory;
import com.io.debezium.configserver.rest.client.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @Author: haijun
 * @Date: 2022/1/30 0030 11:40
 */
@Slf4j
public class RecollectJobLauncher implements Runnable {
    private long jobId;
    private Connector connector;
    private String oldConnectorName;
    private String newConnectName;
    private String topicPrefix;
    private ClusterRepository clusterRepository;
    private JobRepository jobRepository;
    private DataSourceRepository dataSourceRepository;
    private ConnectorRepository connectorRepository;
    Pattern saslPattern = Pattern.compile("AES\\(.*\\)");
    private String topicCleanpolicy = "update_offset";
    private String SNAPSHOT_MODE = "initial";
    private AlertRepository alertRepository;
    private JobService jobService;
    private ConnectorService connectorService;


    public RecollectJobLauncher(long jobId, Connector connector, String oldConnectorName, String newConnectName, String topicPrefix,
                                ClusterRepository clusterRepository, JobRepository jobRepository,
                                DataSourceRepository dataSourceRepository, ConnectorRepository connectorRepository,AlertRepository alertRepository,JobService jobService,ConnectorService connectorService) {
        this.jobId = jobId;
        this.oldConnectorName = oldConnectorName;
        this.newConnectName = newConnectName;
        this.topicPrefix = topicPrefix;
        this.clusterRepository = clusterRepository;
        this.jobRepository = jobRepository;
        this.dataSourceRepository = dataSourceRepository;
        this.connectorRepository = connectorRepository;
        this.alertRepository = alertRepository;
        this.jobService = jobService;
        this.connectorService = connectorService;

        this.connector = connectorRepository.findById(connector.getId()).get();
    }

    /**
     * 检查线程不单独配置线程池了，放在重采线程启动完成后
     */
    private void checkLauncher(long id) {
        //任务状态检查线程
        Job queryJob = jobRepository.findById(id).get();
        Job.RecollectStatus status = queryJob.getRecollectStatus();
        if (status == Job.RecollectStatus.RECOLLECTING) {
            new RecollectCheckLauncher(jobRepository,jobService,id).launchCheckJob();
        } else {
            log.info("job {} recollectStatus is {}, no need to check.", id, status);
        }
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        log.info("Start to recollect jobId {} thread {}, startTime {}......", jobId, Thread.currentThread().getId(), startTime);

        try {
            Job finalJob = jobService.findJobByIdAndLoadConnector(jobId);

            //
            // 修改source connect 的 snapshot.mode 为 when_needed
            String snapshotMode = connector.config.get("snapshot.mode");
            if ("schema_only_recovery".equals(snapshotMode)) {
                connector.config.put("snapshot.mode", SNAPSHOT_MODE);
                connectorService.saveConnectorConfig(connector.getId(), connector.config);

                log.info("jobId [{}] ,source connect snapshot.mode is [{}] should be updated to [{}]", jobId, snapshotMode, SNAPSHOT_MODE);
                // 3.修改steps里的source的 otherConfigurations
                List<StepDesc> collect = finalJob.getSteps().stream().filter(item -> Job.StepType.sources.name().equals(item.getScope())).collect(Collectors.toList());
                StepDesc stepDesc = collect.get(0);

                ConfigObject otherConfigurations = stepDesc.getOtherConfigurations();
                otherConfigurations.put("snapshot.mode", SNAPSHOT_MODE);
                jobService.saveSteps(finalJob.getId(), finalJob.getSteps());
                log.info("update connect snapshot.mode , connectName :{} , mode {} -> {}", connector.name, snapshotMode, SNAPSHOT_MODE);
            }
            // 1.清除topic数据
            if (finalJob.getJobType() == Job.JobType.SYNC || (finalJob.getJobType() == Job.JobType.COLLECT && finalJob.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                Long deployClusterId = finalJob.getDeployClusterId();
                if (deployClusterId == null) {
                    return;
                }
                Cluster cluster = clusterRepository.findById(deployClusterId).get();
                // 删除source connect,refesh时在重新创建
                KafkaConnectClient kafkaConnectClient = null;
                try {
                    kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster.getId().intValue());
                    Response response = kafkaConnectClient.deleteConnector(oldConnectorName);
                    log.info("job [{}] ,delete source connector :[{}],response : {}", jobId, connector.name, response);
                } catch (KafkaConnectException e) {
                    throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                } catch (IOException e) {
                    log.warn("delete connector {} failed,error message: {}", connector.name, e.getMessage());
                } catch (WebApplicationException e) {
                    log.warn("connector {} maybe has already being deleted from cluster {}.", connector.name, finalJob.getDeployClusterId());
                }
                //同步任务,可以从sink connect 中取出监听的topic
                List<Connector> sinkConnectors = jobService.findSinkConnectors(finalJob);
                Set<String> topics = sinkConnectors.stream()
                        .map(item -> item.getConfig().get("topics")).collect(Collectors.toSet());

                for (String topic : topics) {
                    cleanTopicData(cluster, topic);
                }
                if (topicCleanpolicy.equals("update_offset")) {
                    int maxRetries = 10;
                    while (maxRetries > 0) {
                        int successConut = 0;
                        for (String topic : topics) {
                            if (checkTopicHasData(cluster, topic)) {
                                successConut++;
                            }
                        }
                        if (successConut == topics.size()) {
                            log.info("{} topics no data at all", topics);
                            break;
                        } else {
                            maxRetries--;
                            log.info("jobId : [{}] ,jobName :[{}] , topic has data ,need to recheck ,Retry count :[{}]", jobId, finalJob.getName(), 10 - maxRetries);
                            ThreadUtils.sleep(Duration.ofSeconds(30L));
                        }
                    }
                }

                //truncate table和clear external kafka record
                for (Connector conn : sinkConnectors) {
                    try {
                        kafkaConnectClient.deleteConnector(conn.name);
                        log.info("job {} ,delete source conncet :{}", jobId, oldConnectorName);
                    } catch (IOException e) {
                        log.warn("delete connector {} failed,error message: {}", connector.name, e.getMessage());
                    } catch (WebApplicationException e) {
                        log.warn("connector {} maybe has already being deleted from cluster {}.", connector.name, finalJob.getDeployClusterId());
                    }

                    Long datasourceId = Long.valueOf(conn.getConfig().get("datasource.id"));
                    DataSource dataSource = dataSourceRepository.findById(datasourceId).get();
                    IDatasource iDatasource = DatasourceUtil.transform2IDatasource(dataSource);
                    if (iDatasource.isKAFKASource()) {
                        //config sink.topic: "sink_topic_infomover_datasource_config" , datasource.id: "7683"
                        String topic = conn.getConfig().get("sink.topic");
                        int success = iDatasource.clearRecords(topic);
                        if (success == -1) {
                            log.error("clear kafka topic error : {}", topic);
                            throw new RuntimeException("clear kafka topic " + topic + "error");
                        } else if (success == 1) {
                            log.info("clear kafka topic {} finished.", topic);
                        } else {
                            log.error("topic {} ,maybe doesn't exist", topic);
                        }
                    } else if (iDatasource.isJDBCSource()) {
                        //config table.name.format: "CAR_WHJ",   datasource.id: "7764"
                        String table = conn.getConfig().get("table.name.format");
                        int success = iDatasource.clearRecords(table);
                        if (success == -1) {
                            log.error("truncate table error :{}", table);
                            throw new RuntimeException("truncate table " + table + " error");
                        } else if (success == 1) {
                            log.info("truncate table {} finished.", table);
                        } else {
                            log.error("table {} ,maybe doesn't exist", table);
                        }
                    }

                    //recreate sink connector
                    ConnectConnectorConfigResponse connectConnectorConfigResponse = new ConnectConnectorConfigResponse();
                    connectConnectorConfigResponse.setConfig(conn.config);
                    connectConnectorConfigResponse.setName(conn.name);
                    log.info("job [{}] start create connector [{}] ", finalJob.getName(), connectConnectorConfigResponse.getName());

                    try {
                        //aes解密
                        aesDecryptPassword(connectConnectorConfigResponse.getConfig());
                        String connector1 = kafkaConnectClient.createConnector(connectConnectorConfigResponse);
                        log.info("job {} create connector {} on cluster {} success.", finalJob.getName(), connectConnectorConfigResponse.getName(), finalJob.getDeployCluster());
                        connectorService.updateConnectorStatus(connectConnectorConfigResponse.getName(), ConnectorStatus.State.RUNNING);
                        log.info("update connector {} connectorStatus -> {}", connectConnectorConfigResponse.getName(), ConnectorStatus.State.RUNNING);
                    } catch (Exception e) {
                        log.error("job {} create connector {} error msg:{}", finalJob.getName(), connectConnectorConfigResponse.getName(), e.getMessage(), e);
                        throw new RuntimeException("create connector " + connectConnectorConfigResponse.getName() + " error");
                    }
                }

                ThreadUtils.sleep(Duration.ofSeconds(3L));
                ConnectConnectorConfigResponse connectConnectorConfigResponse = new ConnectConnectorConfigResponse();
                connectConnectorConfigResponse.setConfig(connector.config);
                connectConnectorConfigResponse.setName(newConnectName);
                //aes解密
                aesDecryptPassword(connectConnectorConfigResponse.getConfig());
                log.info("job [{}] start create connector [{}] ", finalJob.getName(), connectConnectorConfigResponse.getName());
                try {
                    String connector1 = kafkaConnectClient.createConnector(connectConnectorConfigResponse);
                    log.info("job {} create connector {} on cluster {} success.", finalJob.getName(), connectConnectorConfigResponse.getName(), finalJob.getDeployCluster());
                } catch (Exception e) {
                    log.error("job {} create connector {} error msg:{}", finalJob.getName(), connectConnectorConfigResponse.getName(), e.getMessage(), e);
                    throw new RuntimeException("create connector " + connectConnectorConfigResponse.getName() + "error");
                }

            } else if (finalJob.getJobType() == Job.JobType.COLLECT) {
                List<Connector> sinkConnectors = jobService.findSinkConnectors(finalJob);
                for (Connector sinkConnector : sinkConnectors) {
                    String clusterId = sinkConnector.getConfig().get("id");
                    if (StringUtils.isBlank(clusterId)) {
                        continue;
                    }
                    Cluster cluster = clusterRepository.findById(Long.valueOf(clusterId)).get();

                    // 删除source connect,refesh时在重新创建
                    KafkaConnectClient kafkaConnectClient = null;
                    try {
                        kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster.getId().intValue());
                        kafkaConnectClient.deleteConnector(oldConnectorName);
                        log.info("job {} ,delete source conncet :{}", jobId, oldConnectorName);
                    } catch (KafkaConnectException e) {
                        throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                    } catch (IOException e) {
                        log.warn("delete connector {} failed,error message: {}", connector.name, e.getMessage());
                    } catch (WebApplicationException e) {
                        log.warn("connector {} maybe has already being deleted from cluster {}.", connector.name, finalJob.getDeployClusterId());
                    }
                    ThreadUtils.sleep(Duration.ofSeconds(5L));

                    String tableInclude = connector.config.get("table.include.list");
                    List<String> tableIncludes = Arrays.asList(tableInclude.split(","));
                    Set<String> topics = tableIncludes.stream().map(item -> TopicNameUtil.parseTopicName(jobId, this.topicPrefix, item))
                            .collect(Collectors.toSet());
                    for (String topic : topics) {
                        cleanTopicData(cluster, topic);
                    }

                    // 在重建connect 时候,需要去检查所有的业务topic数据有没有情况
                    // 重复检查10次,每次间隔30秒
                    if (topicCleanpolicy.equals("update_offset")) {
                        int maxRetries = 10;
                        while (maxRetries > 0) {
                            int successConut = 0;
                            for (String topic : topics) {
                                if (checkTopicHasData(cluster, topic)) {
                                    successConut++;
                                }
                            }
                            if (successConut == topics.size()) {
                                log.info("{} topics no data at all", topics);
                                break;
                            } else {
                                maxRetries--;
                                log.info("jobId : [{}] ,jobName :[{}] , topic has data ,need to recheck ,Retry count :[{}]", jobId, finalJob.getName(), 10 - maxRetries);
                                ThreadUtils.sleep(Duration.ofSeconds(30L));
                            }
                        }
                    }
                    ThreadUtils.sleep(Duration.ofSeconds(3L));
                    ConnectConnectorConfigResponse connectConnectorConfigResponse = new ConnectConnectorConfigResponse();
                    connectConnectorConfigResponse.setConfig(connector.config);
                    connectConnectorConfigResponse.setName(newConnectName);
                    log.info("job [{}] start create connector [{}] ", finalJob.getName(), connectConnectorConfigResponse.getName());

                    try {
                        //aes解密
                        aesDecryptPassword(connectConnectorConfigResponse.getConfig());
                        String connector1 = kafkaConnectClient.createConnector(connectConnectorConfigResponse);
                        log.info("job {} create connector {} on cluster {} success.", finalJob.getName(), connectConnectorConfigResponse.getName(), finalJob.getDeployCluster());
                        connectorService.updateConnectorStatus(connectConnectorConfigResponse.getName(), ConnectorStatus.State.RUNNING);
                        log.info("update connector {} connectorStatus -> {}", connectConnectorConfigResponse.getName(), ConnectorStatus.State.RUNNING);
                    } catch (Exception e) {
                        log.error("job {} create connector {} error msg:{}", finalJob.getName(), connectConnectorConfigResponse.getName(), e.getMessage(), e);
                        throw new RuntimeException("create connector " + connectConnectorConfigResponse.getName() + " error");
                    }
                }
            }

        } catch (Exception e) {
            log.error("job {} recollect failed :{}", jobId, e.getMessage(), e);
            jobService.updateJobRecollectStatus(jobId, Job.RecollectStatus.RECOLLECTFAILED);
            log.debug("====recollect failed==========");
        } finally {
            long endTime = System.currentTimeMillis();
            log.info("End to recollect jobId {} thread {}, endTime {} and cost {}ms", jobId, Thread.currentThread().getId(), endTime, endTime - startTime);
        }

        //检查重采任务是否完成
        checkLauncher(jobId);
    }



    /**
     * 清除topic数据
     *
     * @param cluster
     * @param topic
     * @return
     */
    public boolean cleanTopicData(Cluster cluster, String topic) {
        try (AdminClient client = createAdminClient(cluster)) {
            Properties properties = creatPropByCluster(cluster);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // 创建consumer 用来查找topic下的partition数
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

//                switch (topicCleanpolicy) {
//                    case "update_offset":
                DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(topic));
                Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
                for (String s : stringTopicDescriptionMap.keySet()) {
                    TopicDescription topicDescription = stringTopicDescriptionMap.get(s);
                    List<TopicPartitionInfo> partitions = topicDescription.partitions();
                    List<TopicPartition> topicPartitions = new ArrayList<>(partitions.size());
                    partitions.stream().forEach(item -> topicPartitions.add(new TopicPartition(topic, item.partition())));
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
                    Map<TopicPartition, RecordsToDelete> topicPartitionRecordsToDeleteHashMap = new HashMap<>();
                    endOffsets.keySet().stream().forEach(item ->
                            topicPartitionRecordsToDeleteHashMap.put(new TopicPartition(item.topic(), item.partition()),
                                    RecordsToDelete.beforeOffset(endOffsets.get(item).longValue())));
                    log.info("clean [{}] topic data start", topic);
                    DeleteRecordsResult deleteRecordsResult = client.deleteRecords(topicPartitionRecordsToDeleteHashMap);
                    deleteRecordsResult.all().get();
                }
                return true;

//                    case "delete_log":
//                        try {
//                            //构建ConfigResource，指定name为java_tst的Topic
//                            ConfigResource configResource=new ConfigResource(ConfigResource.Type.TOPIC,topic);
//                            //构建集合并添加到configResource进入
//                            Collection<ConfigResource> configResources=new ArrayList<>();
//                            configResources.add(configResource);
//                            //执行方法拿到返回结果
//                            DescribeConfigsResult result = client.describeConfigs(configResources);
//                            //等待返回结果完成
//                            result.all().get();
//                            //解析返回结果
//                            Map<ConfigResource, KafkaFuture<Config>> futureMap = result.values();
//                            KafkaFuture<Config> configKafkaFuture = futureMap.get(configResource);
//                            Collection<ConfigEntry> entries = configKafkaFuture.get().entries();
//                            String retentionMs = null;
//                            for (ConfigEntry entry : entries) {
//                                if (entry.name().equals("retention.ms")) {
//                                    log.info("topic retention.ms config is [{}]", entry.value());
//                                    retentionMs = entry.value();
//                                }
//                            }
//                            if (StringUtils.isNotEmpty(retentionMs)) {
//                                Map<ConfigResource, Collection<AlterConfigOp>> configs =new HashMap<>();
//                                ConfigEntry configEntry=new ConfigEntry("retention.ms", "1000");
//                                //设置一个AlterConfigOp对象用来，处理做什么操作，这里使用的SET(或者APPEND)类型的添加操作
//                                AlterConfigOp alterConfigOp=new AlterConfigOp(configEntry,AlterConfigOp.OpType.SET);
//                                //添加到Collection中，用于传到incrementalAlterConfigs方法中
//                                Collection<AlterConfigOp> configOps=new ArrayList<>();
//                                configOps.add(alterConfigOp);
//                                //给map赋值
//                                configs.put(configResource,configOps);
//                                //调用方法
//                                AlterConfigsResult result_ = client.incrementalAlterConfigs(configs);
//                                if (result.all().isDone()){
//                                    log.info("update topic config success,[{}] -> [{}]", retentionMs, "1000");
//                                    //检查是否成功
//                                    int maxRetries = 20;
//                                    while (maxRetries > 0) {
//                                        // 如果已经清除,需要将配置恢复
//                                        if (checkTopicHasData(cluster, topic)) {
//                                            configs =new HashMap<>();
//                                            configEntry=new ConfigEntry("retention.ms", retentionMs);
//                                            //设置一个AlterConfigOp对象用来，处理做什么操作，这里使用的SET(或者APPEND)类型的添加操作
//                                            alterConfigOp=new AlterConfigOp(configEntry,AlterConfigOp.OpType.SET);
//                                            //添加到Collection中，用于传到incrementalAlterConfigs方法中
//                                            configOps=new ArrayList<>();
//                                            configOps.add(alterConfigOp);
//                                            //给map赋值
//                                            configs.put(configResource,configOps);
//                                            //调用方法
//                                            result_ = client.incrementalAlterConfigs(configs);
//                                            if (result_.all().isDone()) {
//                                                log.info("update topic [{}] config success ,retention.ms :[{}]", topic, retentionMs);
//                                            }
//                                            return true;
//                                        }
//                                        try {
//                                            maxRetries--;
//                                            log.info("topic has data ,need to recheck ,Retry count :[{}]", 20 - maxRetries);
//                                            Thread.sleep(3000 * 10);
//                                        } catch (InterruptedException e) {
//                                            log.error(e.getMessage());
//                                        }
//                                    }
//                                }
//                            }
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        } catch (ExecutionException e) {
//                            e.printStackTrace();
//                        }
//                        break;

//                }

            } catch (Exception e) {
                if (e.getMessage().contains("UnknownTopicOrPartitionException")) {
                    log.error("This server does have [{}] topic,cluster id :{}", topic, cluster.getId());
                } else {
                    log.error("clean topic error ,msg :{}", e.getMessage(), e);
                }
            }
        }

        return false;
    }


    /**
     * 检查topic 是否还存在数据
     *
     * @param cluster
     * @param topic
     * @return
     */
    public boolean checkTopicHasData(Cluster cluster, String topic) {
        try (AdminClient client = createAdminClient(cluster)) {
            Properties properties = creatPropByCluster(cluster);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // 创建consumer 用来查找topic下的partition数
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
                DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(topic));
                Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
                for (String s : stringTopicDescriptionMap.keySet()) {
                    TopicDescription topicDescription = stringTopicDescriptionMap.get(s);
                    List<TopicPartitionInfo> partitions = topicDescription.partitions();
                    List<TopicPartition> topicPartitions = new ArrayList<>(partitions.size());
                    partitions.stream().forEach(item -> topicPartitions.add(new TopicPartition(topic, item.partition())));
                    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
                    Map<String, Long> topicPartition = new HashMap<>();
                    beginningOffsets.keySet().stream().forEach(item -> topicPartition.put(item.topic() + "_" + item.partition(), beginningOffsets.get(item).longValue()));
                    ;
                    long count = endOffsets.keySet().stream().filter(item -> endOffsets.get(item).longValue() > topicPartition.get(item.topic() + "_" + item.partition())).count();
                    if (count > 0) {
                        log.info("[{}] have data, need to recheck", topic);
                        return false;
                    } else {
                        log.info("[{}] don't have data", topic);
                        return true;
                    }
                }
            } catch (Exception e) {
                if (e.getMessage().contains("UnknownTopicOrPartitionException")) {
                    return true;
                }
                log.error("checkTopicHasData error msg :{}", e.getMessage(), e);
            }

        }
        return false;
    }

    public AdminClient createAdminClient(Cluster cluster) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        return AdminClient.create(props);
    }

    public Properties creatPropByCluster(Cluster cluster) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        return props;
    }


    public void deleteTopicRecord(Cluster cluster, Set<String> topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        AdminClient client = null;
        try {
            client = AdminClient.create(props);
            DeleteTopicsOptions options = new DeleteTopicsOptions();
            options.timeoutMs(8000);
            DescribeTopicsResult describeTopicsResult = client.describeTopics(topics);
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
            for (String topic : stringTopicDescriptionMap.keySet()) {
                TopicDescription topicDescription = stringTopicDescriptionMap.get(topic);
                List<TopicPartitionInfo> partitions = topicDescription.partitions();

                for (TopicPartitionInfo partition : partitions) {

                }
            }
            TopicPartition topicPartition = new TopicPartition("122121", 3);
            Map<TopicPartition, RecordsToDelete> topicPartitionRecordsToDeleteMap = Collections.singletonMap(topicPartition, RecordsToDelete.beforeOffset(0L));
            //client.deleteRecords(topicPartition);

            client.close();
            log.info("delete topics :{}", topics);

        } catch (Exception e) {
            if (e.getMessage().contains("This server does not host this topic-partition.")) {
                log.error("topic may have been deleted");
            } else if (e.getMessage().contains("org.apache.kafka.common.errors.TimeoutException")) {
                log.error("cluster maybe is dead");
            } else {
                log.error(e.getMessage(), e);
                throw new RuntimeException("del topics error " + e.getMessage());
            }
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public void deleteTopicsByCluster(Cluster cluster, Set<String> topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        AdminClient client = null;
        try {
            client = AdminClient.create(props);
            DeleteTopicsOptions options = new DeleteTopicsOptions();
            options.timeoutMs(8000);
            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(topics, options);
            deleteTopicsResult.all().get();
            client.close();
            log.info("delete topics :{}", topics);

        } catch (Exception e) {
            if (e.getMessage().contains("This server does not host this topic-partition.")) {
                log.error("topic may have been deleted");
            } else if (e.getMessage().contains("org.apache.kafka.common.errors.TimeoutException")) {
                log.error("cluster maybe is dead");
            } else {
                log.error(e.getMessage(), e);
                throw new RuntimeException("del topics error " + e.getMessage());
            }
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                }
            }
        }
    }

    private void aesDecryptPassword(Map<String, String> config) {
        // source && connector type jdbc
        if (config.containsKey(DataSourceKeyword.DATABASE_PASSWORD)) {
            config.put(DataSourceKeyword.DATABASE_PASSWORD, AesUtils.wrappeDaesDecrypt(config.get(DataSourceKeyword.DATABASE_PASSWORD)));
        }

        // sink && connector type jdbc
        if (config.containsKey(SinkConnectorKeyword.SINK_PASSWD)) {
            config.put(SinkConnectorKeyword.SINK_PASSWD, AesUtils.wrappeDaesDecrypt(config.get(SinkConnectorKeyword.SINK_PASSWD)));
        }

        // sink && connector type kafka
        if (config.containsKey(DataSourceKeyword.PASSWORD)) {
            String passWord = config.get(DataSourceKeyword.PASSWORD);
            if (passWord.startsWith("AES(")) {
                String s = AesUtils.wrappeDaesDecrypt(passWord);
                if (config.containsKey("sasl.jaas.config")) {
                    config.put("sasl.jaas.config", config.get("sasl.jaas.config").replace(passWord, s));
                }
            }
            config.put(DataSourceKeyword.PASSWORD, AesUtils.wrappeDaesDecrypt(config.get(DataSourceKeyword.PASSWORD)));
        }


        // kafka history topic
        //eg org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='AES(admin)';
        if (config.containsKey("database.history.consumer.sasl.jaas.config")) {
            String consumerSasl = config.get("database.history.consumer.sasl.jaas.config");
            Matcher matcher = saslPattern.matcher(consumerSasl);
            if (matcher.find()) {
                String group = matcher.group();
                config.put("database.history.consumer.sasl.jaas.config", consumerSasl.replace(group, AesUtils.wrappeDaesDecrypt(group)));
            }
        }

        //eg org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='AES(admin)';
        if (config.containsKey("database.history.producer.sasl.jaas.config")) {
            String producerSasl = config.get("database.history.producer.sasl.jaas.config");
            Matcher matcher = saslPattern.matcher(producerSasl);
            if (matcher.find()) {
                String group = matcher.group();
                config.put("database.history.producer.sasl.jaas.config", producerSasl.replace(group, AesUtils.wrappeDaesDecrypt(group)));
            }
        }
    }

}
