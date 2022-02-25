package com.info.infomover.datasource;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.infomover.common.setting.DataSource;
import com.info.infomover.common.setting.Setting;
import com.info.infomover.util.AesUtils;
import com.info.infomover.util.CheckUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

@DataSource(type = "kafka")
@Slf4j
public class KafkaDataSource extends AbstractDataSource {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataSource.class);

    public KafkaDataSource() {
        super("kafka");
    }

    @Setting(description = "bootStrapServers")
    @JsonAlias("bootstrap.servers")
    public String bootStrapServers;

    @Setting(values = {"SASL_PLAINTEXT", "PLAINTEXT"}, displayName = "安全协议", description = "安全协议")
    @JsonAlias("security.protocol")
    public String securityProtocol;

    @Setting(bind = {"SASL_PLAINTEXT"}, scope = "security.protocol", displayName = "验证机制", description = "验证机制", values = {"SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"})
    @JsonAlias("sasl.mechanism")
    public String saslMechanism;

    @Setting(bind = {"SASL_PLAINTEXT"}, scope = "security.protocol", displayName = "登录模式", description = "登录模式", values = {"org.apache.kafka.common.security.plain.PlainLoginModule", "org.apache.kafka.common.security.scram.ScramLoginModule"})
    @JsonAlias("login.module")
    public String loginModule;

    @Setting(bind = {"SASL_PLAINTEXT"}, scope = "security.protocol", displayName = "用户名", description = "用户名")
    public String username;

    @Setting(bind = {"SASL_PLAINTEXT"}, scope = "security.protocol", displayName = "密码", description = "密码")
    public String password;

    public String saslConfig;


    @Override
    public String generateJDBCUrl() {
        throw new RuntimeException("UnSupport ");
    }

    @Override
    public List<String> listDatabase() {
        throw new RuntimeException("UnSupport ");
    }

    @Override
    public List<String> listTable(String schema) {
        throw new RuntimeException("UnSupport ");
    }

    @Override
    public List<String> listColumn(String schema, String table) {
        throw new RuntimeException("UnSupport ");
    }

    /**
     * 1 清理成功
     * 0. topic不存在
     * -1 失败
     */
    @Override
    public int clearRecords(String topic) {
        //本次只支持更新offset的方式
        String topicCleanpolicy = "update_offset";
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServers);
        props.put("retries", 3);
        props.put("delivery.timeout.ms", 3000);
        if (StringUtils.isNotBlank(securityProtocol) && !securityProtocol.equals("NONE") && StringUtils.isNotBlank(saslMechanism) && !saslMechanism.equals("NONE")) {
            String format = String.format("%s required username='%s' password='%s';", loginModule, username, AesUtils.wrappeDaesDecrypt(password));
            this.saslConfig = format;
            props.put("sasl.jaas.config", format);
            props.put("sasl.mechanism", this.saslMechanism);
            props.put("security.protocol", this.securityProtocol);
        }
        try (AdminClient client = AdminClient.create(props)) {
            Properties properties = new Properties();
            properties.putAll(props);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // 创建consumer 用来查找topic下的partition数
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

            DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(topic));
            try {
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
                    logger.info("clean [{}] topic data start", topic);
                    DeleteRecordsResult deleteRecordsResult = client.deleteRecords(topicPartitionRecordsToDeleteHashMap);
                    deleteRecordsResult.all().get();
                }
            } catch (Exception e) {
                //org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.
                if (e.getMessage().contains("UnknownTopicOrPartitionException")) {
                    return 0;
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            logger.error("clear kafka records error {}", e.getMessage(), e);
            return -1;
        }

        return 1;

    }

    @Override
    public String getType() {
        return super.getType();
    }

    @Override
    public void setType(String type) {
        super.setType(type);
    }

    @Override
    public boolean isJDBCSource() {
        return false;
    }

    @Override
    public List<DataSourceVerification.DataSourceVerificationDetail> validaConnection() {

        Properties props = new Properties();
        CheckUtil.checkTrue(StringUtils.isEmpty(bootStrapServers), "bootStrapServers is empty");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServers);
        props.put("retries", 3);
        props.put("delivery.timeout.ms", 3000);
        if (StringUtils.isNotBlank(securityProtocol)
                && !securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(saslMechanism)
                && !saslMechanism.equals("NONE")
            //&& StringUtils.isNotBlank(jaasConfig) && !jaasConfig.equals("NONE")
        ) {
            String format = String.format("%s required username='%s' password='%s';", loginModule, username, AesUtils.wrappeDaesDecrypt(password));
            this.saslConfig = format;
            props.put("sasl.jaas.config", format);
            props.put("sasl.mechanism", this.saslMechanism);
            props.put("security.protocol", this.securityProtocol);
        }
        try (AdminClient client = AdminClient.create(props)) {
            System.out.println("create admin client success");
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false);
            options.timeoutMs(10000);
            ListTopicsResult listTopicsResult = client.listTopics(options);
            listTopicsResult.names().get();
            return DataSourceVerification.builder().isSink(true).build().updateIsSuccess(this.getType(), DataSourceVerification.TYPE.CONNECTION);
        } catch (Exception e) {
            e.printStackTrace();
            return DataSourceVerification.builder().isSink(true).build().updateIsSuccess(this.getType());
        }
    }

    @Override
    public boolean isKAFKASource() {
        return true;
    }

    @Override
    public List<String> listTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServers);
        props.put("retries", 3);
        props.put("delivery.timeout.ms", 3000);
        if (StringUtils.isNotBlank(securityProtocol)
                && !securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(saslMechanism)
                && !saslMechanism.equals("NONE")
        ) {
            String format = String.format("%s required username='%s' password='%s';", loginModule, username, AesUtils.wrappeDaesDecrypt(password));
            this.saslConfig = format;
            props.put("sasl.jaas.config", format);
            props.put("sasl.mechanism", this.saslMechanism);
            props.put("security.protocol", this.securityProtocol);
        }
        try (AdminClient client = AdminClient.create(props)) {
            System.out.println("create admin client success");
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false);
            options.timeoutMs(10000);
            ListTopicsResult listTopicsResult = client.listTopics(options);
            List<String> collect = listTopicsResult.names().get().stream().map(item -> String.valueOf(item)).collect(Collectors.toList());
            Collections.sort(collect);
            return collect;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("listDatabase error :{}", e.getMessage(), e);
        }

        return Arrays.asList();
    }

    @Override
    public Map<String, Object> topicDescribe(String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServers);
        props.put("retries", 3);
        props.put("delivery.timeout.ms", 3000);
        if (StringUtils.isNotBlank(securityProtocol)
                && !securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(saslMechanism)
                && !saslMechanism.equals("NONE")
        ) {
            String format = String.format("%s required username='%s' password='%s';", loginModule, username, AesUtils.wrappeDaesDecrypt(password));
            this.saslConfig = format;
            props.put("sasl.jaas.config", format);
            props.put("sasl.mechanism", this.saslMechanism);
            props.put("security.protocol", this.securityProtocol);
        }

        Map<String, Object> returnData = new HashMap<>();
        try (AdminClient client = AdminClient.create(props)) {

            DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(topic));
            TopicDescription topicDescription = describeTopicsResult.all().get().get(topic);
            returnData.put("partitionCount", topicDescription.partitions().size());
            returnData.put("replicationFactor", topicDescription.partitions().size() == 0 ? 0 : topicDescription.partitions().get(0).replicas().size());
            return returnData;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("listDatabase error :{}", e.getMessage(), e);
        }
        return returnData;
    }
}
