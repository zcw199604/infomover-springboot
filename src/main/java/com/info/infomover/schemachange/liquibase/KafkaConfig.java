package com.info.infomover.schemachange.liquibase;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

@Configuration
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @ConfigProperty(name = "infomover.kafka.bootstrap-servers", defaultValue = "localhost:9092")
    String kafkaBrokers;

    @ConfigProperty(name = "infomover.consumer.groupId", defaultValue = "kafka-client-infomover-consumer")
    String consumerGroupId;

    @ConfigProperty(name = "infomover.consumer.clientId", defaultValue = "kafka-client-infomover-consumer-client")
    String consumerClientId;

    @ConfigProperty(name = "infomover.consumer.maxPoolRecords", defaultValue = "1000")
    String maxPoolRecords;

    @ConfigProperty(name = "infomover.consumer.offsetReset", defaultValue = "earliest")
    String offsetReset;

    @ConfigProperty(name = "infomover.consumer.autoCommit", defaultValue = "false")
    String autoCommit;

    @ConfigProperty(name = "infomover.consumer.securityProtocol", defaultValue = "NONE")
    String securityProtocol = "";
    @ConfigProperty(name = "infomover.consumer.saslMechanism", defaultValue = "NONE")
    String saslMechanism = "";
    @ConfigProperty(name = "infomover.consumer.saslJaasConfig", defaultValue = "NONE")
    String jaasConfig = "";

    private String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "UnknownHost";
        }
    }

    /*@Produces
    @ApplicationScoped*/
    public Consumer<String, String> createConsumer() {
        Properties props = new Properties();

        // Kafka Bootstrap
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);

        /*
         * With group id, kafka broker ensures that the same message is not consumed more then once by a
         * consumer group meaning a message can be only consumed by any one member a consumer group.
         *
         * Consumer groups is also a way of supporting parallel consumption of the data i.e. different consumers of
         * the same consumer group consume data in parallel from different partitions.
         */
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

        /*
         * In addition to group.id, each consumer also identifies itself to the Kafka broker using consumer.id.
         * This is used by Kafka to identify the currently ACTIVE consumers of a particular consumer group.
         */
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId + "-" + getHostname());

        // Deserializers for Keys and Values
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        logger.info("securityProtocol {}, saslMechanism {}, jaasConfig {}",securityProtocol, saslMechanism, jaasConfig);

        //sasl config
        if(StringUtils.isNotBlank(securityProtocol) && !securityProtocol.equals("NONE")){
            props.put("security.protocol", securityProtocol);
        }
        if(StringUtils.isNotBlank(saslMechanism) && !saslMechanism.equals("NONE")){
            props.put("sasl.mechanism", saslMechanism);
        }
        if(StringUtils.isNotBlank(jaasConfig) && !jaasConfig.equals("NONE")){
            System.setProperty("java.security.auth.login.config", jaasConfig);
        }

        // Pool size
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoolRecords);

        /*
         * If true the consumer's offset will be periodically committed in the background.
         * Disabled to allow commit or not under some circumstances
         */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);

        /*
         * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the
         * server:
         *   earliest: automatically reset the offset to the earliest offset
         *   latest: automatically reset the offset to the latest offset
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        return consumer;
    }

}
