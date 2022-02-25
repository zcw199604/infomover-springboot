package com.info.infomover.schemachange.monitor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Message;

/**
 * Service for Kafka service which includes a set of primitives to manage events and topics such as:
 * 1. consume events from topic
 * 2. subscribe topic
 * <p>
 * Additionally, it only processes messages of type {@link Message}
 *
 * @author rmarting
 */
@Slf4j
/*@Startup
@ApplicationScoped*/
public class SchemaChangeMonitor {
/*
    @ConfigProperty(name = "infomover.schema.monitor.enable", defaultValue = "true")
    private boolean monitorEnable = true;

    @ConfigProperty(name = "infomover.schema.monitor.topic", defaultValue = "infomover.db.schema.change")
    private String topic;

    @ConfigProperty(name = "infomover.consumer.poolTimeout", defaultValue = "10")
    private Long poolTimeout;

    @ConfigProperty(name = "infomover.schema.monitor.liquibase.home")
    private String liquibaseHome;

    @Inject
    private Consumer<String, String> consumer;

    @Inject
    private SchemaChangeProcessor processor;

    private JsonBuilder jsonBuilder;

    @Inject
    ManagedExecutor exec;

    public static final String LIQUIBASE_HOME = "liquibase_home";

    @PostConstruct
    public void init() {
        if(monitorEnable) {
            log.info("Schema monitor subscribe topic {}", topic);
            log.info("Liquibase home {}", liquibaseHome);
            checkLiquibaseHome(liquibaseHome);
            System.setProperty(LIQUIBASE_HOME, liquibaseHome);
            consumer.subscribe(Collections.singletonList(topic));
            jsonBuilder = JsonBuilder.getInstance();
            exec.execute(() -> {
                while (true) {
                    // Subscribe to Topic
                    try {
                        //TODO 处理消费失败，重复消费的问题
                        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(poolTimeout));
                        int eventCount = consumerRecords.count();
                        if (eventCount > 0) {
                            log.info("Polled #{} records from topic {}", eventCount, topic);
                            Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                            while (iterator.hasNext()) {
                                ConsumerRecord<String, String> consumerRecord = iterator.next();
                                String message = consumerRecord.value();
                                SchemaChangeEvent schemaChangeEvent = jsonBuilder.fromJson(message, SchemaChangeEvent.class);
                                processor.onSchemaChange(schemaChangeEvent);
                                log.info("{} - {}", consumerRecord.key(), message);
                            }
                            // Commit consumption
                            consumer.commitAsync();
                            log.info("Records committed in topic {} from consumer", topic);
                        }else{
                            ThreadUtils.sleep(Duration.ofSeconds(5L));
                        }
                    } catch (Throwable t) {
                        try {
                            TimeUnit.SECONDS.sleep(10);
                        } catch (InterruptedException e) {
                            log.error("Consumer thread Interrupted.", e);
                        }
                        log.error("Consume schema change history message failed.", t);
                    }
                }
            });
            log.info("Launched schema change monitor thread.");
        }
    }

    private void checkLiquibaseHome(String liquibaseHome) {
        if (StringUtils.isBlank(liquibaseHome)) {
            throw new IllegalArgumentException("Liquibase home is empty.");
        }
        File file = new File(liquibaseHome);
        boolean exists = file.exists();
        if (!exists) {
            throw new IllegalArgumentException("Liquibase home file is not exist.");
        }
    }*/
}
