package com.info.infomover.sink;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.infomover.common.setting.Setting;
import com.info.infomover.common.setting.Sink;

@Sink(type = "kafka")
public class KafkaSink{
    @Setting(description = "kafka集群id", displayName = "集群id", required = true, hidden = true)
    @JsonAlias("datasource.id")
    private String id;

    @Setting(description = "kafka集群名称",displayName = "集群名称",required = true, hidden = true)
    @JsonAlias("datasource.name")
    private String name;

    @Setting(description = "bootstrap地址", displayName = "bootstrap地址")
    @JsonAlias("bootstrap.servers")
    private String bootStrapServer;

    @Setting(description = "connector class", displayName = "connector class", defaultValue = "com.info.connect.kafka.sink.KafkaSinkConnector", hidden = true)
    @JsonAlias("connector.class")
    private String connectorClass;

    @Setting(description = "是否使用指标", displayName = "是否使用指标", defaultValue = "true", hidden = true)
    @JsonAlias("metrics.enabled")
    private String metricsEnabled;

    @Setting(description = "sink topic 分区数", displayName = "sink topic 分区数", defaultValue = "1")
    @JsonAlias("sink.topic.partitions")
    private String sinkTopicPartitions;

    @Setting(description = "sink topic 副本数", displayName = "sink topic 副本数", defaultValue = "1")
    @JsonAlias("sink.topic.replication.factor")
    private String sinkTopicReplicationFactor;

    @Setting(description = "消息最大字节限制", displayName = "消息最大字节限制", defaultValue = "2000000")
    @JsonAlias("sink.topic.max.message.bytes")
    private String sinkTopicMaxMessageBytes;

    @Setting(description = "topic清理策略", displayName = "topic清理策略",defaultValue = "delete",values = {"delete","compact"})
    @JsonAlias("sink.topic.cleanup.policy")
    private String sinkTopicCleanupPolicy;

    @Setting(description = "压缩方式", displayName = "压缩方式", defaultValue = "producer", values = {"producer","uncompressed","gzip", "snappy"})
    @JsonAlias("sink.topic.compression.type")
    private String sinkTopicCompressionType;

    @Setting(description = "日志保留时间", displayName = "日志保留时间", defaultValue = "-1")
    @JsonAlias("sink.topic.retention.ms")
    private String sinkTopicRetentionMs;

    @Setting(description = "批量个数", displayName = "批量个数", defaultValue = "1")
    @JsonAlias("batch.size")
    private String batchSize;

    @Setting(description = "key serializer", displayName = "key serializer", advanced = true, defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
    @JsonAlias("key.serializer")
    private String keySerializer;

    @Setting(description = "value serializer", displayName = "value serializer",advanced = true, defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
    @JsonAlias("value.serializer")
    private String valueSerializer;

    @Setting(description = "value converter schemas enable", displayName = "value schemas show", advanced = true, defaultValue = "true", values = {"true", "false"})
    @JsonAlias("value.converter.schemas.enable")
    private String valueCnverterSchemasEnable;

    @Setting(description = "value converter", displayName = "value converter", defaultValue = "org.apache.kafka.connect.storage.StringConverter")
    @JsonAlias("value.converter")
    private String valueConverter;

    @Setting(description = "key converter", displayName = "key converter", defaultValue = "org.apache.kafka.connect.storage.StringConverter")
    @JsonAlias("key.converter")
    private String keyConverter;

    @Setting(description = "key converter schemas enable", displayName = "key schemas show", defaultValue = "true", values = {"true", "false"})
    @JsonAlias("key.converter.schemas.enable")
    private String keyConverterSchemasEnable;

}
