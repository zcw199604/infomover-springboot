package com.info.infomover.datasource;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = MysqlDataSource.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = MysqlDataSource.class, name = "mysql"),
        @JsonSubTypes.Type(value = TidbDataSource.class, name = "tidb"),
        @JsonSubTypes.Type(value = OracleDataSource.class, name = "oracle"),
        @JsonSubTypes.Type(value = PostgresqlDataSource.class, name = "postgres"),
        @JsonSubTypes.Type(value = KafkaDataSource.class, name = "kafka"),
})
public interface IDatasource {

    boolean isJDBCSource();

    boolean isKAFKASource();

    List<String> listTopic();

    Map<String, Object> topicDescribe(String topic);

    String generateJDBCUrl();

    List<DataSourceVerification.DataSourceVerificationDetail> validaConnection();

    List<String> listDatabase();

    List<String> listTable(String schema);

    List<String> listColumn(String schema, String table);

    /**
     * 1 清理成功
     * 0. topic/table不存在
     * -1 失败
     */
    int clearRecords(String target);

}
