package com.info.infomover.sink;


import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.infomover.common.setting.Setting;
import com.info.infomover.common.setting.Sink;

//@Sink(type = "jdbcsink")
public abstract class JdbcSinkBase {

    @Setting(description = "数据源Id", required = true, hidden = true)
    @JsonAlias("datasource.id")
    private String datasourceId;

    @Setting(description = "数据源名称", required = true, hidden = true)
    @JsonAlias("datasource.name")
    private String datasourceName;

    @Setting(description = "插入模式", defaultValue = "insert", values = {"insert", "upsert", "update"}, hidden = true)
    @JsonAlias("insert.mode")
    private String insertMode;

    @Setting(description = "主键模式", defaultValue = "none", values = {"none","record_key","record_value"}, hidden = true)
    @JsonAlias({"pk.mode"})
    private String pkMode;

    @Setting(description = "是否将空记录视为删除。 要求 pk.mode 为 record_key。",displayName = "是否将空记录视为删除",defaultValue = "false", values = {"true", "false"}, hidden = true)
    @JsonAlias("delete.enabled")
    private String deleteEnabled;

    @Setting(description = "批量插入条数", defaultValue = "3000")
    @JsonAlias({"batch.size"})
    private int batchSize;

//    @Setting(description = "任务数", defaultValue = "1")
//    @JsonAlias({"tasks.max"})
//    private String tasksMax;

    @Setting(displayName = "Schema生成(值)", description = "schema生成(值)", defaultValue = "true", values = {"true", "false"})
    @JsonAlias({"value.converter.schemas.enable"})
    private String valueConverterSchemasEnable;


    @Setting(description = "何时在 SQL 语句中引用表名、列名和其他标识符", required = false, hidden = true, displayName = "何时引用标识符", defaultValue = "always")
    @JsonAlias("quote.sql.identifiers")
    private String quoteSqlIdentifiers;

    @Setting(displayName = "目标表名称前缀", description = "拼接在目标表之前,若为空则目标表名为输入表名", required = false, hidden = true)
    @JsonAlias("table.name.format")
    private String tableNameFormat;

    @Setting(description = "是否根据记录模式自动创建目标表", displayName = "是否自动创建目标表", required = false,  defaultValue = "true", values = {"true", "false"})
    @JsonAlias("auto.create")
    private String autoCreate;

    @Setting(description = "当发现相对于记录模式丢失时，是否通过发出自动在表模式中添加列",displayName = "是否自动添加列",required = false, defaultValue = "true", values = {"true", "false"})
    @JsonAlias("auto.evolve")
    private String autoEvolve;

    @Setting(description = "值转换器", hidden = true, values = {"org.apache.kafka.connect.json.JsonConverter"}, defaultValue = "org.apache.kafka.connect.json.JsonConverter", required = false)
    @JsonAlias("value.converter")
    private String valueConverter;

    @Setting(description = "键转换器", hidden = true, values = {"org.apache.kafka.connect.json.JsonConverter"},defaultValue = "org.apache.kafka.connect.json.JsonConverter", required = false)
    @JsonAlias("key.converter")
    private String keyConverter;

    @Setting(description = "键转换器schema生成", hidden = false, required = false, defaultValue = "true", values = {"true", "false"})
    @JsonAlias({"key.converter.schemas.enable"})
    private String keyConverterSchemasEnable;

    @Setting(description = "指标", required = false, defaultValue = "true" , hidden = true)
    @JsonAlias("metrics.enabled")
    private String metricsEnabled;

    @Setting(description = "逗号分隔的主键字段名称", required = false, hidden = true)
    @JsonAlias("pk.fields")
    private String pkFields;

    @Setting(description = "尝试获取有效 JDBC 连接的最大次数",advanced = true,displayName = "最大连接最大次数", defaultValue = "3",required = false)
    @JsonAlias("connection.attempts")
    private String connectionAttempts;

    @Setting(description = "连接尝试之间的间隔时间",advanced = true,displayName = "连接重试间隔",defaultValue = "10000",required = false)
    @JsonAlias("connection.backoff.ms")
    private String connectionBackoffMs;

    @Setting(description = "数据库方言", required = false, defaultValue = "", hidden = true)
    @JsonAlias("dialect.name")
    private String dialectName;

    @Setting(description = "最大重试次数",advanced = true,required = false,defaultValue = "10")
    @JsonAlias("max.retries")
    private String maxRetries;

    @Setting(description = "错误后重试前等待的时间",advanced = true,displayName = "",required = false,defaultValue = "3000")
    @JsonAlias("retry.backoff.ms")
    private String retryBackoffMs;

    @Setting(description = "逗号分隔的记录值字段名称列表。 如果为空，则使用记录值中的所有字段，否则用于过滤到所需字段。",advanced = true,displayName = "白名单过滤条件", required = false, defaultValue = "")
    @JsonAlias("fields.whitelist")
    private String fieldsWhitelist;

    @Setting(description = "时间的值的 JDBC 时区", advanced = true, displayName = "数据库时区", required = false, defaultValue = "UTC")
    @JsonAlias("db.timezone")
    private String dbTimezone;

    @Setting(description = "是否自动创建topic",hidden = true, displayName = "数据库时区", required = false, defaultValue = "false",values = {"false","true"})
    @JsonAlias("topic.creation.enable")
    private String topicCreationEnable;

}
