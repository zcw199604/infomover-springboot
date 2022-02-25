package com.info.infomover.common.setting;

import com.io.debezium.configserver.model.ConnectorProperty;
import com.io.debezium.configserver.model.ConnectorProperty.Type;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class Transformtions {

    public String transform;
    public boolean enabled;
    public List<ConnectorProperty> configurationOptions;

    public Transformtions(String transform, boolean enabled, List<ConnectorProperty> configurationOptions) {
        this.transform = transform;
        this.enabled = enabled;
        this.configurationOptions = configurationOptions;
    }


    public static Map<String, List<ConnectorProperty>> propertyList = new HashMap<>();


    static {

        List<ConnectorProperty> filterList = new ArrayList<>();
        filterList.add(new ConnectorProperty("topic.regex", "正则表达式", "一个可选的正则表达式，用于评估事件的目标主题的名称以确定是否应用过滤逻辑。", Type.STRING, "", false, null, null,"transforml"));
        filterList.add(new ConnectorProperty("language", "表达式语言", "表达式所用的语言。必须以 jsr223. 开头，例如 jsr223.groovy 或 jsr223.graal.js。 Debezium 仅支持通过 JSR 223 API（'ScriptingfortheJava™Platform'）进行引导。", Type.STRING, "", true, null, Arrays.asList(new String[]{"jsr223.groovy", "jsr223.graal.js"}),"transform"));
        filterList.add(new ConnectorProperty("condition", "每条消息计算的表达式", "要为每条消息计算的表达式。必须评估为布尔值，其中 true 的结果保留消息，而 false 的结果删除它。", Type.STRING, "", true, null, null,"transform"));
        filterList.add(new ConnectorProperty("null.handling.mode", "如何处理空消息", "指定转换如何处理空（逻辑删除）消息。", Type.STRING, "keep", true, null, Arrays.asList(new String[]{"keep", "drop", "evaluate"}),"transform"));

        propertyList.put("io.debezium.transforms.Filter", filterList);


        List<ConnectorProperty> contentbasedrouter = new ArrayList<>();
        contentbasedrouter.add(new ConnectorProperty("topic.regex", "正则表达式", "一个可选的正则表达式，用于评估事件的目标主题的名称以确定是否应用过滤逻辑。", Type.STRING, "", false, null, null,"transform"));
        contentbasedrouter.add(new ConnectorProperty("language", "表达式语言", "表达式所用的语言。必须以 jsr223. 开头，例如 jsr223.groovy 或 jsr223.graal.js。 Debezium 仅支持通过 JSR 223 API（'ScriptingfortheJava™Platform'）进行引导。", Type.STRING, "", true, null, Arrays.asList(new String[]{"jsr223.groovy", "jsr223.graal.js"}),"transform"));
        contentbasedrouter.add(new ConnectorProperty("condition", "每条消息计算的表达式", "要为每条消息计算的表达式。必须评估为布尔值，其中 true 的结果保留消息，而 false 的结果删除它。", Type.STRING, "", true, null, null,"transform"));
        contentbasedrouter.add(new ConnectorProperty("null.handling.mode", "如何处理空消息", "指定转换如何处理空（逻辑删除）消息。", Type.STRING, "keep", true, null, Arrays.asList(new String[]{"keep", "drop", "evaluate"}),"transform"));
        propertyList.put("io.debezium.transforms.ContentBasedRouter", contentbasedrouter);

        List<ConnectorProperty> extractnewrecordstate = new ArrayList<>();
        extractnewrecordstate.add(new ConnectorProperty("delete.handling.mode", "删除事件", "Debezium 为每个 DELETE 操作生成一个更改事件记录。默认行为是事件展平 SMT 从流中删除这些记录。", Type.STRING, "drop", true, null, Arrays.asList(new String[]{"drop", "none", "rewrite"}),"transform"));
        extractnewrecordstate.add(new ConnectorProperty("drop.tombstones", "删除记录", "Debezium 为每个 DELETE 操作生成一个墓碑记录。要在流中保留墓碑记录，请指定 drop.tombstones=false", Type.BOOLEAN, true, false, null, Arrays.asList(new String[]{"true", "false"}),"transform"));
        extractnewrecordstate.add(new ConnectorProperty("add.fields.prefix", "Fields prefix", "要为每条消息计算的表达式。必须评估为布尔值，其中 true 的结果保留消息，而 false 的结果删除它。", Type.STRING, "__", false, null, null,"transform"));
        extractnewrecordstate.add(new ConnectorProperty("add.fields", "Fields", "The expression to be evaluated for every message. Must evaluate to a Boolean value where a result of true keeps the message, and a result of false removes it.", Type.STRING, "", false, null, null,"transform"));
        extractnewrecordstate.add(new ConnectorProperty("add.headers.prefix", "Header prefix", "The expression to be evaluated for every message. Must evaluate to a Boolean value where a result of true keeps the message, and a result of false removes it.", Type.STRING, "__", false, null, null,"transform"));
        extractnewrecordstate.add(new ConnectorProperty("add.headers", "Header", "The expression to be evaluated for every message. Must evaluate to a Boolean value where a result of true keeps the message, and a result of false removes it.", Type.STRING, "", false, null, null,"transform"));

        propertyList.put("io.debezium.transforms.ExtractNewRecordState", extractnewrecordstate);

        List<ConnectorProperty> bylogicaltablerouter = new ArrayList<>();


        bylogicaltablerouter.add(new ConnectorProperty("topic.regex", "事件记录的正则表达式", "指定转换应用于每个更改事件记录的正则表达式，以确定是否应将其路由到特定主题", Type.STRING, "", true, null, null,"transform"));
        bylogicaltablerouter.add(new ConnectorProperty("topic.replacement", "Topic 转换", "指定表示目标主题名称的正则表达式。", Type.STRING, "", true, null, null,"transform"));
        bylogicaltablerouter.add(new ConnectorProperty("key.enforce.uniqueness", "强制密钥唯一性", "指示是否向记录的更改事件键添加字段。", Type.BOOLEAN, true, false, null, null,"transform"));
        bylogicaltablerouter.add(new ConnectorProperty("key.field.name", "关键字段名称", "要添加到更改事件键的字段的名称", Type.STRING, "__dbz__physicalTableIdentifier", false, null, null,"transform"));
        bylogicaltablerouter.add(new ConnectorProperty("key.field.regex", "关键字段正则表达式", "指定转换应用于默认目标主题名称以捕获一组或多组字符的正则表达式。", Type.STRING, "", false, null, null,"transform"));
        bylogicaltablerouter.add(new ConnectorProperty("key.field.replacement", "关键字段替换", "指定正则表达式，用于根据为 key 指定的表达式捕获的组来确定插入的 key 字段的值。", Type.STRING, "", false, null, null,"transform"));

        propertyList.put("io.debezium.transforms.ByLogicalTableRouter", bylogicaltablerouter);

        List<ConnectorProperty> valueToKey = new ArrayList<>();

        valueToKey.add(new ConnectorProperty("fields", "字段", "用由记录值中的字段子集形成的新键替换记录键。", Type.STRING, "", true, null, null,"transform"));

        propertyList.put("org.apache.kafka.connect.transforms.ValueToKey", valueToKey);


        List<ConnectorProperty> timestamproute = new ArrayList<>();


        timestamproute.add(new ConnectorProperty("timestamp.format", "时间戳格式", "与 java.text.SimpleDateFormat 兼容的时间戳的格式字符串。有关其他详细信息，请参阅 SimpleDateFormat", Type.STRING, "yyyyMMdd", true, null, null,"transform"));
        timestamproute.add(new ConnectorProperty("topic.format", "主题格式", "格式字符串，可以分别包含 ${topic} 和 ${timestamp} 作为主题和时间戳的占位符。", Type.STRING, "${topic}-${timestamp}", true, null, null,"transform"));

        propertyList.put("org.apache.kafka.connect.transforms.TimestampRoute", timestamproute);



        List<ConnectorProperty> TimestampConverter = new ArrayList<>();


        TimestampConverter.add(new ConnectorProperty("target.type", "时间戳格式", "示例:string, unix, Date, Time, or Timestamp", Type.STRING, "", true, null, null,"transform"));
        TimestampConverter.add(new ConnectorProperty("field", "时间字段", "COL_TIMESTAMP", Type.STRING, "", true, null, null,"transform"));
        TimestampConverter.add(new ConnectorProperty("format", "格式", "示例:yyyy-MM-dd HH:mm:ss.SSS", Type.STRING, "", true, null, null, "transform", true));

        propertyList.put("org.apache.kafka.connect.transforms.TimestampConverter$Value", TimestampConverter);


    }
}
