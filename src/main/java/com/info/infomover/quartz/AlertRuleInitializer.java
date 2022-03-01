package com.info.infomover.quartz;

import com.info.infomover.entity.AlertRule;
import com.info.infomover.entity.QAlertRule;
import com.info.infomover.prom.constants.MetricMethod;
import com.info.infomover.prom.constants.MetricName;
import com.info.infomover.prom.constants.Operator;
import com.info.infomover.prom.constants.StepDuration;
import com.info.infomover.repository.AlertRuleRepository;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 初始化内置的告警规则
 * 1. Source Connector 5min(缺省值， 通过connector.alert.fire.nodata.minutes进行修改) 没有读取数据进行告警的规则
 * 该内置规则默认对所有的发布任务的启用的Kafka Connector生效，如果任务存在关联的自建告警规则，则内置告警规则失效
 * 2. JDBC Sink connector 5min(缺省值， 通过connector.alert.fire.nodata.minutes进行修改) 没有输出数据进行告警的规则
 * （如果source connector出现告警，该规则不会生效）
 * 3. Kafka Sink connector 5min(缺省值， 通过connector.alert.fire.nodata.minutes进行修改) 没有输出数据进行告警的规则
 * （如果source connector出现告警，该规则不会生效; 如果没有开启metrics.enable, 该规则也不生效）
 * <p>
 * 4. Kafka Cluster Connect Node 5min(缺省值，通过cluster.alert.fire.down.minutes进行修改） Node节点宕机
 */
@Component
public class AlertRuleInitializer {
    private static final Logger logger = LoggerFactory.getLogger(AlertRuleInitializer.class);

    public static final String runFail = "RUN_FAILED";
    public static final String noDataSeen = "NO_DATA_READ";
    public static final String jdbcSinkNoData = "NO_DATA_JDBC_SINK";
    public static final String kafkaSinkNoData = "NO_DATA_KAFKA_SINK";
    public static final String clusterDown = "ClusterNodeDown";

    @Autowired
    private JPAQueryFactory queryFactory;

    @Autowired
    private AlertRuleRepository alertRuleRepository;

    @Value("${connector.alert.fire.nodata.minutes:5}")
    private Integer noDataKeepMinute;

    @Value("${connector.alert.fire.runFail.minutes:5}")
    private Integer runFailKeepMinute;

    @Value("${cluster.alert.fire.down.minutes:5}")
    private Integer clusterDownKeepMinute;

    public static final Map<String, AlertRule> builtinRuleMap = new HashMap<>();

    @PostConstruct
    public void start() {
        logger.info("server start at {}", LocalDateTime.now());
        // init runFailRule check connector status in kafka cluster
        // 对所有的任务生效，即使存在关联的custom AlertRule，也优先进行该规则检查
        AlertRule runFailRule = initRunFailRule();
        AlertRule sourceRule = initNoDataRule(noDataSeen, MetricName.TotalNumberOfEventsSeen.name());
        AlertRule jdbcSinkRule = initNoDataRule(jdbcSinkNoData, "jdbc_sink_total");
        AlertRule kafkaSinkRule = initNoDataRule(kafkaSinkNoData, "kafka_sink_total");

        builtinRuleMap.put(runFail, runFailRule);
        builtinRuleMap.put(noDataSeen, sourceRule);
        builtinRuleMap.put(jdbcSinkNoData, jdbcSinkRule);
        builtinRuleMap.put(kafkaSinkNoData, kafkaSinkRule);

        // init cluster down rule
        AlertRule clusterDownRule = initClusterDownRule();
        builtinRuleMap.put(clusterDown, clusterDownRule);
    }

    @Transactional
    public AlertRule initClusterDownRule() {
        QAlertRule alertRule = QAlertRule.alertRule;
        List<AlertRule> result = queryFactory.select(alertRule).from(alertRule)
                .where(alertRule.type.eq(AlertRule.RuleType.builtinKafka),
                        alertRule.name.eq(clusterDown), alertRule.target.eq(AlertRule.Target.Cluster)).fetchResults().getResults();
        AlertRule rule;
        if (result.isEmpty()) {
            rule = new AlertRule();
            rule.name = clusterDown;
            rule.description = String.format("Kafka集群中的Connector节点宕机超过%d分钟", clusterDownKeepMinute);
            rule.keepTime = clusterDownKeepMinute;
            rule.duration = StepDuration.M;
            rule.method = MetricMethod.None;
            rule.operator = "";
            rule.creator = "system";
            rule.createTime = LocalDateTime.now();
            rule.type = AlertRule.RuleType.builtinKafka;
            rule.target = AlertRule.Target.Cluster;
        } else {
            rule = result.get(0);
            if (!rule.keepTime.equals(clusterDownKeepMinute) && "system".equals(rule.lastModifier)) {
                rule.keepTime = clusterDownKeepMinute;
                rule.description = String.format("Kafka集群中的Connector节点宕机超过%d分钟", clusterDownKeepMinute);
                rule.lastModifier = "system";
                rule.updateTime = LocalDateTime.now();
            }
        }
        alertRuleRepository.save(rule);
        return rule;

    }

    @Transactional
    public AlertRule initRunFailRule() {
        QAlertRule alertRule = QAlertRule.alertRule;
        List<AlertRule> result = queryFactory.select(alertRule).from(alertRule).where(
                alertRule.type.eq(AlertRule.RuleType.builtinKafka), alertRule.name.eq(runFail),
                alertRule.target.eq(AlertRule.Target.Job)
        ).fetchResults().getResults();

        AlertRule rule;
        if (result.isEmpty()) {
            rule = new AlertRule();
            rule.name = runFail;
            rule.description = String.format("在Kafka集群中运行失败超过%d分钟", runFailKeepMinute);
            rule.keepTime = runFailKeepMinute;
            rule.duration = StepDuration.M;
            rule.method = MetricMethod.None;
            rule.operator = "";
            rule.creator = "system";
            rule.createTime = LocalDateTime.now();
            rule.type = AlertRule.RuleType.builtinKafka;
            rule.target = AlertRule.Target.Job;
        } else {
            rule = result.get(0);
            if (!rule.keepTime.equals(runFailKeepMinute) && "system".equals(rule.lastModifier)) {
                rule.keepTime = runFailKeepMinute;
                rule.description = String.format("在Kafka集群中运行失败超过%d分钟", runFailKeepMinute);
                rule.lastModifier = "system";
                rule.updateTime = LocalDateTime.now();
            }
        }
        alertRuleRepository.save(rule);
        return rule;
    }

    @Transactional
    public AlertRule initNoDataRule(String name, String metricName) {
        QAlertRule alertRule = QAlertRule.alertRule;
        List<AlertRule> result = queryFactory.select(alertRule).from(alertRule).where(
                alertRule.type.eq(AlertRule.RuleType.builtinKafka), alertRule.name.eq(runFail),
                alertRule.target.eq(AlertRule.Target.Job)
        ).fetchResults().getResults();

        AlertRule rule;

        if (result.size() != 0) {
            rule = result.get(0);
            if (!rule.keepTime.equals(noDataKeepMinute) && "system".equals(rule.lastModifier)) {
                rule.keepTime = noDataKeepMinute;
                rule.description = String.format("%d分钟没有数据处理", noDataKeepMinute);
                rule.lastModifier = "system";
                rule.updateTime = LocalDateTime.now();
            }
        } else {
            rule = new AlertRule();
            rule.name = name;
            rule.description = String.format("%d分钟没有数据处理", noDataKeepMinute);
            rule.metric = metricName;
            rule.method = MetricMethod.Counter_Increase;
            rule.operator = Operator.LE.name();
            rule.threshold = 0d;
            rule.keepTime = noDataKeepMinute;
            rule.duration = StepDuration.M;
            rule.creator = "system";
            rule.createTime = LocalDateTime.now();
            rule.type = AlertRule.RuleType.builtin;
            rule.target = AlertRule.Target.Job;
        }
        alertRuleRepository.save(rule);
        return rule;
    }

    @Transactional
    public AlertRule getRule(String name, AlertRule.RuleType type) {
        AlertRule rule = builtinRuleMap.get(name);
        if (rule == null) {
            QAlertRule alertRule = QAlertRule.alertRule;
            List<AlertRule> result = queryFactory.select(alertRule).from(alertRule).where(
                    alertRule.type.eq(AlertRule.RuleType.builtinKafka), alertRule.name.eq(runFail),
                    alertRule.target.eq(AlertRule.Target.Job)
            ).limit(1).fetchResults().getResults();
            rule = result.get(0);
            builtinRuleMap.put(rule.name, rule);
        }
        return rule;
    }
}
