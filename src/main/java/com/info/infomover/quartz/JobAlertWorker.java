package com.info.infomover.quartz;

import com.info.infomover.entity.*;
import com.info.infomover.prom.PromClient;
import com.info.infomover.prom.constants.MetricMethod;
import com.info.infomover.prom.constants.Operator;
import com.info.infomover.prom.query.PromFactor;
import com.info.infomover.prom.query.PromQuery;
import com.info.infomover.prom.request.QueryRequest;
import com.info.infomover.prom.response.PromResult;
import com.info.infomover.repository.*;
import com.info.infomover.service.AlertService;
import com.info.infomover.service.JobService;
import com.info.infomover.util.ConnectorUtil;
import com.io.debezium.configserver.model.ConnectorStatus;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import javax.inject.Singleton;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Singleton
public class JobAlertWorker {
    private static final Logger logger = LoggerFactory.getLogger(JobAlertWorker.class);
    
    private static final String SOURCE_METRIC_PREFIX = "debezium_metrics_";
    
    private LocalDateTime executeTime;
    
    @Autowired
    PromClient promClient;

    @Autowired
    private ConnectorRepository connectorRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private ServiceInstanceRepository serviceInstanceRepository;

    @Autowired
    private AlertService alertService;

    @Autowired
    private JobAlertRuleRepository jobAlertRuleRepository;

    @Autowired
    private AlertRuleRepository alertRuleRepository;

    @Autowired
    private AlertRuleInitializer alertRuleInitializer;

    /**
     * 检查任务关联的连接器是否符合告警规则
     * @param job
     */
    public void execute(Job job) {
        executeTime = LocalDateTime.now();
        List<AlertRule> ruleList = loadAndCacheRule(job.getId());
        
        Long cluster = job.getDeployClusterId();
        String clusterName = job.getDeployCluster();
        boolean sourceAlert = false;

        Connector sourceConnector = jobService.findOneSourceConnector(job);
        List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
        
        if (Job.JobType.SYNC.equals(job.getJobType())) {
            String dbServerName = sourceConnector.getConfig().get("database.server.name");
            sourceAlert = sourceAlert(job, sourceConnector, cluster, clusterName, ruleList, dbServerName);
        } else if (Job.JobType.COLLECT.equals(job.getJobType())) {
            if (Job.SinkRealType.EXTERNAL.equals(job.getSinkRealType())) {
                String dbServerName = sourceConnector.getConfig().get("database.server.name");
                
                sourceAlert = sourceAlert(job, sourceConnector, cluster, clusterName, ruleList, dbServerName);
            } else {
                for (Connector sinkConnector : sinkConnectors) {
                    cluster = Long.parseLong(sinkConnector.getConfig().get("id"));
                    clusterName = sinkConnector.getConfig().get("name");
                    String dbServerName = sinkConnector.getConfig().get("dbServerName");
                    sourceAlert = sourceAlert(job, sourceConnector, cluster, clusterName, ruleList, dbServerName);
                }
            }
        }
        
        // sourceAlert == false 表示sourceConnector运行正常，检查sinkConnector; 否则不检查sinkConnector
        if (!sourceAlert) {
            if (Job.JobType.SYNC.equals(job.getJobType())) {
                AlertRule jdbcSinkRule = AlertRuleInitializer.builtinRuleMap.get(AlertRuleInitializer.jdbcSinkNoData);
                for (Connector sinkConnector : sinkConnectors) {
                    if ("true".equals(sinkConnector.getConfig().getOrDefault("metrics.enabled", "false"))) {
                        sinkAlert(job, sinkConnector, jdbcSinkRule, job.getDeployClusterId(), job.getDeployCluster());
                    }
                }
            } else if (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.EXTERNAL.equals(job.getSinkRealType())) {
                AlertRule kafkaSinkRule = AlertRuleInitializer.builtinRuleMap.get(AlertRuleInitializer.kafkaSinkNoData);
                for (Connector sinkConnector : sinkConnectors) {
                    if ("true".equals(sinkConnector.getConfig().getOrDefault("metrics.enabled", "false"))) {
                        cluster = job.getDeployClusterId();
                        clusterName = job.getDeployCluster();
                        sinkAlert(job, sinkConnector, kafkaSinkRule, cluster, clusterName);
                    }
                }
            }
        }
    }

    /**
     * 检查目标sink类型的连接器是否触发告警
     * @param job 任务
     * @param connector 连接器
     * @param rule 告警规则
     * @param cluster kafka集群ID
     * @param clusterName kafka集群名称
     */
    private void sinkAlert(Job job, Connector connector, AlertRule rule, Long cluster, String clusterName) {
        if (isPause(job, connector)) return;
        
        Alert alert = initAlert(job, connector, cluster, clusterName);
        
        Alert.AlertStatus status = null;
        
        PromQuery promQuery = initSinkPromQuery(rule, connector.name);
        
        ConnectorStatus.State runState = runStateInCluster(cluster, connector.name);
        if (ConnectorStatus.State.RUNNING.equals(runState)) {
            if (notExistRunFail(alert)) {
                status = doCheckByPromSingle(promQuery, job.getName(), connector.name);
                alert.name = rule.name;
                alert.ruleId = rule.getId();
                alert.ruleName = rule.name;
                alert.description = rule.description;
                if (status==null) {
                    ignoreOrResolveAlert(alert, job);
                }
            } else {
                ignoreOrResolveAlert(alert, job);
            }
        } else if (ConnectorStatus.State.FAILED.equals(runState)) {
            status = Alert.AlertStatus.pending;
        }
        persistAlert(job, alert, status);
    }

    /**
     * 检查目标连接器是否触发告警;
     * @param job 任务
     * @param connector 连接器
     * @param cluster kafka集群ID
     * @param clusterName kafka集群名称
     * @param rules 告警规则
     * @param dbServerName Prometheus metric {name=''} 条件值
     * @return true-触发告警；false-运行正常
     */
    private boolean sourceAlert(Job job, Connector connector, Long cluster, String clusterName, List<AlertRule> rules, String dbServerName) {
        if (isPause(job, connector)) return false;
        
        Alert alert = initAlert(job, connector, cluster, clusterName);
        
        Alert.AlertStatus status = null;
        
        ConnectorStatus.State runState = runStateInCluster(cluster, connector.name);
        if (ConnectorStatus.State.RUNNING.equals(runState) ) {
            if (notExistRunFail(alert)) {
                status = checkByPromBatch(job, dbServerName, rules, alert);
            } else {
                ignoreOrResolveAlert(alert, job);
            }
        } else if (ConnectorStatus.State.FAILED.equals(runState)) {
            status = Alert.AlertStatus.pending;
        } else {
            return false;
        }
        
        persistAlert(job, alert, status);
        return status != null;
    }
    
    @Transactional
    public void ignoreOrResolveAlert(Alert alert, Job job) {
        alert.status = Alert.AlertStatus.firing;

        List<Alert> alerts = alertService.findByAlert(alert);
        if (alerts != null && alerts.size() > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("resolve firing alert Job:{} Connector:{} Cluster:{} Status:{}",
                        alert.job, alert.connector, alert.cluster, alert.status.name());
            }
            for (Alert existsAlert : alerts) {
                existsAlert.updateTime = executeTime;
                existsAlert.lastModifierId = job.getCreatorId();
                existsAlert.lastModifier = job.getCreatorChineseName();
                existsAlert.status = Alert.AlertStatus.resolved;
            }
        }
    
        alert.status = Alert.AlertStatus.pending;
        alerts = alertService.findByAlert(alert);
        if (alerts != null && alerts.size() > 0) {
            for (Alert existsAlert : alerts) {
                if (logger.isDebugEnabled()) {
                    logger.debug("ignore firing alert Job:{} Connector:{} Cluster:{} Status:{}",
                            alert.job, alert.connector, alert.cluster, alert.status.name());
                }
                existsAlert.updateTime = executeTime;
                existsAlert.lastModifierId = job.getCreatorId();
                existsAlert.lastModifier = job.getCreatorChineseName();
                existsAlert.status = Alert.AlertStatus.ignore;
            }
        }
    }
    

    /**
     * 检查pending状态的RunFail 告警是否存在
     */
    @Transactional
    public boolean notExistRunFail(Alert runFailAlert) {
        return alertService.findByAlert(runFailAlert).isEmpty();
    }
    
    private Alert.AlertStatus checkByPromBatch(Job job, String dbServerName, List<AlertRule> rules, Alert alert) {
        Alert.AlertStatus status;
        for (AlertRule rule : rules) {
            PromQuery promQuery = initSourcePromQuery(rule, dbServerName);
            
            status = doCheckByPromSingle(promQuery, job.getName(), alert.connector);
    
            alert.name = rule.name;
            alert.description = rule.description;
            alert.ruleId = rule.getId();
            alert.ruleName = rule.name;
            if (status != null) {
                return status;
            } else {
                ignoreOrResolveAlert(alert, job);
            }
        }
        return null;
    }
    
    private Alert.AlertStatus doCheckByPromSingle(PromQuery promQuery, String jobName, String connectorName) {
        QueryRequest request = QueryRequest.build(promQuery, System.currentTimeMillis() / 1000);
        try {
            PromResult promResult = promClient.query(request).readEntity(PromResult.class);
            if (promResult != null && "success".equals(promResult.getStatus())) {
                PromResult.CommonData data = promResult.getData();
                if (data.getResult() != null && data.getResult().size() > 0) {
                    for (PromResult.Result result : data.getResult()) {
                        if (result.getValue() != null && result.getValue().size() > 0) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Job:{} Connector:{}query:{}", jobName, connectorName, request.query);
                            }
                            return Alert.AlertStatus.firing;
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Job:{} Connector:{} get null from prometheus then alert resolve", jobName, connectorName);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Alert Rule maybe error ", e);
        }
        return null;
    }
    
    @Transactional
    public void persistAlert(Job job, Alert alert, Alert.AlertStatus status) {
        alert.status = Alert.AlertStatus.firing;
        if (status != null) {
            List<Alert> firingAlerts = alertService.findByAlert(alert);
            if (firingAlerts==null || firingAlerts.size()<=0) {
                if (Alert.AlertStatus.pending.equals(status)) {
                    alert.status = Alert.AlertStatus.pending;
                    List<Alert> alerts = alertService.findByAlert(alert);
                    if (alerts != null && alerts.size() > 0) {
                        Alert existsAlert = alerts.get(0);
                        AlertRule runFailRule = alertRuleInitializer.getRule(AlertRuleInitializer.runFail, AlertRule.RuleType.builtinKafka);
                        if (existsAlert.createTime
                                .plus(runFailRule.keepTime * runFailRule.duration.ms, ChronoUnit.MILLIS)
                                .isBefore(executeTime)) {
    
                            if (logger.isDebugEnabled()) {
                                logger.debug("alert change status to firing Job:{} Connector:{} Cluster:{} Status:{}",
                                        alert.job, alert.connector, alert.cluster, alert.status.name());
                            }
                            
                            existsAlert.status = Alert.AlertStatus.firing;
                            existsAlert.updateTime = executeTime;
                            existsAlert.lastModifierId = job.getCreatorId();
                            existsAlert.lastModifier = job.getCreatorChineseName();
                            alertRepository.save(existsAlert);
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("first firing alert Job:{} Connector:{} Cluster:{} Status:{}",
                                    alert.job, alert.connector, alert.cluster, alert.status.name());
                        }
                        alertRepository.save(alert);
                    }
                } else if (Alert.AlertStatus.firing.equals(status)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("first firing alert Job:{} Connector:{} Cluster:{} Status:{}",
                                alert.job, alert.connector, alert.cluster, alert.status.name());
                    }
                    alert.status = status;
                    alertRepository.save(alert);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("update firing alert Job:{} Connector:{} Cluster:{} Status:{}",
                            alert.job, alert.connector, alert.cluster, alert.status.name());
                }
                Alert existsAlert = firingAlerts.get(0);
                existsAlert.updateTime = executeTime;
                alertRepository.saveAndFlush(existsAlert);
            }
        } else {
            AlertRule runFailRule = alertRuleInitializer.getRule(AlertRuleInitializer.runFail, AlertRule.RuleType.builtinKafka);
            alert.name = runFailRule.name;
            alert.ruleName = runFailRule.name;
            alert.description = runFailRule.description;
            alert.ruleId = runFailRule.getId();
            if (logger.isDebugEnabled()) {
                logger.debug("ignore or resolve alert Job:{} Connector:{} Cluster:{} Status:{}",
                        alert.job, alert.connector, alert.cluster, alert.status.name());
            }
            ignoreOrResolveAlert(alert, job);
        }
    }
    
    private PromQuery initSourcePromQuery(AlertRule rule, String dbServerName) {
        String metric = SOURCE_METRIC_PREFIX + rule.metric;
        String method = rule.method.methodName;
        Integer keepTime = rule.keepTime;
        String operator = "";
        if (StringUtils.isNotBlank(rule.operator)) {
            operator = Operator.valueOf(rule.operator).symbol;
        }
        if (MetricMethod.Common_Absent.equals(rule.method) || MetricMethod.None.equals(rule.method)) {
            keepTime = 0;
        }
        Double threshold = rule.threshold;
        PromFactor nameFactor = PromFactor.factor("name", dbServerName);
        PromFactor contextFactor = PromFactor.factor("context", "streaming");
        
        return PromQuery.withMethod(metric, method, keepTime, rule.duration.duration, operator, threshold,
                nameFactor, contextFactor);
    }
    
    private PromQuery initSinkPromQuery(AlertRule rule, String connectorName) {
        String operator = "";
        if (StringUtils.isNotBlank(rule.operator)) {
            operator = Operator.valueOf(rule.operator).symbol;
        }
        
        PromFactor nameFactor = PromFactor.factor("name", connectorName);
        return PromQuery.withMethod(rule.metric, rule.method.methodName, rule.keepTime, rule.duration.duration,
                operator, rule.threshold, nameFactor);
    }

    /**
     * 检查在Kafka集群中的运行状态
     * @param cluster
     * @param connectorName
     * @return 运行状态
     */
    private ConnectorStatus.State runStateInCluster(Long cluster, String connectorName) {
        try {
            return ConnectorUtil.connectorStatus(cluster, connectorName);
        } catch (IOException e) {
            logger.error("Cluster:{} Connector:{} get running state from kafka error, then state set DESTROYED : {}",
                    cluster, connectorName, e.getMessage());
            return ConnectorStatus.State.DESTROYED;
        }
    }

    /**
     * 初始化Alert信息
     * @param job 任务
     * @param connector 连接器
     * @param cluster kafka集群ID
     * @param clusterName kafka集群名称
     * @return
     */

    private Alert initAlert(Job job, Connector connector, Long cluster, String clusterName) {
        AlertRule runFailRule = alertRuleInitializer.getRule(AlertRuleInitializer.runFail, AlertRule.RuleType.builtinKafka);
        Alert alert = new Alert();
        alert.job = job.getName();
        alert.jobId = job.getId();
        alert.connector = connector.name;
        alert.category = connector.category;
        alert.clusterId = cluster;
        alert.cluster = clusterName;
        alert.creator = job.getCreatorChineseName();
        alert.creatorId = job.getCreatorId();
        alert.createTime = executeTime;
        alert.plugin = connector.connectorType;
        alert.name = runFailRule.name;
        alert.description = runFailRule.description;
        alert.ruleName = runFailRule.name;
        alert.updateTime = alert.createTime;
        alert.status = Alert.AlertStatus.pending;
        alert.type = Alert.Type.Job;
        
        return alert;
    }
    
    @Transactional
    public List<AlertRule> loadAndCacheRule(Long jobId) {
        List<JobAlertRule> jobAlertRules = jobAlertRuleRepository.findByJobId(jobId);
        List<AlertRule> ruleList = new ArrayList<>();
        if (jobAlertRules != null && jobAlertRules.size() > 0) {
            for (JobAlertRule jar : jobAlertRules) {
                AlertRule rule = JobAlertManager.ruleMap.computeIfAbsent(jar.ruleId, s ->
                        alertRuleRepository.findById(jar.ruleId).get());
                if (rule != null) {
                    rule.priority = jar.priority;
                    ruleList.add(rule);
                } else {
                    jobAlertRuleRepository.delete(jar);
                }
            }
        }
        
        if (ruleList.size() > 0) {
            ruleList.sort(Comparator.comparing(rule -> rule.priority));
            return ruleList;
        } else {
            return Collections.singletonList(alertRuleInitializer.getRule(AlertRuleInitializer.noDataSeen, AlertRule.RuleType.builtin));
        }
    }
    
    private boolean isPause(Job job, Connector connector) {
        String cluster = "";
        String key ;
        if (Job.JobType.SYNC.equals(job.getJobType()) || (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.EXTERNAL.equals(job.getSinkRealType()))) {
            cluster = String.valueOf(job.getDeployClusterId());
        } else if (Job.JobType.COLLECT.equals(job.getJobType())) {
            cluster = connector.getConfig().get("id");
        }
        key = connector.name + "_" + job.getName() + "_" + connector.category.name() + cluster;
        if (JobAlertManager.pauseAlertConnector.contains(key)) {
            return true;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("alert manager : Job:{} Connector:{}", job.getName(), connector.name);
        }
        return false;
    }
}
