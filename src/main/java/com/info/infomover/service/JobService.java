package com.info.infomover.service;

import com.info.infomover.entity.ConfigObject;
import com.info.infomover.entity.Connector;
import com.info.infomover.entity.Job;
import com.info.infomover.entity.StepDesc;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface JobService {
    public Connector findOneSourceConnector(Job job);

    public void setAllDeployTopics(Collection<String> collection, Job job);

    public List<Connector> findSourceConnectors(Job job);

    public List<Connector> findSinkConnectors(Job job);

    /**
     * 为kafka sink配置sasl信息
     *
     * @param config
     */
    public void configurateSaslSettings(Map<String, String> config);

    /**
     * 处理
     * 处理step里的 cluster 节点
     * 取出 otherConfigurations 里的 cluster.id 和 cluster.name
     *
     * @param job
     */
    public void filterClusterStep(Job job);

    /**
     * 抓取schema 和 table 时,如果有特殊字符 eg: -,#,! 会被 "" 包裹 导致debezium 创建 connect 时 出错(读取不到库表/ 创建topic出错)
     * 替换 config 里 key->value 的 \"
     *
     * @param config config
     * @param key    config_key
     */
    public void replaceSpecialCharacters(Map config, String key);

    /**
     * 根据 step 里的 tableMapping 生成多个sink connect
     *
     * @param dbServer
     * @param connector
     * @param tableMappings
     */
    public void generateConnectorBasedOnTable(Job job, String dbServer, Connector connector, List<ConfigObject> tableMappings);

    public void removeTransFormValue(Job job, Map<String, String> config, String typeClass);

    public void putTransFormValue(Job job, Map<String, String> config, String typeClass);

    /**
     * 检查是否需要添加默认transform
     * 1.用户已经配置transform,则需要检查是否已经配置 ExtractNewRecordState 没有则添加，有则需要拼接上
     * 2.用户没有配置transform,默认添加
     *
     * @param connector
     */
    public void setTransformDefaultValue(Job job, Connector connector, String typeClass);


    public void putConverters(Map<String, String> config);


    public void generateConnector(Job job);

    /**
     * 拼接查询关键字
     * @param job
     */
    public void spliceKeyWord(Job job);

    /**
     * 将datasource中配置转换成debezium connector中的配置
     *
     * @param topicPrefix
     */
    public void stepsToConnector(Job job, String topicPrefix);


    public Job findJobByIdAndLoadConnector(long jobId);

    public Job findJobById(long jobId);

    public void saveJobSnapshot(long jobId, Job.SnapshotStatus snapshotStatus);

    public void updateJobRecoveryStatus(long jobId, Job.RecoveryStatus runningStatus);

    public void updateJobRecollectStatus(long jobId, Job.RecollectStatus status);

    public int updateJobRecollectStatusByWhere(long jobId, Job.RecollectStatus status);

    public void saveSteps(Long jobId, List<StepDesc> steps);

    public void updateJobDeployStatus(Long jobId, Job.DeployStatus deployStatus);

    public List<Job> findByClusterIds(Long[] clusterIds);
}
