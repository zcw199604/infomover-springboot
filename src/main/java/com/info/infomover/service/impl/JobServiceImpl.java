package com.info.infomover.service.impl;

import com.info.infomover.datasource.IDatasource;
import com.info.infomover.entity.*;
import com.info.infomover.repository.DataSourceRepository;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.service.JobService;
import com.info.infomover.util.*;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@Transactional
public class JobServiceImpl implements JobService {

    @Autowired
    private DataSourceRepository dataSourceRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    @Override
    @Transactional
    public List<Connector> findSourceConnectors(Job job) {

        List<Connector> list = new ArrayList<>();
        for (int i = 0; i < job.getConnectors().size(); i++) {
            Connector connector = new Connector();
            BeanUtils.copyProperties(job.getConnectors().get(i), connector);
            list.add(connector);
        }
        List<Connector> sourceConnectors = list.stream()
                .filter(connector -> connector.category == Connector.Category.Source)
                .collect(Collectors.toList());
        return sourceConnectors;
    }

    @Override
    @Transactional
    public List<Connector> findSinkConnectors(Job job) {
        List<Connector> list = new ArrayList<>();
        for (int i = 0; i < job.getConnectors().size(); i++) {
            Connector connector = new Connector();
            BeanUtils.copyProperties(job.getConnectors().get(i), connector);
            list.add(connector);
        }
        List<Connector> sinkConnectors = list.stream()
                .filter(connector -> connector.category == Connector.Category.Sink)
                .collect(Collectors.toList());
        return sinkConnectors;
    }


    @Override
    public Connector findOneSourceConnector(Job job) {
        if (CollectionUtils.isEmpty(job.getConnectors())) {
            throw new RuntimeException("job connectors is null");
        }
        List<Connector> sourceConnectors = job.getConnectors().stream()
                .filter(connector -> connector.category == Connector.Category.Source)
                .collect(Collectors.toList());

        CheckUtil.checkTrue(CollectionUtils.isEmpty(sourceConnectors),"job connectors does not contain source connector");
        CheckUtil.checkTrue(sourceConnectors.size() != 1,"job source connector size greater than 1 ");

        return sourceConnectors.get(0);
    }

    @Override
    public void configurateSaslSettings(Map<String, String> config) {
        String securityProtocol = config.get("security.protocol");
        String saslMechanism = config.get("sasl.mechanism");
        if(StringUtils.isNotBlank(securityProtocol) && StringUtils.isNotBlank(saslMechanism)) {
            String loginModule = config.get("login.module");
            String username = config.get("username");
            String password = config.get("password");
            String str = "%s required username='%s' password='%s';";
            String saslJaasConfig = String.format(str, loginModule, username, password);
            config.put("sasl.jaas.config", saslJaasConfig);
        }
    }

    @Override
    public void filterClusterStep(Job job) {
        List<StepDesc> clustersSteps = job.getSteps().stream().filter(item -> "clusters".equals(item.getScope())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(clustersSteps)) {
            return;
        }
        // clusters 节点
        StepDesc clusterDesc = clustersSteps.get(0);
        ConfigObject clusterConfigurations = clusterDesc.getOtherConfigurations();
        try {
            if (clusterConfigurations.containsKey(ClusterKeyWord.CLUSTER_ID)) {
                Object o = clusterConfigurations.get(ClusterKeyWord.CLUSTER_ID);
                Long clusterId = Long.valueOf(o.toString());
                // 已经发布后不允许修改集群信息
                CheckUtil.checkTrue(job.getDeployStatus() == Job.DeployStatus.DEPLOYED && (
                        job.getDeployClusterId() != null &&
                                clusterId.longValue() != job.getDeployClusterId().longValue()), "It is not allowed to modify the cluster information after deploy");

                String clusterName = clusterConfigurations.get(ClusterKeyWord.CLUSTER_NAME).toString();
                job.setDeployClusterId(clusterId);
                job.setDeployCluster(clusterName);
            }
        } catch (NumberFormatException | NullPointerException e) {
            throw new RuntimeException("cluster id mabay is empty or error");
        }
    }

    @Override
    public void replaceSpecialCharacters(Map config, String key) {
        if (!config.containsKey(key) || config.get(key) == null) {
            return;
        }
        String str = config.get(key).toString();
        if (StringUtils.isBlank(str)) {
            return;
        }
        if (str.contains("\"")) {
            str = str.replaceAll("\"", "");
            config.put(key, str);
        }
    }

    @Override
    public void generateConnectorBasedOnTable(Job job,String dbServer, Connector connector, List<ConfigObject> tableMappings) {
        job.getConnectors().remove(connector);
        for (Map<String, Object> tableMapping : tableMappings) {
            Map<String, String> configs = new HashMap<>();
            configs.putAll(connector.config);
            if (tableMapping.get("pk") != null && StringUtils.isNotBlank(tableMapping.get("pk").toString())) {
                String pk = tableMapping.get("pk").toString();
                configs.put("pk.fields", pk);
                configs.put("delete.enabled", "true");
                configs.put("pk.mode", "record_key");
                configs.put("insert.mode", "upsert");
            } else {
                configs.put("delete.enabled", "false");
                configs.put("pk.mode", "none");
            }

            String topics = TopicNameUtil.parseTopicName(job.getId(), job.getTopicPrefix()
                    , tableMapping.get("name").toString());

            configs.put("topics", topics);
            String name = tableMapping.get("name").toString();
            if (name.contains("\"")) {
                name = name.replaceAll("\"", "");
            }
            replaceSpecialCharacters(tableMapping, "alias");
            String newTab = tableMapping.getOrDefault("alias", name).toString();
            configs.put("table.name.format", newTab);

            if ("kafka".equals(connector.connectorType) && (Job.JobType.SYNC.equals(job.getJobType()) || (job.getJobType() ==
                    Job.JobType.COLLECT
                    && job.getSinkRealType() == Job.SinkRealType.EXTERNAL))) {
                configs.put("sink.topic", newTab);
            }

            Connector connectorSink = new Connector();
            connectorSink.name = connector.name + "_" + name;
            configs.put("name", connector.name + "_" + name);
            connectorSink.config = configs;
            connectorSink.category = connector.category;
            connectorSink.connectorType = connector.connectorType;
            job.getConnectors().add(connectorSink);
        }
    }

    @Override
    public void removeTransFormValue(Job job, Map<String, String> config, String typeClass) {
        if (config == null) {
            return;
        }
        switch (typeClass) {
            case "io.debezium.transforms.ExtractNewRecordState":
                config.remove(String.format("transforms.%s.type", job.getId()));
                config.remove(String.format("transforms.%s.delete.handling.mode", job.getId()));
                config.remove(String.format("transforms.%s.drop.tombstones", job.getId()));
                break;
            case "com.info.connect.transform.CustomerTopicRouter":
                config.remove(String.format("transforms.%s.regex", "topicRoute"));
                config.remove(String.format("transforms.%s.replacement", "topicRoute"));
                config.remove(String.format("transforms.%s.specialChar", "topicRoute"));
                config.remove(String.format("transforms.%s.type", "topicRoute"));
                break;
            default:
                break;
        }
    }

    @Override
    public void putTransFormValue(Job job, Map<String, String> config, String typeClass) {
        if (config == null) {
            return;
        }
        switch (typeClass) {
            case "io.debezium.transforms.ExtractNewRecordState":
                config.put(String.format("transforms.%s.type", job.getId()), "io.debezium.transforms.ExtractNewRecordState");
                config.put(String.format("transforms.%s.delete.handling.mode", job.getId()), "none");
                config.put(String.format("transforms.%s.drop.tombstones", job.getId()), "false");
                break;
            case "com.info.connect.transform.CustomerTopicRouter":
                String dbServer = this.findSourceConnectors(job).get(0).config.get("database.server.name");
                config.put(String.format("transforms.%s.regex", "topicRoute"), String.format("%s.(.*)", dbServer));
                config.put(String.format("transforms.%s.replacement", "topicRoute"), String.format("%s_%d_$1", job.getTopicPrefix(), job.getId()));
                config.put(String.format("transforms.%s.specialChar", "topicRoute"), "$,^,%,.");
                config.put(String.format("transforms.%s.type", "topicRoute"), "com.info.connect.transform.CustomerTopicRouter");
                break;
            default:
                break;
        }
    }

    @Override
    public void setTransformDefaultValue(Job job, Connector connector, String typeClass) {
        if (connector == null || connector.config == null) {
            return;
        }
        Object transFromKey = null;
        switch (typeClass) {
            case "io.debezium.transforms.ExtractNewRecordState":
                transFromKey = job.getId();
                break;
            case "com.info.connect.transform.CustomerTopicRouter":
                transFromKey = "topicRoute";
                break;
        }
        // 包含transform 需要判断用户是否已经添加
        if (connector.config.containsKey("transforms") &&
                StringUtils.isNotEmpty(connector.config.get("transforms"))) {
            String transform = connector.config.get("transforms");
            String[] split = transform.split(",");
            boolean notInclude = true;
            for (String transformKey : split) {
                // 用户已经配置 ExtractNewRecordState则不添加
                String transformValue = connector.config.get(StringUtils.join(new String[]{"transforms", transformKey, "type"}, "."));
                if (typeClass.equals(transformValue)) {
                    notInclude = false;
                }
            }
            if (notInclude) {
                connector.config.put("transforms", transform + "," + transFromKey);
                this.putTransFormValue(job,connector.config, typeClass);
            } else {
                // 已经包含 ExtractNewRecordState
                // 则需要移除默认添加的transform
                connector.config.put("transforms", transform.replace("," + transFromKey, ""));
                this.removeTransFormValue(job,connector.config, typeClass);
            }

        } else {
            connector.config.put("transforms", String.valueOf(transFromKey));
            this.putTransFormValue(job, connector.config, typeClass);
        }
    }

    @Override
    public void putConverters(Map<String, String> config) {
        config.put("converters", "timestampConverter");
        config.put("timestampConverter.type", "com.info.connect.convert.CustomerTimestampConverter");
        //config.put("timestampConverter.format.datetime", "YYYY-MM-dd HH:mm:ss.SSS");
        config.put("timestampConverter.format.timestamp", "YYYY-MM-dd HH:mm:ss.SSS");
    }

    @Override
    public void generateConnector(Job job) {
        List<Connector> sourceConnectors = this.findSourceConnectors(job);
        if (sourceConnectors.size() == 0) {
            return;
        }
        Connector firstSourceConnector = this.findSourceConnectors(job).get(0);
        // 处理 schema 上的"”
        replaceSpecialCharacters(firstSourceConnector.config, "database.include.list");
        replaceSpecialCharacters(firstSourceConnector.config, "schema.include.list");
        replaceSpecialCharacters(firstSourceConnector.config, "table.include.list");
        //firstSourceConnector.config.put("database.whitelist", firstSourceConnector.config.get("database.include.list"));
        //firstSourceConnector.config.put("table.whitelist", firstSourceConnector.config.get("table.include.list"));
        //firstSourceConnector.config.put("snapshot.include.collection.list", firstSourceConnector.config.get("table.include.list"));
        //database.history.store.only.monitored.tables.ddl
        //处理配置database.server.name,后期不允许界面配置，直接后端生成
        firstSourceConnector.getConfig().put("database.server.name", TopicNameUtil.parseInternalTopicName(job.getId(), job.getTopicPrefix()));
        if (job.getJobType() == Job.JobType.COLLECT ||
                (job.getJobType() == Job.JobType.SYNC && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
            // source 添加 timestampConverter
            putConverters(firstSourceConnector.config);
        }

        //判断并重组所有的transform
        this.setTransformDefaultValue(job,firstSourceConnector, "com.info.connect.transform.CustomerTopicRouter");

        if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
            String dbServer = firstSourceConnector.config.get("database.server.name");
            List<Connector> sinkConnectors = this.findSinkConnectors(job);

            if (CollectionUtils.isNotEmpty(sinkConnectors)) {
                for (Connector connector : sinkConnectors) {

                    this.setTransformDefaultValue(job, connector, "io.debezium.transforms.ExtractNewRecordState");
                    List<ConfigObject> tableMappings = connector.getTableMapping();
                    if (CollectionUtils.isEmpty(tableMappings)) {
                        throw new RuntimeException("tableMaping is empty");
                    }
                    this.generateConnectorBasedOnTable(job,dbServer, connector, tableMappings);
                }
            }
        }else if (job.getJobType() == Job.JobType.COLLECT) {
            //setTransformDefaultValue(firstSourceConnector);
            String databaseServerName = firstSourceConnector.getConfig().get("database.server.name");
            for (Connector connector : this.findSinkConnectors(job)) {
                Map<String, String> config = connector.getConfig();
                String id = config.get("id");
                config.put("dbServerName", databaseServerName + "_" + id);
                int databaseServerId = 10000 + new Random().nextInt(89999);
                config.put("database.server.id", String.valueOf(databaseServerId));
                if ("postgres".equals(firstSourceConnector.connectorType)) {
                    config.put("slot.name", config.get("slot.name") + "_" + UUID.randomUUID().toString().substring(0, 8));
                }
            }
        }
    }

    @Override
    public void spliceKeyWord(Job job) {
        List<Connector> sourceConnectors = this.findSourceConnectors(job);
        List<String> word = new ArrayList<>();
        word.add(job.getName());
        if (CollectionUtils.isNotEmpty(sourceConnectors) && sourceConnectors.size() == 1) {
            Connector connector = sourceConnectors.get(0);
            String connectorType = connector.connectorType;
            String dbName = null;
            if ("mysql".equals(connectorType) || "tidb".equals(connectorType)) {
                dbName = connector.getConfig().get("database.include.list");
            } else if ("oracle".equals(connectorType)) {
                dbName = connector.getConfig().get("schema.include.list");
            } else if ("postgres".equals(connectorType)) {
                dbName = connector.getConfig().get("schema.include.list");
            }
            if (StringUtils.isNotEmpty(dbName)) {
                String[] dbSplit = dbName.split(",");

                String tableInclude = connector.getConfig().get("table.include.list");
                if (StringUtils.isNotEmpty(tableInclude)) {
                    for (int i = 0; i < dbSplit.length; i++) {
                        word.add(tableInclude.replace(dbSplit[i] + ".", ""));
                    }
                }
                String dbHostName = connector.getConfig().get("database.hostname");
                word.add(dbHostName);
            }

        }
        job.setKeyWord(StringUtils.join(word, ","));
    }

    @Override
    public void stepsToConnector(Job job, String topicPrefix) {
        job.setTopicPrefix(topicPrefix);
        List<StepDesc> steps = job.getSteps();
        List<StepDesc> collect = steps.stream().filter(item -> Job.StepType.sources.name().equals(item.getScope())
                || Job.StepType.sinks.name().equals(item.getScope())).collect(Collectors.toList());

        this.filterClusterStep(job);

        if (job.getConnectors() == null) {
            job.setConnectors(new ArrayList<>(collect.size()));
        }
        String sourceConnectName = null;
        for(Connector conn : job.getConnectors()){
            if(conn.category == Connector.Category.Source){
                sourceConnectName = conn.name;
                break;
            }
        }

        job.getConnectors().clear();
        for (StepDesc stepDesc : collect) {
            Connector connector = new Connector();
            if(stepDesc.getScope().equals(Job.StepType.sources.name()) && sourceConnectName != null){
                connector.name = sourceConnectName;
            }else {
                connector.name = "connector_" + String.valueOf(job.getId()) + "_" + String.valueOf(stepDesc.getId());
            }
            connector.setTableMapping(stepDesc.getTableMapping());
            ConfigObject otherConfigurations = stepDesc.getOtherConfigurations();
            Map<String, String> configurations = new HashMap<>();
            for (String s : otherConfigurations.keySet()) {
                configurations.put(s, String.valueOf(otherConfigurations.get(s)));
            }
            connector.config = configurations;
            String datasourceIdStr = connector.config.get("datasource.id");
            if (datasourceIdStr != null) {
                Long datasourceId = Long.valueOf(datasourceIdStr);
                DataSource dataSource = dataSourceRepository.findById(datasourceId).get();
                if (dataSource == null) {
                    throw new RuntimeException(String.format("datasourceId %s is not exist", datasourceId));
                }
                //先取出所有数据源配置参数
                Map<String, String> config = new HashMap<>();
                config.putAll(dataSource.config);
                if (Job.StepType.sinks.name().equals(stepDesc.getScope())) {
                    IDatasource iDatasource = DatasourceUtil.transform2IDatasource(dataSource);
                    if (iDatasource.isJDBCSource()) {
                        config.put(SinkConnectorKeyword.SINK_URL, iDatasource.generateJDBCUrl());
                        config.put(SinkConnectorKeyword.SINK_USER, config.get("database.user"));
                        config.put(SinkConnectorKeyword.SINK_PASSWD, config.get("database.password"));
                        config.put("connector.class", "com.info.connect.jdbc.JdbcSinkConnector");
                    }else if(stepDesc.getType().equalsIgnoreCase("kafka")){
                        config.put("connector.class", "com.info.connect.kafka.sink.KafkaSinkConnector");
                        configurateSaslSettings(config);
                    }

                } else if (Job.StepType.sources.name().equals(stepDesc.getScope())) {
                    if ("mysql".equals(stepDesc.getType())) {
                        // mysql需要客户端标识id
                        int databaseServerId = 10000 + new Random().nextInt(89999);
                        config.put("database.server.id", String.valueOf(databaseServerId));
                        config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
                    } else if ("oracle".equals(stepDesc.getType())) {
                        config.put("connector.class", "io.debezium.connector.oracle.OracleConnector");
                    } else if ("postgres".equals(stepDesc.getType())) {
                        // pg 需要修改solt.name 为随机值
                        connector.config.remove("solt.name");
                        config.put("solt.name", "debezium_" + UUID.randomUUID().toString().replace("-", ""));
                        config.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
                    }
                }
                connector.config.putAll(config);
            }
            connector.connectorType = stepDesc.getType();
            if (Job.StepType.sources.name().equals(stepDesc.getScope())) {
                connector.category = Connector.Category.Source;
            } else if (Job.StepType.sinks.name().equals(stepDesc.getScope())) {
                connector.category = Connector.Category.Sink;
            }
            job.getConnectors().add(connector);
        }

        this.generateConnector(job);
        this.spliceKeyWord(job);
    }

    @Override
    public void setAllDeployTopics(Collection<String> collection,Job job) {
        if (job.getAllDeployTopics() == null) {
            job.setAllDeployTopics(new HashSet<>());
        }
        job.getAllDeployTopics().addAll(collection);
    }

    @Override
    @Transactional
    public Job findJobByIdAndLoadConnector(long jobId) {
        log.info("Find job {} and load all connectors", jobId);
        Job job = jobRepository.findById(jobId).get();
        job.getConnectors().size();//懒加载
        for (Connector connector : job.getConnectors()) {
            connector.getConfig().size();
        }
        return job;
    }

    @Override
    public Job findJobById(long jobId) {
        return jobRepository.findById(jobId).get();
    }
    @Transactional
    @Override
    public void saveJobSnapshot(long jobId, Job.SnapshotStatus snapshotStatus) {
        log.info("Update job {} snapshot status {}", jobId, snapshotStatus);
        Job job = jobRepository.findById(jobId).get();
        if (job != null) {
            job.setSnapshotStatus(snapshotStatus);
            jobRepository.saveAndFlush(job);
        } else {
            log.error("Not found job by id: {}", jobId);
        }
    }
    @Transactional
    @Override
    public void updateJobRecoveryStatus(long jobId, Job.RecoveryStatus runningStatus) {
        Job job = jobRepository.findById(jobId).get();
        if (job != null) {
            job.setRecoveryStatus(runningStatus);
            jobRepository.saveAndFlush(job);
        } else {
            log.error("Not found job by id: {}", jobId);
        }
    }
    @Transactional
    @Override
    public void updateJobRecollectStatus(long jobId, Job.RecollectStatus status) {
        Job job = jobRepository.findById(jobId).get();
        if (job != null) {
            job.setRecollectStatus(status);
            jobRepository.saveAndFlush(job);
        } else {
            log.error("Not found job by id: {}", jobId);
        }
    }

    @Transactional
    @Override
    public int updateJobRecollectStatusByWhere(long jobId, Job.RecollectStatus status) {
        Job job = jobRepository.findById(jobId).get();
        if (job != null && job.getRecollectStatus() != Job.RecollectStatus.RECOLLECTFAILED) {
            job.setRecollectStatus(status);
            jobRepository.saveAndFlush(job);
            return 1;
        } else {
            log.error("Not found job by id: {}", jobId);
            return 0;
        }
    }

    @Transactional
    @Override
    public void saveSteps(Long jobId, List<StepDesc> steps) {
        Job job = jobRepository.findById(jobId).get();
        if (job != null) {
            job.setSteps(steps);
            jobRepository.saveAndFlush(job);
        } else {
            log.error("Not found job by id: {}", jobId);
        }
    }

    @Transactional
    @Override
    public void updateJobDeployStatus(Long jobId, Job.DeployStatus deployStatus) {
        Job job = jobRepository.findById(jobId).get();
        if (job != null) {
            job.setDeployStatus(deployStatus);
            jobRepository.saveAndFlush(job);
        } else {
            log.error("Not found job by id: {}", jobId);
        }
    }

    @Override
    public List<Job> findByClusterIds(Long[] clusterIds) {
        QJob job = QJob.job;
        List<Job> results = queryFactory.select(job).from(job).where(job.deployClusterId.in(clusterIds)).fetchResults().getResults();
        return results;
    }
}
