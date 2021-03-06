package com.info.infomover.resources;

import com.info.baymax.common.utils.JsonUtils;
import com.info.infomover.common.Log;
import com.info.infomover.common.LogConst;
import com.info.infomover.common.Logged;
import com.info.infomover.entity.*;
import com.info.infomover.param.JobDeployParam;
import com.info.infomover.recollect.RecollectJobLauncher;
import com.info.infomover.repository.*;
import com.info.infomover.resources.response.ActionStatus;
import com.info.infomover.security.SecurityUtils;
import com.info.infomover.service.ConnectorService;
import com.info.infomover.service.JobService;
import com.info.infomover.util.*;
import com.io.debezium.configserver.model.ConnectConnectorConfigResponse;
import com.io.debezium.configserver.model.ConnectConnectorStatusResponse;
import com.io.debezium.configserver.model.ConnectorStatus;
import com.io.debezium.configserver.rest.client.KafkaConnectClient;
import com.io.debezium.configserver.rest.client.KafkaConnectClientFactory;
import com.io.debezium.configserver.rest.client.KafkaConnectException;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import io.debezium.relational.history.KafkaDatabaseHistory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.jboss.resteasy.client.exception.ResteasyWebApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@RequestMapping("/api/job")
@Controller
@ResponseBody
@Logged
//@Tag(name = "infomover job", description = "??????????????????")
public class JobResource {

    private static final Logger logger = LoggerFactory.getLogger(JobResource.class);
    private static final String SINK_METRICS_ENABLED_KEY = "metrics.enabled";
    private static final String METRICS_EXPORT_PORT_KEY = "metrics.export.port";

    @Value("${sink.metrics.export.port.jdbc:8008}")
    private String sinkMetricsExportPortJdbc = "8008";

    @Value("${sink.metrics.export.port.kafka:8009}")
    private String sinkMetricsExportPortKafka = "8009";

    @Value("${infomover.schema.monitor.enable:true}")
    private boolean monitorSchemaEnable;

    @Value("${infomover.kafka.bootstrap-servers:localhost:9092}")
    private String kafkaBrokers;

    @Value("${infomover.consumer.securityProtocol:NONE}")
    private String securityProtocol = "";

    @Value("${infomover.consumer.saslMechanism:NONE}")
    private String saslMechanism = "";

    @Value("${infomover.consumer.saslJaasConfig:NONE}")
    private String jaasConfig = "";

    @Value("${topic.clean.policy:update_offset}")
    private String topicCleanpolicy;

    @Value("${infomover.schema.monitor.topic:infomover.db.schema.change}")
    private String schemaTopic;

    @Value("${quarkus.application.name:infomover}")
    private String topicPrefix;

    Pattern saslPattern = Pattern.compile("AES\\(.*\\)");

    /*@Inject
    SchemaChangeProcessor schemaChangeProcessor;*/

    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private DataSourceRepository dataSourceRepository;

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private ConnectorRepository connectorRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private JobAlertRuleRepository jobAlertRuleRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    private ExecutorService executorService = new ThreadPoolExecutor(3, 10, 1L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
    private ExecutorService recollectExecutorService = new ThreadPoolExecutor(5, 20, 1L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
    private ExecutorService checktaskExecutorService = new ThreadPoolExecutor(5, 20, 1L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());

    private static final JsonBuilder jsonBuilder = JsonBuilder.getInstance();


    @GetMapping
    /*@Operation(description = "????????????")
    @Parameters({
            @Parameter(name = "pageNum", description = "??????????????????", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "??????????????????", in = ParameterIn.QUERY),
            @Parameter(name = "deploy_status", description = "????????????", in = ParameterIn.QUERY),
            @Parameter(name = "name", description = "?????????", in = ParameterIn.QUERY),
            @Parameter(name = "creator_name", description = "??????????????????", in = ParameterIn.QUERY),
            @Parameter(name = "after", description = "??????????????????", in = ParameterIn.QUERY),
            @Parameter(name = "before", description = "??????????????????", in = ParameterIn.QUERY),
            @Parameter(name = "sortby", description = "????????????", in = ParameterIn.QUERY),
            @Parameter(name = "sortdirection", description = "????????????", in = ParameterIn.QUERY),
    })*/
    @Log(action = "query", description = "job list query", itemId = "#name", param = "#status")
    public Response list(/* */@RequestParam(value = "pageNum",defaultValue = "1",required = false) int page, @RequestParam(value = "pageSize",defaultValue = "10",required = false) int limit,
                              @RequestParam(value = "deploy_status",required = false) List<Job.DeployStatus> status, @RequestParam(value = "name",required = false)
                                      String name, @RequestParam(value = "keyWord",required = false) String keyWord,
                              @RequestParam(value = "creator_name",required = false) String creatorName,
                              @RequestParam(value = "owner",required = false) String owner,
                              @RequestParam(value = "after",required = false) String createAfter, @RequestParam(value = "before",required = false) String createBefore,
                              @RequestParam(value = "jobType",required = false) Job.JobType jobType,
                              @RequestParam(value = "sourceCategory",required = false) String sourceCategory,
                              @RequestParam(value = "sinkCategory",required = false) String sinkCategory,
                              @RequestParam(value = "sortby",defaultValue = "createTime",required = false) String sortBy,
                              @RequestParam(value = "sortdirection",defaultValue = "DESC",required = false) Sort.Direction sortDirection) {

        QJob jobT = QJob.job;
        JPAQuery<Job> from = queryFactory.select(jobT).from(jobT);
        if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
            User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
            if (user == null) {
                throw new RuntimeException("no user found with name " + User.Role.User.name().equals(user.getRole()));
            }
            from.where(jobT.creatorId.eq(user.getId()));
        }

        if (status!=null && !status.isEmpty()) {
            from.where(jobT.deployStatus.in(status));
        }

        if (!StringUtils.isEmpty(name)) {
            from.where(jobT.name.like("%" + name + "%"));
        }

        if (!StringUtils.isEmpty(keyWord)) {
            from.where(jobT.keyWord.like("%" + keyWord + "%").or(jobT.name.like("%" + keyWord + "%")));
        }

        if (!StringUtils.isEmpty(creatorName)) {
            from.where(jobT.creatorChineseName.like("%" + creatorName + "%"));
        }

        if (!StringUtils.isEmpty(owner)) {
            from.where(jobT.owner.like("%" + owner + "%"));
        }

        if (jobType != null) {
            from.where(jobT.jobType.eq(jobType));
        }

        if (!StringUtils.isEmpty(sourceCategory)) {
            from.where(jobT.sourceCategory.eq(sourceCategory));
        }
        if (!StringUtils.isEmpty(sinkCategory)) {
            from.where(jobT.sinkCategory.eq(sinkCategory));
        }

        if (!StringUtils.isEmpty(createAfter)) {
            from.where(jobT.createTime.after(LocalDate.parse(createAfter).atStartOfDay()));
        }
        if (!StringUtils.isEmpty(createBefore)) {
            from.where(jobT.createTime.before(LocalDate.parse(createBefore).atStartOfDay()));
        }

        Order order = sortDirection.isAscending() ? Order.ASC : Order.DESC;
        com.querydsl.core.types.Path<Object> fieldPath = com.querydsl.core.types.dsl.Expressions.path(Object.class, jobT, sortBy);
        from.orderBy(new OrderSpecifier(order, fieldPath));

        QueryResults<Job> jobQueryResults = from.offset(Page.getOffest(page,limit)).limit(limit).fetchResults();
        Page<Job> list = Page.of(jobQueryResults.getResults(), jobQueryResults.getTotal(), jobQueryResults.getLimit());

        for (Job job : list.content) {
            String project = job.getProject();
            Map<String, Object> alertParam = new HashMap<>();
            alertParam.put("jobId", job.getId());
            alertParam.put("status", Alert.AlertStatus.firing);
            alertParam.put("updateTime", LocalDateTime.of(LocalDate.now(), LocalTime.MIN));
            QAlert alert = QAlert.alert;
            long alertCount = queryFactory.select(alert).from(alert).where(alert.jobId.eq(job.getId()))
                    .where(alert.status.eq(Alert.AlertStatus.firing))
                    .where(alert.updateTime.before(LocalDateTime.of(LocalDate.now(), LocalTime.MIN))).fetchCount();

            job.setAlertCount(alertCount);
            job.setNote(null);
            job.setSteps(null);
            job.setConnectors(null);
            job.setLinks(null);
        }
        return Response.ok(list).build();
    }

    @GetMapping("/{jobId}")
    /*@Operation(description = "????????????")
    @Path("/{jobId}")
    @Parameters({
            @Parameter(name = "jobId", description = "??????id", required = true, in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    @Log(action = LogConst.ACTION_QUERY, description = "??????jobId??????", itemId = "#jobId")
    public Response getJob( @PathVariable("jobId") Long jobId) {
        Job job = jobRepository.findById(jobId).get();
        if (job == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
            User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
            if (user == null) {
                throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
            }
            if (user.getId().longValue() != job.getCreatorId().longValue()) {
                throw new RuntimeException("no available connector for user " + user.chineseName);
            }
        }
        return Response.ok(job).build();
    }

    @PostMapping
    @Transactional
    @RolesAllowed({"User", "Admin"})
    public Response create( @RequestBody Job job) {
        User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        if (jobRepository.countByName(job.getName()) > 0) {
            throw new RuntimeException("job name " + job.getName() + " is existed");
        }
        job.setId(null);
        job.setDeployStatus(Job.DeployStatus.UN_DEPLOYED);
        job.setCreatorId(user.getId());
        job.setKeyWord(job.getName());
        job.setCreatorChineseName(user.chineseName);
        job.setCreateTime(LocalDateTime.now());
        job.setLastModifier(user.chineseName);
        job.setUpdateTime(LocalDateTime.now());
        if (job.getJobType() == Job.JobType.SYNC) {
            job.setSinkRealType(Job.SinkRealType.NONE);

        }
        this.addSinkAndLinkDefaultValue(job);
        jobRepository.saveAndFlush(job);
        return Response.ok(job).build();
    }

    /**
     * ??????sourceCategory ???sinkCategory ????????????step link ??????
     *
     * @param job
     */
    private void addSinkAndLinkDefaultValue(Job job) {
        if (StringUtils.isEmpty(job.getSourceCategory()) || StringUtils.isEmpty(job.getSinkCategory())) {
            return;
        }

        List<StepDesc> steps = new ArrayList<>();
        int index = 1;
        String sourceStepId = job.getSourceCategory() + "_" + String.valueOf(index);
        steps.add(this.initStepDesc(sourceStepId, "sources", job.getSourceCategory(), 150, 15, "output"));
        if (job.getSourceCategory().equals(job.getSinkCategory())) {
            index++;
        }

        String sinkStepId = job.getSinkCategory() + "_" + String.valueOf(index);
        steps.add(this.initStepDesc(sinkStepId, "sinks", job.getSinkCategory(), 550, 15, "input"));

        // ???????????????cluster????????????????????????????????????
        // ????????? ???????????????sink?????????
        steps.add(this.initStepDesc("clusters_1", "clusters", "cluster", 950, 15, "input"));


        job.setSteps(steps);
        List<LinkDesc> links = new ArrayList<>();
        LinkDesc linkDesc = new LinkDesc();
        linkDesc.setSource(sourceStepId);
        linkDesc.setSourceOutput("output");
        linkDesc.setTarget(sinkStepId);
        linkDesc.setTargetInput("input");
        links.add(linkDesc);
        job.setLinks(links);
    }

    private StepDesc initStepDesc(String id, String scope, String category, int x, int y, String put) {
        StepDesc stepDesc = new StepDesc();
        stepDesc.setId(id);
        stepDesc.setScope(scope);
        stepDesc.setType(category);
        stepDesc.setName(category);
        stepDesc.setX(x);
        stepDesc.setY(y);
        ConfigObject configObject = new ConfigObject();
        if ("sinks".equals(scope)) {
            configObject.put(put, new String[]{put});
            configObject.put("output", new String[]{"output"});
        } else {
            configObject.put(put, new String[]{put});
        }
        stepDesc.setUiConfigurations(configObject);
        stepDesc.setOtherConfigurations(new ConfigObject());
        ConfigObject transform = new ConfigObject();
        ConfigObject transform_0 = new ConfigObject();
        transform_0.put("name", "");
        transform_0.put("type", "");
        transform.put("??????_0", transform_0);
        stepDesc.setTransform(transform);

        ConfigObject filter = new ConfigObject();
        ConfigObject filter_0 = new ConfigObject();
        ConfigObject tables = new ConfigObject();
        tables.put("leftFields", new String[]{});
        tables.put("rightFields", new String[]{});

        filter_0.put("database", "");
        filter_0.put("tables", tables);

        filter.put("??????_0", filter_0);
        stepDesc.setTransform(transform);
        stepDesc.setFilter(filter);
        return stepDesc;
    }

    @PutMapping
    @Transactional
    @RolesAllowed({"User", "Admin"})
    @Log(action = LogConst.ACTION_UPDATE, itemId = "#job.id", description = "??????job??????", param = "#job.name")
    public Response update( @RequestBody Job job) {
        User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        //??????datasource????????????Connector???
        jobService.stepsToConnector(job, topicPrefix);
        //???sink connector??????????????????deploy connector

        long sourceCount = jobService.findSourceConnectors(job).size();
        if (sourceCount != 1L) {
            throw new RuntimeException("source connector count can only be 1");
        }
        if (job.getId() == null) {
            throw new RuntimeException("job id is empty");
        }

        Job job1 = jobRepository.findById(job.getId()).get();
        if (job1 == null) {
            logger.error("job id not found in DB");
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", "job id not found in DB");
            return Response.status(Response.Status.NOT_FOUND).entity(map).build();
        }

        if (!job1.getName().equals(job.getName())) {
            if (jobRepository.countByName(job.getName()) > 0) {
                throw new RuntimeException("job name " + job.getName() + " is existed");
            }
            job1.setName(job.getName());
        }
        job1.setSteps(job.getSteps());
        job1.setLinks(job.getLinks());
        job1.setKeyWord(job.getKeyWord());
        job1.setDeployCluster(job.getDeployCluster());
        job1.setDeployClusterId(job.getDeployClusterId());
        Map<String, Connector> oriCon = job1.getConnectors().stream().collect(Collectors.toMap(Connector::getName, connector -> connector));
        Map<String, Connector> newCon = job.getConnectors().stream().collect(Collectors.toMap(Connector::getName, connector -> connector));

        for (Connector connector : job.getConnectors()) {
            if (oriCon.containsKey(connector.getName())) {
                Connector connector1 = oriCon.get(connector.getName());
                connector1.config.clear();
                connector1.config.putAll(connector.config);
                connector1.setTableMapping(connector.getTableMapping());
            } else {
                oriCon.put(connector.name, connector);
            }
        }
        for (Connector connector : job1.getConnectors()) {
            if (!newCon.containsKey(connector.name)) {
                connectorRepository.delete(connector);
                oriCon.remove(connector.name);
            }
        }
        List<Connector> tmp = oriCon.entrySet().stream().map(item -> item.getValue()).collect(Collectors.toList());
        job1.getConnectors().clear();
        job1.getConnectors().addAll(tmp);

        job1.setLastModifier(user.chineseName);
        job1.setUpdateTime(LocalDateTime.now());
        if (job1.getDeployClusterId() != null && (job1.getDeployStatus() == Job.DeployStatus.DEPLOYED || job1.getDeployStatus() == Job.DeployStatus.PAUSED)) {
            for (Connector con : jobService.findSourceConnectors(job1)) {
                try {
                    Cluster clusterEntity = clusterRepository.findById(job1.getDeployClusterId()).get();
                    handlehistoryTopics(job1, con, clusterEntity);
                } catch (Exception e) {
                    logger.error("create connector {} failed,error message: {}", con.name, e.getMessage());
                    throw new RuntimeException("update connector: " + con.name + " failed, " + e.getMessage());
                }
            }
        }
        jobRepository.save(job1);
        return Response.ok(job).build();
    }

    public Response update_bak( Job job) {
        try {
            long sourceCount = job.getConnectors().stream().filter(j -> j.category == Connector.Category.Source).count();
            if (sourceCount > 1L) {
                throw new RuntimeException("source connector count can only be 1");
            }
            Job originJob = jobRepository.findById(job.getId()).get();
            if (originJob == null) {
                throw new IllegalArgumentException("can't find Job by id: " + job.getId());
            }
            originJob.setName(job.getName());
            originJob.setNote(job.getNote());
            originJob.setDeployStatus(job.getDeployStatus());

            //TODO ???????????????????????????Connector
            for (Connector oriCon : originJob.getConnectors().toArray(new Connector[0])) {
                boolean isDeleted = true;
                for (Connector con : job.getConnectors()) {
                    if (con.getId().longValue() == oriCon.getId().longValue()) {
                        isDeleted = false;
                        oriCon.config = con.config;
                        oriCon.name = con.name;
                        break;
                    }
                }
                if (isDeleted) {
                    connectorRepository.delete(oriCon);
                    originJob.getConnectors().remove(oriCon);
                }
            }

            //TODO ???????????????connector
            job.getConnectors().stream().forEach(con -> {
                if (con.getId() == null) {
                    originJob.getConnectors().add(con);
                } else {
                    for (Connector oriCon : originJob.getConnectors()) {
                        if (con.getId().longValue() == oriCon.getId().longValue()) {
                            oriCon.config = con.config;
                            oriCon.name = con.name;
                            break;
                        }
                    }
                }

            });
            jobRepository.saveAndFlush(originJob);
            return Response.ok(job).build();
        } catch (Exception e) {
            logger.error("update error", e);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
    }

    @Transactional
    @PostMapping("/refresh")
    @RolesAllowed({"User", "Admin"})
    public Response refresh(@RequestBody Long[] jobIds, @RequestParam(value = "recovery",required = false)  boolean recovery) {
        for (Long jobId : jobIds) {

            Job userJob = jobService.findJobByIdAndLoadConnector(jobId.longValue());
            if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
                User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
                if (user == null) {
                    throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
                }
                if (user.getId().longValue() != userJob.getCreatorId().longValue()) {
                    throw new RuntimeException("no available connector for user " + user.chineseName);
                }
            }
            if (recovery) {
                jobService.updateJobRecoveryStatus(jobId, Job.RecoveryStatus.RECOVERYING);
            }
            executorService.execute(() -> {
                        try {
                            Job job = userJob;
                            //Job job = jobRepository.findById(jobId.longValue()).get();
                            logger.info("executor refresh job ,jobId :{},recovery:{}", jobId, recovery);
                            if ((job.getDeployClusterId() == null && (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT
                                    && job.getSinkRealType() == Job.SinkRealType.EXTERNAL))) && job.getDeployStatus() == Job.DeployStatus.UN_DEPLOYED) {
                                //return Response.serverError().entity("job " + job.name + " was UN_DEPLOYED please deploy first.").build();
                                throw new RuntimeException("job " + job.getName() + " was UN_DEPLOYED please deploy first.");
                            }
                            if (recovery) {
                                recoverConnect(job);
                            }
                            if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                                KafkaConnectClient kafkaConnectClient;
                                try {
                                    kafkaConnectClient = KafkaConnectClientFactory.getClient(job.getDeployClusterId().intValue());
                                } catch (KafkaConnectException e) {
                                    throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                                }
                                Set<String> topics = jobService.findSinkConnectors(job).stream()
                                        .map(item -> item.getConfig().get("topics")).collect(Collectors.toSet());
                                job.setAllDeployTopics(topics);
                                LinkedList<Connector> connectors = new LinkedList<>();
                                connectors.addAll(jobService.findSourceConnectors(job));
                                connectors.addAll(jobService.findSinkConnectors(job));
                                for (Connector con : connectors) {
                                    try {
                                        ConnectConnectorConfigResponse configResponse = new ConnectConnectorConfigResponse(con.name, con.config);
                                        if (con.category == Connector.Category.Sink) {
                                            if ("true".equals(configResponse.getConfig().get(SINK_METRICS_ENABLED_KEY))) {
                                                configResponse.getConfig().put(METRICS_EXPORT_PORT_KEY, getSinkMetricsExportPort(job));
                                            }
                                        }
                                        aesDecryptPassword(configResponse.getConfig());
                                        Response result = kafkaConnectClient.updateConnectorConfig(configResponse.getName(), configResponse.getConfig());
                                        logger.info("update connector {} response: {}", con.name, result.getStatus());
                                    } catch (IOException e) {
                                        logger.warn("update connector {} failed,error message: {}", con.name, e.getMessage());
                                        throw new RuntimeException("create connector: " + con.name + " failed, " + e.getMessage());
                                    }
                                }
                            } else if (job.getJobType() == Job.JobType.COLLECT) {
                                Connector firstSourceConnector = jobService.findOneSourceConnector(job);
                                if (!firstSourceConnector.config.containsKey(SourceConnectorKeyword.SOURCE_TABLE) || StringUtils.isEmpty(firstSourceConnector.config.get(SourceConnectorKeyword.SOURCE_TABLE))) {
                                    throw new RuntimeException("connector no include table list");
                                }
                                //??????table list
                                String tableInclude = firstSourceConnector.config.get("table.include.list");
                                List<String> tableIncludes = Arrays.asList(tableInclude.split(","));
                                Set<String> topics = tableIncludes.stream().map(item -> TopicNameUtil.parseTopicName(job.getId(), this.topicPrefix, item))
                                        .collect(Collectors.toSet());
                                job.setAllDeployTopics(topics);
                                List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
                                if (sinkConnectors != null) {
                                    for (Connector connector : sinkConnectors) {
                                        try {
                                            Map<String, String> configs = connector.config;
                                            String id = configs.get("id");
                                            Cluster clusterEntity = clusterRepository.findById(Long.valueOf(id)).get();
                                            handlehistoryTopics(job, firstSourceConnector, clusterEntity);

                                            firstSourceConnector.config.put("topics", firstSourceConnector.config.get("database.server.name") + job.getId());
                                            String regex = firstSourceConnector.config.get("transforms.topicRoute.regex");
                                            if (StringUtils.isNotEmpty(firstSourceConnector.config.get("transforms.topicRoute.regex"))) {
                                                // TODO topic??????regex
                                                firstSourceConnector.config.put("transforms.topicRoute.regex", regex.replaceAll(
                                                        firstSourceConnector.config.get("database.server.name"), configs.get("dbServerName")));
                                            }

                                            firstSourceConnector.config.put("database.server.name", configs.get("dbServerName"));
                                            //??????sink????????????mysql binlog ???,database.server.id ????????????
                                            firstSourceConnector.config.put("database.server.id", configs.get("database.server.id"));
                                            final KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(clusterEntity.getId());
                                            final List<String> deployedConnectorNames = kafkaConnectClient.listConnectors();
                                            ConnectConnectorConfigResponse configResponse = new ConnectConnectorConfigResponse(firstSourceConnector.name, firstSourceConnector.config);
                                            createOrUpdateConnector(kafkaConnectClient, deployedConnectorNames, configResponse, job);
                                        } catch (KafkaConnectException e) {
                                            logger.error("create connector {} failed,error message: {}", firstSourceConnector.name, e.getMessage());
                                        } catch (Exception e) {
                                            logger.error("create connector {} failed,error message: {}", firstSourceConnector.name, e.getMessage());
                                            throw new RuntimeException("create connector: " + firstSourceConnector.name + " failed, " + e.getMessage());
                                        }
                                    }
                                }
                            }

                            if (recovery) {
                                if (checkTaskStatus(job, false)) {
                                    logger.info("update job recoveryStatus {} -> {}", job.getRecoveryStatus(), Job.RecoveryStatus.RECOVERYSUCCESS);
                                    jobService.updateJobRecoveryStatus(jobId, Job.RecoveryStatus.RECOVERYSUCCESS);
                                } else {
                                    logger.info("update job recoveryStatus {} -> {}", job.getRecoveryStatus(), Job.RecoveryStatus.RECOVERYTIMEOUT);
                                    jobService.updateJobRecoveryStatus(jobId, Job.RecoveryStatus.RECOVERYTIMEOUT);
                                }
                            }
                        } catch (Exception exception) {
                            logger.error("recovery failed :{}", exception.getMessage(), exception);
                            if (recovery) {
                                jobService.updateJobRecoveryStatus(jobId, Job.RecoveryStatus.RECOVERYFAILED);
                            }
                        }
                    }


            );
        }

        return Response.ok().build();
    }


    @PostMapping("/rebuild")
    @Transactional
    @RolesAllowed({"User", "Admin"})
    public Response rebuild(@RequestBody Long[] jobIds) {
        for (Long jobId : jobIds) {
            Job userJob = jobService.findJobByIdAndLoadConnector(jobId.longValue());
            if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
                User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
                if (user == null) {
                    throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
                }
                if (user.getId().longValue() != userJob.getCreatorId().longValue()) {
                    throw new RuntimeException("no available connector for user " + user.chineseName);
                }
            }
            try {
                rebuildJob(userJob);
            } catch (Exception e) {
                logger.error("recollect job {} error :", jobId, e);
                jobService.updateJobRecollectStatus(jobId, Job.RecollectStatus.RECOLLECTFAILED);
            }
        }
        return Response.ok().build();
    }

    /**
     * 1.???????????????????????????source connect
     * 2.??????????????????history topic ?????? internal topic
     * 3.??????source connect.name ?????? source step.name
     *
     * @param job
     */
    @Transactional
    public void rebuildJob(Job job) throws Exception {
        long id = job.getId();
        logger.info("jobId {} start to rebuild", id);
        //????????????????????????
        job.setRecollectStatus(Job.RecollectStatus.RECOLLECTING);
        jobService.updateJobRecollectStatus(id, Job.RecollectStatus.RECOLLECTING);

        List<Connector> sourceConnectors = jobService.findSourceConnectors(job);
        Connector connector = sourceConnectors.get(0);
        // 1.??????source connect.name
        String oldConnectorName = connector.name;
        String connectName = connector.name;
        String connectSuffix = "";
        // 3.??????steps??????source??? otherConfigurations
//        List<StepDesc> collect = job.getSteps().stream().filter(item -> Job.StepType.sources.name().equals(item.getScope())).collect(Collectors.toList());
        if (connectName.contains("_rebuild_")) {
            String[] rebuild_s = connectName.split("_rebuild_");
            Integer number = Integer.valueOf(rebuild_s[1]);
            int num = number.intValue() + 1;
            connectSuffix = "_rebuild_" + num;
            connectName = rebuild_s[0] + connectSuffix;
        } else {
            connectSuffix = "_rebuild_" + "1";
            connectName = connectName + connectSuffix;
        }

        if (connector.config.containsKey("database.server.id")) {
            String serverId = connector.config.getOrDefault("database.server.id", "");
            if (StringUtils.isNotBlank(serverId)) {
                connector.config.put("database.server.id", Integer.toString(Integer.parseInt(serverId) + 1));
                connectorService.saveConnectorConfig(connector);
            }
        }
        connectorService.updateConnectName(connector.getId(), connectName);

        // ???????????????????????????????????????????????????
        //????????????????????????????????????jobId???newConnectName???????????????
        try {
            alertRepository.deleteByJobId(job.getId());
        } catch (Exception e) {
            logger.error("delete alert {} failed,error message: {}", e.getMessage(), e);
        }


        logger.info("jobId {} update connect name [{}] -> [{}]", id, oldConnectorName, connectName);

        final String newConnectName = connectName;
        //??????????????????
        recollectExecutorService.execute(
                new RecollectJobLauncher(id, connector, oldConnectorName, newConnectName, this.topicPrefix,
                        clusterRepository, jobRepository, dataSourceRepository, connectorRepository, alertRepository, jobService, connectorService));

    }

    /**
     * ?????? ???????????? task ????????????
     * ??????10???,????????????30???
     * ????????????????????????job.recoveryStatus
     *
     * @param job
     * @return
     */
    public boolean checkTaskStatus(Job job, boolean checkSinkConnectorFlag) throws Exception {
        ThreadUtils.sleep(Duration.ofSeconds(30L));
        List<Connector> sourceConnectors = jobService.findSourceConnectors(job);
        List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
        Connector connector = sourceConnectors.get(0);
        logger.info("check job {} connector status :{}", job.getName(), connector.name);
        int maxRetries = 10;
        while (maxRetries > 0) {
            if (checkSinkConnectorFlag) {//recollect??????
                Job jobById = jobRepository.findById(job.getId()).get();
                if (jobById != null && jobById.getRecollectStatus() != Job.RecollectStatus.RECOLLECTING) {
                    return false;
                }
            }
            if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                Long deployClusterId = job.getDeployClusterId();
                if (deployClusterId == null) {
                    return false;
                }

                try {
                    boolean sourceRes = false;
                    boolean sinkRes = false;
                    ConnectorStatus connecorStatus = getConnecorStatus(deployClusterId.intValue(), connector.name);
                    long count = connecorStatus.getTaskStates().keySet().stream()
                            .filter(item -> connecorStatus.getTaskState(item).taskStatus == ConnectorStatus.State.RUNNING)
                            .count();
                    if (connecorStatus.getTaskStates().size() > 0 && count == connecorStatus.getTaskStates().size()) {
                        sourceRes = true;
                    }

                    if (checkSinkConnectorFlag) {
                        for (Connector conn : sinkConnectors) {
                            ConnectorStatus status = getConnecorStatus(deployClusterId.intValue(), conn.name);
                            long count_sink = status.getTaskStates().keySet().stream()
                                    .filter(item -> status.getTaskState(item).taskStatus == ConnectorStatus.State.RUNNING)
                                    .count();
                            if (status.getTaskStates().size() > 0 && count_sink == status.getTaskStates().size()) {
                                sinkRes = true;
                            }
                        }
                    } else {
                        sinkRes = true;
                    }

                    if (sourceRes && sinkRes) {
                        return true;
                    }
                } catch (Exception e) {
                    if (e instanceof WebApplicationException && (((WebApplicationException) e).getResponse().getStatus() == 404 || ((WebApplicationException) e).getResponse().getStatus() == 409)) {
                        logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                    } else if (e instanceof ResteasyWebApplicationException && (((ResteasyWebApplicationException) e).getResponse().getStatus() == 404 || ((ResteasyWebApplicationException) e).getResponse().getStatus() == 409)) {
                        logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                    } else {
                        logger.error("check source&sink connector {} error:", connector.name, e);
                        throw e;
                    }
                }

            } else if (job.getJobType() == Job.JobType.COLLECT) {
                for (Connector sinkConnector : sinkConnectors) {
                    String id = sinkConnector.getConfig().get("id");
                    if (StringUtils.isBlank(id)) {
                        return false;
                    }
                    try {
                        ConnectorStatus connecorStatus = getConnecorStatus(Integer.valueOf(id), connector.name);
                        long count = connecorStatus.getTaskStates().keySet().stream()
                                .filter(item -> connecorStatus.getTaskState(item).taskStatus == ConnectorStatus.State.RUNNING)
                                .count();
                        if (connecorStatus.getTaskStates().size() > 0 && count == connecorStatus.getTaskStates().size()) {
                            return true;
                        }
                    } catch (Exception e) {
                        if (e instanceof WebApplicationException && (((WebApplicationException) e).getResponse().getStatus() == 404 || ((WebApplicationException) e).getResponse().getStatus() == 409)) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                        } else if (e instanceof ResteasyWebApplicationException && (((ResteasyWebApplicationException) e).getResponse().getStatus() == 404 || ((ResteasyWebApplicationException) e).getResponse().getStatus() == 409)) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                        } else {
                            logger.error("check collect connector {} error:", connector.name, e);
                            throw e;
                        }
                    }
                }
            }
            maxRetries--;

            ThreadUtils.sleep(Duration.ofSeconds(30L));
        }
        return false;
    }


    /**
     * 1.??????history topic
     * 2.??????source connect ??? snapshot.mode ??? schema_only_recovery
     * 3.??????steps??????source??? otherConfigurations
     *
     * @param job
     */
    public void recoverConnect(Job job) {
        logger.info("recoverConnect start ,jobId:{} ", job.getId());
        List<Connector> sourceConnectors = jobService.findSourceConnectors(job);
        Connector connector = sourceConnectors.get(0);
        String historyTopic = connector.config.get(KafkaDatabaseHistory.TOPIC.name());
        Set<String> delTopic = new HashSet<>() {{
            add(historyTopic);
        }};

        // 1.??????history topic
        if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
            Long deployClusterId = job.getDeployClusterId();
            if (deployClusterId == null) {
                return;
            }

            Cluster cluster = clusterRepository.findById(deployClusterId).get();

            // ??????source connect,refesh??????????????????
            KafkaConnectClient kafkaConnectClient;
            try {
                kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster.getId().intValue());
                kafkaConnectClient.deleteConnector(connector.name);
                logger.info("job {} ,delete source conncet :{}", job.getId(), connector.name);
            } catch (KafkaConnectException e) {
                throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
            } catch (IOException e) {
                logger.warn("delete connector {} failed,error message: {}", connector.name, e.getMessage());
            } catch (WebApplicationException e) {
                if (e.getResponse().getStatus() == 404 || e.getMessage().contains("404")) {
                    logger.warn("connector {} maybe has already being deleted from cluster {}.", connector.name, job.getDeployClusterId());
                } else {
                    throw e;
                }
            }

            deleteTopicsByCluster(cluster, delTopic);
        } else if (job.getJobType() == Job.JobType.COLLECT) {
            List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
            for (Connector sinkConnector : sinkConnectors) {
                String id = sinkConnector.getConfig().get("id");
                if (StringUtils.isBlank(id)) {
                    continue;
                }
                Cluster cluster = clusterRepository.findById(Long.valueOf(id)).get();

                // ??????source connect,refesh??????????????????
                KafkaConnectClient kafkaConnectClient;
                try {
                    kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster.getId().intValue());
                    kafkaConnectClient.deleteConnector(connector.name);
                    logger.info("job {} ,delete source conncet :{}", job.getId(), connector.name);
                } catch (KafkaConnectException e) {
                    throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                } catch (IOException e) {
                    logger.warn("delete connector {} failed,error message: {}", connector.name, e.getMessage());
                } catch (WebApplicationException e) {
                    if (e.getResponse().getStatus() == 404) {
                        logger.warn("connector {} maybe has already being deleted from cluster {}.", connector.name, job.getDeployClusterId());
                    } else {
                        throw e;
                    }
                }

                deleteTopicsByCluster(cluster, delTopic);
            }
        }

        // 2.??????source connect ??? snapshot.mode ??? schema_only_recovery
        String snapshotMode = connector.config.get("snapshot.mode");
        connector.config.put("snapshot.mode", "schema_only_recovery");

        connectorService.saveConnectorConfig(connector);
        logger.info("update connect snapshot.mode , connectName :{} , mode {} ->{}", connector.name, snapshotMode, "schema_only_recovery");
        // 3.??????steps??????source??? otherConfigurations
        List<StepDesc> collect = job.getSteps().stream().filter(item -> Job.StepType.sources.name().equals(item.getScope())).collect(Collectors.toList());
        StepDesc stepDesc = collect.get(0);

        ConfigObject otherConfigurations = stepDesc.getOtherConfigurations();
        otherConfigurations.put("snapshot.mode", "schema_only_recovery");
        jobService.saveSteps(job.getId(), job.getSteps());
    }

    @Transactional
    @PostMapping("/deploy")
    @RolesAllowed({"User", "Admin"})
    public Response deploy( @RequestBody JobDeployParam jobDeployParam) throws Exception {
        List<ActionStatus> response = new ArrayList<>();
        for (Long jobId : jobDeployParam.getJobIds()) {
            Job job = jobRepository.findById(jobId.longValue()).get();
            if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
                User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
                if (user == null) {
                    throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
                }
                if (user.getId().longValue() != job.getCreatorId().longValue()) {
                    throw new RuntimeException("no available connector for user " + user.chineseName);
                }
            }
            job.getConnectors().size();//?????????
            // ??????connector ??????
            validConnector(job);

            // ?????????????????????????????????topic,??????????????????????????????????????????topic
            //?????????????????????KafkaSink?????????????????????SourceConnector
            if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                Long deployClusterId = job.getDeployClusterId();
                if (deployClusterId == null) {
                    throw new RuntimeException("cluster id is empty");
                }
                final KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(deployClusterId);
                final List<String> deployedConnectorNames = kafkaConnectClient.listConnectors();
                final Cluster clusterEntity = clusterRepository.findById(deployClusterId).get();
                createSourceConnector(job, clusterEntity, deployedConnectorNames, kafkaConnectClient);
                createSinkConnector(kafkaConnectClient, deployedConnectorNames, job, clusterEntity);

                List<String> topics = jobService.findSinkConnectors(job).stream().
                        map(item -> item.getConfig().get("topics")).collect(Collectors.toList());
                jobService.setAllDeployTopics(topics, job);

            } else if (job.getJobType() == Job.JobType.COLLECT) {
                Connector firstSourceConnector = jobService.findOneSourceConnector(job);
                if (!firstSourceConnector.config.containsKey(SourceConnectorKeyword.SOURCE_TABLE) || StringUtils.isEmpty(firstSourceConnector.config.get(SourceConnectorKeyword.SOURCE_TABLE))) {
                    throw new RuntimeException("connector no include table list");
                }
                String tableInclude = firstSourceConnector.config.get("table.include.list");
                List<String> tableIncludes = Arrays.asList(tableInclude.split(","));
                Set<String> topics = tableIncludes.stream().map(item -> TopicNameUtil.parseTopicName(job.getId(), this.topicPrefix, item))
                        .collect(Collectors.toSet());
                job.setAllDeployTopics(topics);
                List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
                if (sinkConnectors != null) {
                    for (Connector sinkConnector : sinkConnectors) {
                        try {
                            Map<String, String> configs = sinkConnector.config;
                            String id = configs.get("id");
                            Cluster clusterEntity = clusterRepository.findById(Long.valueOf(id)).get();
                            handlehistoryTopics(job, firstSourceConnector, clusterEntity);

                            String topic = firstSourceConnector.config.get("database.server.name") + job.getId();
                            firstSourceConnector.config.put("topics", topic);

                            String regex = firstSourceConnector.config.get("transforms.topicRoute.regex");
                            if (StringUtils.isNotEmpty(firstSourceConnector.config.get("transforms.topicRoute.regex"))) {
                                // TODO topic??????regex
                                firstSourceConnector.config.put("transforms.topicRoute.regex", regex.replaceAll(
                                        firstSourceConnector.config.get("database.server.name"), configs.get("dbServerName")));
                            }
                            firstSourceConnector.config.put("database.server.name", configs.get("dbServerName"));
                            //??????sink????????????mysql binlog ???,database.server.id ????????????
                            firstSourceConnector.config.put("database.server.id", configs.get("database.server.id"));
                            final KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(clusterEntity.getId());
                            final List<String> deployedConnectorNames = kafkaConnectClient.listConnectors();
                            ConnectConnectorConfigResponse configResponse = new ConnectConnectorConfigResponse(firstSourceConnector.name, firstSourceConnector.config);
                            createOrUpdateConnector(kafkaConnectClient, deployedConnectorNames, configResponse, job);
                        } catch (IOException e) {
                            logger.error("create connector {} failed,error message: {}", firstSourceConnector.name, e.getMessage());
                            throw new RuntimeException("create connector: " + firstSourceConnector.name + " failed, " + e.getMessage());
                        }
                    }
                }
            }

            // ??????connect status
            long update = connectorService.updateConnectorStatus(job.getId(), ConnectorStatus.State.RUNNING);
            logger.info("update job connect connectorStatus -> {} rows :{}", ConnectorStatus.State.RUNNING, update);

            //????????????????????????schema change??????
            if (monitorSchemaEnable && job.isSnapshot()) {
                liqiuBaseSnapshotLauncher(job);
            }
            job.setUpdateTime(LocalDateTime.now());
            job.setDeployStatus(Job.DeployStatus.DEPLOYED);
            jobRepository.saveAndFlush(job);

        }
        return Response.ok(response).build();
    }

    /**
     * TODO ????????????springboot ???????????????schemachange
     * deploy??????schema change??????
     */
    private void liqiuBaseSnapshotLauncher(Job job) {
        /*String workingDir = schemaChangeProcessor.getWorkingDir();
        job.connectors.size();//load connectors
        job.snapshotStatus = Job.SnapshotStatus.IN_PROGRESS;
        job.persistAndFlush();
        LiquibaseSnapshotLauncher snapshotLauncher =
                new LiquibaseSnapshotLauncher(job, workingDir, jobRepository, connectorRepository);
        executorService.execute(snapshotLauncher);*/
    }

    /**
     * create or update connector
     *
     * @param kafkaConnectClient     kafkaClient
     * @param deployedConnectorNames ????????????connectorName
     * @param configResponse         connector config
     * @param job                    job
     * @throws IOException
     */
    private void createOrUpdateConnector(KafkaConnectClient kafkaConnectClient, List<String> deployedConnectorNames, ConnectConnectorConfigResponse configResponse, Job job) throws IOException {

        aesDecryptPassword(configResponse.getConfig());
        if (!deployedConnectorNames.contains(configResponse.getName())) {
            String result = kafkaConnectClient.createConnector(configResponse);
            logger.info("job {} create connector {} on cluster {} success.", job.getName(), configResponse.getName(), job.getDeployCluster());
        } else {
            Response result = kafkaConnectClient.updateConnectorConfig(configResponse.getName(), configResponse.getConfig());
            logger.info("job {} update connector {} on cluster {} successe.", job.getName(), configResponse.getName(), job.getDeployCluster());
        }
    }

    private void aesDecryptPassword(Map<String, String> config) {
        // source && connector type jdbc
        if (config.containsKey(DataSourceKeyword.DATABASE_PASSWORD)) {
            config.put(DataSourceKeyword.DATABASE_PASSWORD, AesUtils.wrappeDaesDecrypt(config.get(DataSourceKeyword.DATABASE_PASSWORD)));
        }

        // sink && connector type jdbc
        if (config.containsKey(SinkConnectorKeyword.SINK_PASSWD)) {
            config.put(SinkConnectorKeyword.SINK_PASSWD, AesUtils.wrappeDaesDecrypt(config.get(SinkConnectorKeyword.SINK_PASSWD)));
        }

        // sink && connector type kafka
        if (config.containsKey(DataSourceKeyword.PASSWORD)) {
            String passWord = config.get(DataSourceKeyword.PASSWORD);
            if (passWord.startsWith("AES(")) {
                String s = AesUtils.wrappeDaesDecrypt(passWord);
                if (config.containsKey("sasl.jaas.config")) {
                    config.put("sasl.jaas.config", config.get("sasl.jaas.config").replace(passWord, s));
                }
            }
            config.put(DataSourceKeyword.PASSWORD, AesUtils.wrappeDaesDecrypt(config.get(DataSourceKeyword.PASSWORD)));
        }


        // kafka history topic
        //eg org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='AES(admin)';
        if (config.containsKey("database.history.consumer.sasl.jaas.config")) {
            String consumerSasl = config.get("database.history.consumer.sasl.jaas.config");
            Matcher matcher = saslPattern.matcher(consumerSasl);
            if (matcher.find()) {
                String group = matcher.group();
                config.put("database.history.consumer.sasl.jaas.config", consumerSasl.replace(group, AesUtils.wrappeDaesDecrypt(group)));
            }
        }

        //eg org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='AES(admin)';
        if (config.containsKey("database.history.producer.sasl.jaas.config")) {
            String producerSasl = config.get("database.history.producer.sasl.jaas.config");
            Matcher matcher = saslPattern.matcher(producerSasl);
            if (matcher.find()) {
                String group = matcher.group();
                config.put("database.history.producer.sasl.jaas.config", producerSasl.replace(group, AesUtils.wrappeDaesDecrypt(group)));
            }
        }
    }

    /**
     * ??????connector ??????
     *
     * @param job job detail
     */
    private void validConnector(Job job) {
        if (job.getConnectors() == null || job.getConnectors().size() == 0) {
            throw new RuntimeException("not found connector node in job");
        } else if (jobService.findSourceConnectors(job).size() == 0) {
            throw new RuntimeException("not found source connector node in job");
        } else if (jobService.findSinkConnectors(job).size() == 0) {
            throw new RuntimeException("not found sink connector node in job");
        }
    }

    private void createSourceConnector(final Job job, final Cluster clusterEntity, final List<String> deployedConnectorNames, final KafkaConnectClient kafkaConnectClient) {
        List<Connector> sourceConnectors = jobService.findSourceConnectors(job);
        for (Connector con : sourceConnectors) {
            if (!con.config.containsKey(SourceConnectorKeyword.SOURCE_TABLE) || StringUtils.isEmpty(con.config.get(SourceConnectorKeyword.SOURCE_TABLE))) {
                throw new RuntimeException("connector no include table list");
            }
            try {
                handlehistoryTopics(job, con, clusterEntity);
                ConnectConnectorConfigResponse configResponse = new ConnectConnectorConfigResponse(con.name, con.config);
                createOrUpdateConnector(kafkaConnectClient, deployedConnectorNames, configResponse, job);
            } catch (IOException e) {
                logger.error("create connector {} failed,error message: {}", con.name, e.getMessage());
                throw new RuntimeException("create connector: " + con.name + " failed, " + e.getMessage());
            }
        }
    }

    /**
     * ??????connector ?????? kafka broker ?????? history topics
     */
    private void handlehistoryTopics(Job job, Connector con, Cluster clusterEntity) {
        if (monitorSchemaEnable && job.isSnapshot()) {
            con.config.put(KafkaDatabaseHistory.BOOTSTRAP_SERVERS.name(), kafkaBrokers);
            con.config.put(KafkaDatabaseHistory.TOPIC.name(), schemaTopic);
            if (StringUtils.isNotBlank(securityProtocol) && !securityProtocol.equals("NONE") && StringUtils.isNotBlank(saslMechanism) && !saslMechanism.equals("NONE")
                    && StringUtils.isNotBlank(jaasConfig) && !jaasConfig.equals("NONE")) {
                List<String> lines = FileUtil.readLines(jaasConfig);
                StringBuilder jaasText = new StringBuilder();
                String jaasConf = null;
                if (lines != null && lines.size() > 2) {
                    for (int i = 1; i < lines.size() - 1; i++) {
                        jaasText.append(lines.get(i) + " ");
                    }
                    jaasText.setLength(jaasText.length() - 1);
                    jaasConf = jaasText.toString().replaceAll("\"", "'");
                } else {
                    throw new IllegalArgumentException("jaasconfig file is error.");
                }

                con.config.put("database.history.consumer.security.protocol", securityProtocol);
                con.config.put("database.history.consumer.sasl.mechanism", saslMechanism);
                con.config.put("database.history.consumer.sasl.jaas.config", jaasConf);
                con.config.put("database.history.producer.security.protocol", securityProtocol);
                con.config.put("database.history.producer.sasl.mechanism", saslMechanism);
                con.config.put("database.history.producer.sasl.jaas.config", jaasConf);
            }
        } else {
            if (clusterEntity == null) {
                return;
            }
            con.config.put(KafkaDatabaseHistory.BOOTSTRAP_SERVERS.name(), clusterEntity.brokerList);
            String topicName = TopicNameUtil.parseHistoryTopicName(job.getId(), this.topicPrefix);
            con.config.put(KafkaDatabaseHistory.TOPIC.name(), topicName);
            if (StringUtils.isNotBlank(clusterEntity.securityProtocol) && StringUtils.isNotBlank(clusterEntity.saslMechanism)) {
                con.config.put("database.history.consumer.security.protocol", clusterEntity.securityProtocol);
                con.config.put("database.history.consumer.sasl.mechanism", clusterEntity.saslMechanism);
                con.config.put("database.history.consumer.sasl.jaas.config", clusterEntity.initSaslJaasConfig());
                con.config.put("database.history.producer.security.protocol", clusterEntity.securityProtocol);
                con.config.put("database.history.producer.sasl.mechanism", clusterEntity.saslMechanism);
                con.config.put("database.history.producer.sasl.jaas.config", clusterEntity.initSaslJaasConfig());
            }
        }
    }

    private void createSinkConnector(KafkaConnectClient kafkaConnectClient, List<String> deployedConnectorNames, Job job, Cluster clusterEntity) {
        List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
        if (sinkConnectors != null) {
            for (Connector connector : sinkConnectors) {
                try {
                    Map<String, String> configs = connector.config;
                    ConnectConnectorConfigResponse configResponse = new ConnectConnectorConfigResponse(connector.name, configs);
                    if ("true".equals(configResponse.getConfig().get(SINK_METRICS_ENABLED_KEY))) {
                        configResponse.getConfig().put(METRICS_EXPORT_PORT_KEY, getSinkMetricsExportPort(job));
                    }
                    if (StringUtils.isNotBlank(clusterEntity.securityProtocol) && StringUtils.isNotBlank(clusterEntity.saslMechanism)) {
                        configResponse.getConfig().put("database.history.consumer.security.protocol", clusterEntity.securityProtocol);
                        configResponse.getConfig().put("database.history.consumer.sasl.mechanism", clusterEntity.saslMechanism);
                        configResponse.getConfig().put("database.history.consumer.sasl.jaas.config", clusterEntity.initSaslJaasConfig());
                        configResponse.getConfig().put("database.history.producer.security.protocol", clusterEntity.securityProtocol);
                        configResponse.getConfig().put("database.history.producer.sasl.mechanism", clusterEntity.saslMechanism);
                        configResponse.getConfig().put("database.history.producer.sasl.jaas.config", clusterEntity.initSaslJaasConfig());
                    }

                    createOrUpdateConnector(kafkaConnectClient, deployedConnectorNames, configResponse, job);
                } catch (IOException e) {
                    logger.error("create connector {} failed,error message: {}", connector.name, e.getMessage());
                    throw new RuntimeException("create connector: " + connector.name + " failed, " + e.getMessage());
                }
            }
        }
    }

    private String getSinkMetricsExportPort(Job job) {
        if (Job.JobType.SYNC.equals(job.getJobType())) {
            return sinkMetricsExportPortJdbc;
        } else if (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.EXTERNAL.equals(job.getSinkRealType())) {
            return sinkMetricsExportPortKafka;
        }
        return sinkMetricsExportPortJdbc;
    }

    @Transactional
    @PostMapping("/pause")
    @Consumes(MediaType.APPLICATION_JSON)
    @RolesAllowed({"User", "Admin"})
    public Response pause(@RequestBody Long[] jobIds) {
        for (Long jobId : jobIds) {
            Job job = jobRepository.findById(jobId.longValue()).get();
            User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
            if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
                if (user == null) {
                    throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
                }
                if (user.getId().longValue() != job.getCreatorId().longValue()) {
                    throw new RuntimeException("no available connector for user " + user.chineseName);
                }

            }
            if (job.getDeployClusterId() == null && job.getDeployStatus() == Job.DeployStatus.UN_DEPLOYED) {
                return Response.serverError().entity("job " + job.getName() + " was UN_DEPLOYED please deploy first.").build();
            }

            if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                KafkaConnectClient kafkaConnectClient;
                try {
                    kafkaConnectClient = KafkaConnectClientFactory.getClient(job.getDeployClusterId().intValue());
                } catch (KafkaConnectException e) {
                    throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                }
                for (Connector connector : job.getConnectors()) {
                    try {
                        Response response = kafkaConnectClient.pauseConnector(connector.name);
                        logger.info("pause connector {} response: {}", connector.name, response);
                    } catch (IOException e) {
                        throw new RuntimeException("pause connecotr " + connector.name + " failed,error message: " + e.getMessage());
                    } catch (WebApplicationException e) {
                        if (e.getResponse().getStatus() == 404) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }
            } else if (job.getJobType() == Job.JobType.COLLECT) {
                Connector firstSourceConnector = jobService.findOneSourceConnector(job);
                List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
                for (Connector sinkConnector : sinkConnectors) {
                    String id = sinkConnector.getConfig().get("id");
                    KafkaConnectClient kafkaConnectClient;
                    try {
                        kafkaConnectClient = KafkaConnectClientFactory.getClient(Integer.valueOf(id));
                    } catch (KafkaConnectException e) {
                        throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                    }
                    try {
                        Response response = kafkaConnectClient.pauseConnector(firstSourceConnector.name);
                        logger.info("pause connector {} response: {}", firstSourceConnector.name, response);
                    } catch (IOException e) {
                        throw new RuntimeException("pause connecotr " + firstSourceConnector.name + " failed,error message: " + e.getMessage());
                    } catch (WebApplicationException e) {
                        if (e.getResponse().getStatus() == 404) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", firstSourceConnector.name, job.getDeployClusterId());
                            continue;
                        } else {
                            throw e;
                        }

                    }
                }
            }


            // ??????connect status
            long update = connectorService.updateConnectorStatus(job.getId(), ConnectorStatus.State.PAUSED);
            logger.info("update job connect connectorStatus -> {} rows :{}", ConnectorStatus.State.PAUSED, update);

            job.setDeployStatus(Job.DeployStatus.PAUSED);
            jobRepository.saveAndFlush(job);
        }
        return Response.ok().build();
    }

    @Transactional
    @PostMapping("/resume")
    @RolesAllowed({"User", "Admin"})
    public Response resume(@RequestBody Long[] jobIds) {
        for (Long jobId : jobIds) {
            Job job = jobRepository.findById(jobId.longValue()).get();
            if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
                User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
                if (user == null) {
                    throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
                }
                if (user.getId().longValue() != job.getCreatorId().longValue()) {
                    throw new RuntimeException("no available connector for user " + user.chineseName);
                }
            }
            if (job.getDeployClusterId() == null && job.getDeployStatus() == Job.DeployStatus.UN_DEPLOYED) {
                return Response.serverError().entity("job " + job.getName() + " was UN_DEPLOYED please deploy first.").build();
            }

            if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                KafkaConnectClient kafkaConnectClient;
                try {
                    kafkaConnectClient = KafkaConnectClientFactory.getClient(job.getDeployClusterId().intValue());
                } catch (KafkaConnectException e) {
                    throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                }
                for (Connector connector : job.getConnectors()) {
                    try {
                        Response response = kafkaConnectClient.resumeConnector(connector.name);
                        logger.info("resume connector {} response: {}", connector.name, response);
                    } catch (IOException e) {
                        logger.warn("resume connecotr {} failed,error message: {}", connector.name, e.getMessage());
                        throw new RuntimeException("resume connecotr " + connector.name + " failed,error message: " + e.getMessage());
                    } catch (WebApplicationException e) {
                        if (e.getResponse().getStatus() == 404) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }
            } else {
                Connector firstSourceConnector = jobService.findOneSourceConnector(job);
                List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
                for (Connector sinkConnector : sinkConnectors) {
                    String id = sinkConnector.getConfig().get("id");
                    KafkaConnectClient kafkaConnectClient;
                    try {
                        kafkaConnectClient = KafkaConnectClientFactory.getClient(Integer.valueOf(id));
                    } catch (KafkaConnectException e) {
                        throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                    }
                    try {
                        Response response = kafkaConnectClient.resumeConnector(firstSourceConnector.name);
                        logger.info("resume connector {} response: {}", firstSourceConnector.name, response);
                    } catch (IOException e) {
                        logger.warn("resume connecotr {} failed,error message: {}", firstSourceConnector.name, e.getMessage());
                        throw new RuntimeException("resume connecotr " + firstSourceConnector.name + " failed,error message: " + e.getMessage());
                    } catch (WebApplicationException e) {
                        if (e.getResponse().getStatus() == 404) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", firstSourceConnector.name, job.getDeployClusterId());
                            continue;
                        } else {
                            throw e;
                        }

                    }
                }
            }

            job.setDeployStatus(Job.DeployStatus.DEPLOYED);
            jobRepository.saveAndFlush(job);
        }
        return Response.ok().build();
    }


    @Transactional
    @PostMapping("/restart")
    @RolesAllowed({"User", "Admin"})
    public Response restart( @RequestBody Long[] jobIds) {
        for (Long jobId : jobIds) {
            Job job = jobRepository.findById(jobId.longValue()).get();
            if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
                User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
                if (user == null) {
                    throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
                }
                if (user.getId().longValue() != job.getCreatorId().longValue()) {
                    throw new RuntimeException("no available connector for user " + user.chineseName);
                }
            }
            if (job.getDeployClusterId() == null && job.getDeployStatus() == Job.DeployStatus.UN_DEPLOYED) {
                return Response.serverError().entity("job " + job.getName() + " was UN_DEPLOYED please deploy first.").build();
            }

            if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                KafkaConnectClient kafkaConnectClient;
                try {
                    kafkaConnectClient = KafkaConnectClientFactory.getClient(job.getDeployClusterId().intValue());
                } catch (KafkaConnectException e) {
                    throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                }
                for (Connector connector : job.getConnectors()) {
                    try {
                        Response response = kafkaConnectClient.restartConnector(connector.name);
                        logger.info("restart connector {} response: {}", connector.name, response);
                    } catch (IOException e) {
                        throw new RuntimeException("restart connecotr " + connector.name + " failed,error message: " + e.getMessage());
                    } catch (WebApplicationException e) {
                        if (e.getResponse().getStatus() == 404) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }
            } else if (job.getJobType() == Job.JobType.COLLECT) {
                Connector firstSourceConnector = jobService.findOneSourceConnector(job);
                List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
                for (Connector sinkConnector : sinkConnectors) {
                    String id = sinkConnector.getConfig().get("id");
                    KafkaConnectClient kafkaConnectClient;
                    try {
                        kafkaConnectClient = KafkaConnectClientFactory.getClient(Integer.valueOf(id));
                    } catch (KafkaConnectException e) {
                        throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                    }
                    try {
                        Response response = kafkaConnectClient.restartConnector(firstSourceConnector.name);
                        logger.info("restart connector {} response: {}", sinkConnector.name, response);
                    } catch (IOException e) {
                        throw new RuntimeException("restart connecotr " + sinkConnector.name + " failed,error message: " + e.getMessage());
                    } catch (WebApplicationException e) {
                        if (e.getResponse().getStatus() == 404) {
                            logger.warn("connector {} maybe has already being deleted from cluster {} .", sinkConnector.name, job.getDeployClusterId());
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }
            }
        }
        return Response.ok().build();
    }

    @Transactional
    @DeleteMapping("/delete")
    @RolesAllowed({"User", "Admin"})
    @Log(action = LogConst.ACTION_DELETE, itemId = "#jobIds")
    public Response delete(@RequestBody Long[] jobIds, @RequestParam(value = "delAlert",required = false)  boolean delAlert, @RequestParam(value = "delTopic",required = false)  boolean delTopic) {
        User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        CheckUtil.checkTrue(user == null, "no user found with name " + SecurityUtils.getCurrentUserUsername());

        for (Long jobId : jobIds) {
            Job job = jobRepository.findById(jobId.longValue()).get();

            CheckUtil.checkTrue(job == null, "job id mabay is error");

            if (User.Role.User.name().equals(user.getRole())) {
                if (user.getId().longValue() != job.getCreatorId().longValue()) {
                    throw new RuntimeException("no available connector for user " + user.chineseName);
                }
            }

            if (job.getDeployClusterId() != null && (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL))) {
                KafkaConnectClient kafkaConnectClient;
                try {
                    kafkaConnectClient = KafkaConnectClientFactory.getClient(job.getDeployClusterId().intValue());
                } catch (KafkaConnectException e) {
                    throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                }

                for (Connector connector : job.getConnectors()) {
                    String latestSnapshotFile = connector.getLatestSnapshotFile();
                    // TODO ??????springboot ????????????schema change
                    /*if (StringUtils.isNotBlank(latestSnapshotFile)) {
                        String workingDir = schemaChangeProcessor.getWorkingDir();
                        File file = new File(workingDir + File.separator + latestSnapshotFile);
                        logger.info("Delete connector {} schema change file {}", connector.name, latestSnapshotFile);
                        file.delete();
                    }*/
                    if (job.getDeployStatus() != Job.DeployStatus.UN_DEPLOYED) {
                        // 500?????????,??????????????????connect
                        try {
                            deleteConnect(kafkaConnectClient, connector.name, job);
                        } catch (WebApplicationException e) {
                            if (e.getResponse().getStatus() == 500) {
                                deleteConnect(kafkaConnectClient, connector.name, job);
                            } else {
                                throw e;
                            }
                        }
                    }
                }
            } else if (job.getJobType() == Job.JobType.COLLECT) {
                List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
                List<Connector> sourceConnectors = jobService.findSourceConnectors(job);
                if (job.getDeployStatus() != Job.DeployStatus.UN_DEPLOYED && CollectionUtils.isNotEmpty(sourceConnectors) && sourceConnectors.size() == 1) {
                    Connector firstSourceConnector = sourceConnectors.get(0);
                    KafkaConnectClient kafkaConnectClient;
                    for (Connector sinkConnector : sinkConnectors) {
                        String id = sinkConnector.getConfig().get("id");
                        try {
                            kafkaConnectClient = KafkaConnectClientFactory.getClient(Integer.valueOf(id));
                            Response response = kafkaConnectClient.deleteConnector(firstSourceConnector.name);
                            logger.info("delete connector {} response: {}", firstSourceConnector.name, response);
                        } catch (KafkaConnectException e) {
                            throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
                        } catch (IOException e) {
                            logger.warn("delete connector {} failed,error message: {}", firstSourceConnector.name, e.getMessage());
                        } catch (WebApplicationException e) {
                            if (e.getResponse().getStatus() == 404) {
                                logger.warn("connector {} maybe has already being deleted from cluster {}.", firstSourceConnector.name, job.getDeployClusterId());
                                continue;
                            } else {
                                throw e;
                            }
                        }

                    }
                }
            }


            // TODO ??????springboot ????????????schema change
            /*SchemaChange.deleteByJobId(schemaChangeProcessor.getWorkingDir(), jobId);
            logger.info("Delete schema change event by jobId {}", jobId);*/

            Long ruleSize = jobAlertRuleRepository.deleteByJobId(jobId);
            logger.info("Delete Job Alert Rule {} by jobId {}", ruleSize, jobId);

            if (delAlert) {
                long alertSize = alertRepository.deleteByJobId(jobId);
                logger.info("Delete alert {} by jobId {}", alertSize, jobId);
            }
            if (delTopic) {
                delTopic(job);
            }
            jobRepository.delete(job);
        }
        return Response.ok().build();
    }


    public void deleteConnect(KafkaConnectClient kafkaConnectClient, String connectName, Job job) {
        try {
            Response response = kafkaConnectClient.deleteConnector(connectName);
            logger.info("delete connector {} response: {}", connectName, response);
        } catch (IOException e) {
            logger.warn("delete connector {} failed,error message: {}", connectName, e.getMessage());
        } catch (WebApplicationException e) {
            if (e.getResponse().getStatus() == 404) {
                logger.warn("connector {} maybe has already being deleted from cluster {}.", connectName, job.getDeployClusterId());
            } else {
                throw e;
            }
        }
    }


    public void delTopic(Job job) {
        Set<String> allDeployTopics = job.getAllDeployTopics();
        if (CollectionUtils.isEmpty(allDeployTopics)) {
            return;
        }
        allDeployTopics.add(TopicNameUtil.parseHistoryTopicName(job.getId(), this.topicPrefix));
        if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
            Long deployClusterId = job.getDeployClusterId();
            if (deployClusterId == null) {
                return;
            }
            Cluster cluster = clusterRepository.findById(deployClusterId).get();
            allDeployTopics.add(TopicNameUtil.parseInternalTopicName(job.getId(), this.topicPrefix));
            deleteTopicsByCluster(cluster, allDeployTopics);
        } else if (job.getJobType() == Job.JobType.COLLECT) {
            List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
            for (Connector sinkConnector : sinkConnectors) {
                String id = sinkConnector.getConfig().get("id");
                if (StringUtils.isBlank(id)) {
                    return;
                }
                Cluster cluster = clusterRepository.findById(Long.valueOf(id)).get();
                allDeployTopics.add(TopicNameUtil.parseInternalTopicName(job.getId(), this.topicPrefix) + "_" + id);
                deleteTopicsByCluster(cluster, allDeployTopics);
            }
        }
    }


    public AdminClient createAdminClient(Cluster cluster) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        return AdminClient.create(props);
    }

    public Properties creatPropByCluster(Cluster cluster) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        return props;
    }


    public void deleteTopicRecord(Cluster cluster, Set<String> topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        AdminClient client = null;
        try {
            client = AdminClient.create(props);
            DeleteTopicsOptions options = new DeleteTopicsOptions();
            options.timeoutMs(8000);
            //options.retryOnQuotaViolation(false);
            DescribeTopicsResult describeTopicsResult = client.describeTopics(topics);
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
            for (String topic : stringTopicDescriptionMap.keySet()) {
                TopicDescription topicDescription = stringTopicDescriptionMap.get(topic);
                List<TopicPartitionInfo> partitions = topicDescription.partitions();

                for (TopicPartitionInfo partition : partitions) {

                }
            }
            TopicPartition topicPartition = new TopicPartition("122121", 3);
            Map<TopicPartition, RecordsToDelete> topicPartitionRecordsToDeleteMap = Collections.singletonMap(topicPartition, RecordsToDelete.beforeOffset(0L));
            //client.deleteRecords(topicPartition);

            client.close();
            logger.info("delete topics :{}", topics);

        } catch (Exception e) {
            if (e.getMessage().contains("This server does not host this topic-partition.")) {
                logger.error("topic may have been deleted");
            } else if (e.getMessage().contains("org.apache.kafka.common.errors.TimeoutException")) {
                logger.error("cluster maybe is dead");
            } else {
                logger.error(e.getMessage(), e);
                throw new RuntimeException("del topics error " + e.getMessage());
            }
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public void deleteTopicsByCluster(Cluster cluster, Set<String> topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokerList);
        if (StringUtils.isNotBlank(cluster.securityProtocol)
                && !cluster.securityProtocol.equals("NONE")
                && StringUtils.isNotBlank(cluster.saslMechanism)
                && !cluster.saslMechanism.equals("NONE")
        ) {
            String saslJaasConfig = cluster.initSaslJaasConfig();
            Matcher matcher = saslPattern.matcher(saslJaasConfig);
            if (matcher.find()) {
                String group = matcher.group();
                saslJaasConfig = saslJaasConfig.replace(cluster.password, AesUtils.wrappeDaesDecrypt(group));
            }
            props.put("sasl.jaas.config", saslJaasConfig);
            props.put("sasl.mechanism", cluster.saslMechanism);
            props.put("security.protocol", cluster.securityProtocol);
        }
        AdminClient client = null;
        try {
            client = AdminClient.create(props);
            DeleteTopicsOptions options = new DeleteTopicsOptions();
            options.timeoutMs(8000);
            //options.retryOnQuotaViolation(false);
            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(topics, options);
            deleteTopicsResult.all().get();
            client.close();
            logger.info("delete topics :{}", topics);

        } catch (Exception e) {
            if (e.getMessage().contains("This server does not host this topic-partition.")) {
                logger.error("topic may have been deleted");
            } else if (e.getMessage().contains("org.apache.kafka.common.errors.TimeoutException")) {
                logger.error("cluster maybe is dead");
            } else {
                logger.error(e.getMessage(), e);
                throw new RuntimeException("del topics error " + e.getMessage());
            }
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                }
            }
        }
    }

    //@Operation(description = "????????????")
    @GetMapping("/{jobId}/connectors")
    /*@Parameters({
            @Parameter(name = "jobId", description = "??????id", required = true, in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response getJobConnectors( @PathVariable("jobId") Long jobId,
                                      @RequestParam(value = "pageNum",defaultValue = "1",required = false)  int pageNum,
                                      @RequestParam(value = "pageSize",defaultValue = "10",required = false)  int pageSize,
                                      @RequestParam(value = "connectName",required = false)  String connectName
    ) {

        Job job = jobRepository.findById(jobId).get();
        if (job == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
            User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
            if (user == null) {
                throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
            }
            if (user.getId().longValue() != job.getCreatorId().longValue()) {
                throw new RuntimeException("no available connector for user " + user.chineseName);
            }
        }

        Map<String, Object> pageData = new HashMap<>();
        List<ConnectorStatus> connectorStatuses = new ArrayList<>();


        QConnector connectorT = QConnector.connector;

        JPAQuery<Connector> from = queryFactory.select(connectorT).from(connectorT);
        from.where(connectorT.jobId.eq(job.getId()));

        if (StringUtils.isNotEmpty(connectName)) {
            from.where(connectorT.name.like("%" + connectName + "%"));
        }

        // ?????????????????????cluster ??????sink?????????
        if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
//            List<Connector> connectors = job.connectors;

            QueryResults<Connector> connectorQueryResults = from.orderBy(connectorT.category.desc()
                    ,connectorT.id.desc()).offset(Page.getOffest(pageNum,pageSize)).limit(pageSize).fetchResults();
            List<Connector> connectors = connectorQueryResults.getResults();
            pageData.put("totalElements", connectorQueryResults.getTotal());
            for (Connector connector : connectors) {
                try {
                    ConnectorStatus connecorStatus = getConnecorStatus(job.getDeployClusterId().intValue(), connector.name);
                    connecorStatus.setDatabaseName(connector.connectorType);
                    connecorStatus.setClusterId(job.getDeployClusterId());
                    connecorStatus.setCategory(connector.category);
                    connecorStatus.setLastEventTime(connector.getLastEventTime());
                    connectorStatuses.add(connecorStatus);
                } catch (WebApplicationException e) {
                    if (e.getResponse().getStatus() == 404) {
                        logger.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                        continue;
                    } else {
                        throw e;
                    }
                }
            }
        } else if (job.getJobType() == Job.JobType.COLLECT) {
            //List<Connector> sinkConnectors = job.findSinkConnectors()
            from.where(connectorT.category.eq(Connector.Category.Sink));

            QueryResults<Connector> connectorQueryResults = from.orderBy(connectorT.category.desc()
                    ,connectorT.id.desc()).offset(Page.getOffest(pageNum,pageSize)).limit(pageSize).fetchResults();
            List<Connector> sinkConnectors = connectorQueryResults.getResults();

            pageData.put("totalElements", connectorQueryResults.getTotal());
            Connector oneSourceConnector = jobService.findOneSourceConnector(job);
            for (Connector sinkConnector : sinkConnectors) {
                String id = sinkConnector.getConfig().get("id");
                try {
                    ConnectorStatus connecorStatus = getConnecorStatus(Integer.valueOf(id), oneSourceConnector.name);
                    connecorStatus.setClusterId(Long.valueOf(id));
                    connecorStatus.setDatabaseName(oneSourceConnector.connectorType);
                    // ??????????????????????????????source
                    connecorStatus.setCategory(Connector.Category.Source);
                    connecorStatus.setLastEventTime(sinkConnector.getLastEventTime());
                    connectorStatuses.add(connecorStatus);
                } catch (WebApplicationException e) {
                    if (e.getResponse().getStatus() == 404) {
                        logger.warn("connector {} maybe has already being deleted from cluster {} .", sinkConnector.name, Integer.valueOf(id));
                        continue;
                    } else {
                        throw e;
                    }
                }
            }
        }
        pageData.put("content", connectorStatuses);

        return Response.ok(pageData).build();
    }

    @PostMapping("/{jobId}/{type}/parseTopic")
    public Response topicNameParse(@PathVariable("jobId") Long jobId, @PathVariable("type") String type, @RequestBody List<Map<String, Object>> tableNames) {
        if (CollectionUtils.isEmpty(tableNames) || StringUtils.isEmpty(type)) {
            return Response.ok(tableNames).build();
        }

        type = type.toLowerCase();
        if ("kafka".equals(type)) {
            for (Map<String, Object> tableName : tableNames) {
                if (!tableName.containsKey("name") || StringUtils.isEmpty(tableName.get("name").toString())) {
                    tableName.put("tmpAlias", "");
                    continue;
                }
                tableName.put("tmpAlias", TopicNameUtil.parseTopicName(jobId, topicPrefix, tableName.get("name").toString()));
            }
        } else if ("mysql".equals(type) || "oracle".equals(type) || "postgres".equals(type) || "tidb".equals(type)) {
            for (Map<String, Object> tableName : tableNames) {
                if (!tableName.containsKey("alias") || StringUtils.isEmpty(tableName.get("alias").toString())) {
                    tableName.put("tmpAlias", "");
                    continue;
                }
                tableName.put("tmpAlias", tableName.get("alias").toString().toUpperCase());
            }
        }

        return Response.ok(tableNames).build();
    }

    private ConnectorStatus getConnecorStatus(int cluster, String connectorName) {
        ConnectorStatus connectorState = new ConnectorStatus(connectorName);
        try {
            KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);
            ConnectConnectorConfigResponse connectorInfo = kafkaConnectClient.getConnectorInfo(connectorName);
            String connectorTypeClass = connectorInfo.getConfig().get("connector.class");
            logger.debug("Kafka Connect connector status details: " + connectorInfo);
            String connectorStatus1 = kafkaConnectClient.getConnectorStatus(connectorName);
            ConnectConnectorStatusResponse connectorStatus = JsonUtils.fromJson(connectorStatus1,ConnectConnectorStatusResponse.class);
            connectorState.setConnectorType(connectorTypeClass);
            if (connectorState.getConnectorType().toLowerCase().equals("jdbcsink")) {
                String url = connectorInfo.getConfig().get(SinkConnectorKeyword.SINK_URL);
                if (url.contains("jdbc:oracle")) {
                    connectorState.setDatabaseName("Oracle");
                } else if (url.contains("jdbc:mysql")) {
                    connectorState.setDatabaseName("MySQL");
                } else if (url.contains("jdbc:postgresql")) {
                    connectorState.setDatabaseName("PostgreSQL");
                }
            } else if (connectorState.getConnectorType().toLowerCase().equals("kafkasink")) {
                connectorState.setDatabaseName("Kafka");
            }
            connectorState.setConnectorStatus(connectorStatus.connectorStatus.status);
            connectorState.setDbServerName(connectorInfo.getConfig().get("database.server.name"));
            connectorStatus.taskStates.forEach(
                    taskStatus -> {
                        connectorState.setTaskState(
                                taskStatus.id,
                                taskStatus.status,
                                parserError(taskStatus.getErrorsAsList()));
                        if (!ConnectorStatus.State.RUNNING.equals(taskStatus.status)) {
                            connectorState.setConnectorStatus(taskStatus.status);
                        }
                    });
            return connectorState;
        } catch (IOException | KafkaConnectException e) {
            logger.error("getConnecorStatus {} on cluster {}", connectorName, cluster, e);
        }
        return connectorState;
    }

    private List<String> parserError(List<String> errorList) {
        if (errorList == null || errorList.size() == 0) {
            return null;
        }

        List<String> result = errorList.stream().filter(s -> s.startsWith("Caused by:")).collect(Collectors.toList());
        if (result == null || result.size() == 0) {
            String exception = errorList.get(0);
            result = new ArrayList<>();
            result.add(exception);
            return result;
        } else {
            return result;
        }
    }
}
