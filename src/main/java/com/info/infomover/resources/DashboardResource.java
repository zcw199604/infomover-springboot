package com.info.infomover.resources;

import com.info.infomover.entity.Connector;
import com.info.infomover.entity.Job;
import com.info.infomover.entity.QJob;
import com.info.infomover.entity.User;
import com.info.infomover.prom.PromClient;
import com.info.infomover.prom.constants.MetricMethod;
import com.info.infomover.prom.constants.StepDuration;
import com.info.infomover.prom.query.PromFactor;
import com.info.infomover.prom.query.PromQuery;
import com.info.infomover.prom.request.RangeQueryRequest;
import com.info.infomover.prom.response.PromResult;
import com.info.infomover.prom.response.PromResultConverter;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.security.SecurityUtils;
import com.info.infomover.service.JobService;
import com.info.infomover.util.ConnectorUtil;
import com.io.debezium.configserver.model.ConnectorStatus;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RequestMapping("/api/dashboard")
@ResponseBody
@Controller
//@Tag(name = "infomover dashboard", description = "dashboard管理接口")
public class DashboardResource {
    private static final Logger logger = LoggerFactory.getLogger(DashboardResource.class);

    private static final String METRIC_SOURCE_READ = "debezium_metrics_TotalNumberOfEventsSeen";
    private static final String METRIC_JDBC_SINK = "jdbc_sink_total";
    private static final String METRIC_KAFKA_SINK = "kafka_sink_total";

    @Inject
    PromClient promClient;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    //@Operation(description = "任务详情")
    @GetMapping("/job")
    @RolesAllowed({"User", "Admin"})
    public Response jobView() {
        Map<Job.DeployStatus, Integer> jobMap = Arrays.stream(Job.DeployStatus.values())
                .collect(Collectors
                        .toMap(deployStatus -> deployStatus, deployStatus -> 0));

        Map<ConnectorStatus.State, Integer> connectorMap = Arrays.stream(ConnectorStatus.State.values())
                .collect(Collectors.toMap(state -> state, state -> 0));
        int connectorTotal = 0;

        List<Job> jobList;
        User loginUser = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        if (User.Role.User.name().equals(loginUser.getRole())) {
            jobList = jobRepository.findByCreatorId(loginUser.getId());
        } else {
            jobList = jobRepository.findAll();
        }
        jobList = jobRepository.findAll();
        Map<Long, List<String>> clusterConnector = new HashMap<>();

        int size = 0;
        for (Job job : jobList) {
            Integer counter = jobMap.get(job.getDeployStatus());
            jobMap.put(job.getDeployStatus(), counter + 1);

            List<Connector> connectorList;

            if (Job.DeployStatus.UN_DEPLOYED.equals(job.getDeployStatus())) {
                size = job.getConnectors() == null ? 0 : job.getConnectors().size();

                if (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.INTERNAL.equals(job.getSinkRealType())) {
                    size = jobService.findSinkConnectors(job) == null ? 0 : jobService.findSinkConnectors(job).size();
                }
                connectorMap.put(ConnectorStatus.State.UNASSIGNED, connectorMap.getOrDefault(ConnectorStatus.State.UNASSIGNED, 0) + size);
            } else {
                if (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.INTERNAL.equals(job.getSinkRealType())) {
                    Connector sourceConnector = jobService.findOneSourceConnector(job);
                    connectorList = jobService.findSinkConnectors(job);
                    size = jobService.findSinkConnectors(job) == null ? 0 : jobService.findSinkConnectors(job).size();
                    for (Connector connector : connectorList) {
                        Long cluster = Long.parseLong(connector.getConfig().get("id"));
                        List<String> connectorNames = clusterConnector.computeIfAbsent(cluster, s -> new ArrayList<>());
                        connectorNames.add(sourceConnector.name + cluster);
                    }
                } else if (Job.JobType.SYNC.equals(job.getJobType()) || (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.EXTERNAL.equals(job.getSinkRealType()))) {
                    connectorList = job.getConnectors();
                    size = job.getConnectors() == null ? 0 : job.getConnectors().size();
                    for (Connector connector : connectorList) {
                        List<String> connectorNames = clusterConnector.computeIfAbsent(job.getDeployClusterId(), s -> new ArrayList<>());
                        connectorNames.add(connector.name + job.getDeployClusterId());
                    }
                }
            }
            connectorTotal += size;
        }

        for (Map.Entry<Long, List<String>> entry : clusterConnector.entrySet()) {
            try {
                Map<String, ConnectorStatus.State> statusMap = ConnectorUtil.listWithStatus(entry.getKey());
                for (String connectorCluster : entry.getValue()) {
                    ConnectorStatus.State state = statusMap.getOrDefault(connectorCluster, null);
                    if (state == null) {
                        connectorMap.put(ConnectorStatus.State.DESTROYED, connectorMap.getOrDefault(ConnectorStatus.State.DESTROYED, 0) + 1);
                    } else {
                        connectorMap.put(state, connectorMap.getOrDefault(state, 0) + 1);
                    }
                }
            } catch (IOException e) {
                logger.warn("ClusterId:{} get connector status error, then set DESTROYED \n {}", entry.getKey(), e.getMessage(), e);
                connectorMap.put(ConnectorStatus.State.DESTROYED, connectorMap.getOrDefault(ConnectorStatus.State.DESTROYED, 0) + entry.getValue().size());
            }
        }

        return Response.ok(JobConnectorView.build(jobMap, jobList.size(), connectorMap, connectorTotal)).build();
    }

    //@Operation(description = "指标变化曲线")
    @PostMapping("/metric")
    @RolesAllowed({"User", "Admin"})
    public Response promMetric( @RequestParam(value = "start",required = false)  Long start, @RequestParam(value = "end",required = false)  Long end,
                               @RequestBody List<Long> jobIds) {

        Map<String, Map<String, String>> metricConnector = loadDeployedJob(jobIds, SecurityUtils.getCurrentUserUsername());

        long range = (end - start) / 1000;
        int step;
        StepDuration duration;

        if (range <= 0) {
            step = 432;
            duration = StepDuration.S;
            end = System.currentTimeMillis();
            start = end - 60 * 60 * 24 * 1000;
        } else {
            step = (int) (range / 200);
            duration = StepDuration.S;
        }

        Map<Long, Map<String, Object>> resultMap = new HashMap<>();

        List<String> promNames;
        PromQuery promQuery = null;

        for (Map.Entry<String, Map<String, String>> entry : metricConnector.entrySet()) {
            String metricName = entry.getKey();
            Map<String, String> dbServerNameConnectorName = entry.getValue();
            if (dbServerNameConnectorName.size() > 0) {
                promNames = new ArrayList<>(dbServerNameConnectorName.keySet());
                if (METRIC_SOURCE_READ.equals(metricName)) { // source connector
                    promQuery = PromQuery.withMethod(METRIC_SOURCE_READ, MetricMethod.Counter_Increase.methodName, 1, "m",
                            PromFactor.factor("name", promNames),
                            PromFactor.factor("context", "streaming"));
                } else if (METRIC_JDBC_SINK.equals(metricName)) { // jdbc sink connector
                    promQuery = PromQuery.withMethod(METRIC_JDBC_SINK, MetricMethod.Counter_Increase.methodName, 1, "m",
                            PromFactor.factor("name", promNames));
                } else if (METRIC_KAFKA_SINK.equals(metricName)) { // kafka sink connector
                    promQuery = PromQuery.withMethod(METRIC_KAFKA_SINK, MetricMethod.Counter_Increase.methodName, 1, "m",
                            PromFactor.factor("name", promNames));
                }
                if (promQuery != null) {
                    RangeQueryRequest request = RangeQueryRequest.build(promQuery, start / 1000, end / 1000, step, duration);
                    PromResult sourceResult = promClient.queryRange(request).readEntity(PromResult.class);
                    if ("success".equals(sourceResult.getStatus())) {
                        PromResultConverter.convertMatrix(sourceResult.getData(), resultMap, dbServerNameConnectorName);
                    } else {
                        logger.error("get connector matrix error:{}", sourceResult);
                        throw new RuntimeException("get connector matrix error");
                    }
                }
            }
        }

        Set<String> keySet = new HashSet<>();
        resultMap.values().forEach(map -> keySet.addAll(map.keySet()));
        List<Map<String, Object>> collect = resultMap.values().stream()
                .sorted(Comparator.comparing(o -> ((Long) o.get("time"))))
                .peek(map -> keySet.forEach(element -> map.computeIfAbsent(element, s -> "")))
                .collect(Collectors.toList());

        return Response.ok(collect).build();
    }

    public static class JobConnectorView {
        public Map<Job.DeployStatus, Integer> job;
        public Integer jobTotal = 0;
        public Map<ConnectorStatus.State, Integer> connector;
        public Integer connectorTotal = 0;

        public static JobConnectorView build(Map<Job.DeployStatus, Integer> job, Integer jobTotal,
                                             Map<ConnectorStatus.State, Integer> connector, Integer connectorTotal) {
            JobConnectorView view = new JobConnectorView();
            view.job = job;
            view.jobTotal = jobTotal;
            view.connector = connector;
            view.connectorTotal = connectorTotal;

            return view;
        }
    }


    @Transactional
    public Map<String, Map<String, String>> loadDeployedJob(List<Long> jobIds, String userName) {
        List<Job> jobList;

        if (jobIds == null || jobIds.isEmpty()) {
            QJob job = QJob.job;
            JPAQuery<Job> from = queryFactory.select(job).from(job);
            from.where(job.deployStatus.eq(Job.DeployStatus.DEPLOYED));

            if (SecurityUtils.getCurrentRole().contains(User.Role.User.name())) {
                User user = userRepository.findByName(userName);
                if (user == null) {
                    throw new RuntimeException("no user found with name " + userName);
                }
                User loginUser = userRepository.findByName(userName);
                from.where(job.creatorId.eq(loginUser.getId()));
            }
            jobList = from.offset(0).limit(8).fetchResults().getResults();
        } else {
            jobList = new ArrayList<>();
            for (Long jobId : jobIds) {
                Job job = jobRepository.findById(jobId).get();
                if (Job.DeployStatus.DEPLOYED.equals(job.getDeployStatus())) {
                    jobList.add(job);
                }
            }
        }
        return distConnector(jobList);
    }

    private Map<String, Map<String, String>> distConnector(List<Job> jobList) {
        Map<String, Map<String, String>> resultMap = new HashMap<>();

        resultMap.put(METRIC_SOURCE_READ, new HashMap<>());
        resultMap.put(METRIC_JDBC_SINK, new HashMap<>());
        resultMap.put(METRIC_KAFKA_SINK, new HashMap<>());

        for (Job job : jobList) {

            Connector sourceConnector = jobService.findOneSourceConnector(job);
            List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
            if (Job.JobType.SYNC.equals(job.getJobType())) {
                resultMap.get(METRIC_SOURCE_READ).put(sourceConnector.getConfig().get("database.server.name"), sourceConnector.name);

                resultMap.get(METRIC_JDBC_SINK).putAll(sinkConnectors.stream().collect(Collectors.toMap(connector -> connector.name, connector -> connector.name)));
            } else if (Job.JobType.COLLECT.equals(job.getJobType())) {
                if (Job.SinkRealType.EXTERNAL.equals(job.getSinkRealType())) {
                    resultMap.get(METRIC_SOURCE_READ).put(sourceConnector.getConfig().get("database.server.name"), sourceConnector.name);

                    resultMap.get(METRIC_KAFKA_SINK).putAll(sinkConnectors.stream().collect(Collectors.toMap(connector -> connector.name, connector -> connector.name)));
                } else if (Job.SinkRealType.INTERNAL.equals(job.getSinkRealType())) {
                    resultMap.get(METRIC_SOURCE_READ).putAll(sinkConnectors.stream().collect(Collectors.toMap(connector -> connector.getConfig().get("dbServerName"), connector -> sourceConnector.name)));
                }
            }
        }
        return resultMap;
    }
}
