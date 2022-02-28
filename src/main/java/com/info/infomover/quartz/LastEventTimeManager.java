package com.info.infomover.quartz;

import com.info.infomover.entity.Connector;
import com.info.infomover.entity.Job;
import com.info.infomover.entity.ServiceInstance;
import com.info.infomover.prom.PromClient;
import com.info.infomover.prom.query.PromFactor;
import com.info.infomover.prom.query.PromQuery;
import com.info.infomover.prom.request.QueryRequest;
import com.info.infomover.prom.response.PromResult;
import com.info.infomover.repository.ConnectorRepository;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.repository.ServiceInstanceRepository;
import com.info.infomover.service.JobService;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@ApplicationScoped
public class LastEventTimeManager {
    
    private static final Logger logger = LoggerFactory.getLogger(LastEventTimeManager.class);
    
    private static AtomicBoolean running = new AtomicBoolean(false);
    
    @Inject
    @RestClient
    PromClient promClient;

    @Autowired
    private ConnectorRepository connectorRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ServiceInstanceRepository serviceInstanceRepository;

    @Scheduled(cron = "{connector.lastEventTime.interval.cron}")
    public void refresh() {
        if (running.get()) {
            return;
        }

        synchronized (LastEventTimeManager.class) {
            if (running.get()) {
                return;
            }
            running.set(true);
        }

        try {
            logger.info("last event time refresh action at {}", LocalDateTime.now());
            List<Job> jobList = loadJob();

            for (Job job : jobList) {
                List<Connector> byJobId = connectorRepository.findByJobId(job.getId());
                job.setConnectors(byJobId);

                List<Connector> sourceConnectors = jobService.findSourceConnectors(job);
                if (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.INTERNAL.equals(job.getSinkRealType())) {
                    sourceConnectors = jobService.findSinkConnectors(job);
                }
                LocalDateTime lastEventTime = getLastEventTime(job, sourceConnectors);
                if (lastEventTime != null) {
                    updateLastEventTime(job, lastEventTime);
                }
            }
            logger.info("last event time refresh end at {}", LocalDateTime.now());
        } catch (Exception e) {
            logger.error("last event time refresh error:{}", e.getMessage(), e);
        } finally {
            running.set(false);
        }

    }
    
    private LocalDateTime getLastEventTime(Job job, List<Connector> connectors){
        long ms = Long.MAX_VALUE;
        for (Connector connector : connectors) {
            String name = connector.getConfig().get("database.server.name");
            if (Job.JobType.COLLECT.equals(job.getJobType()) && Job.SinkRealType.INTERNAL.equals(job.getSinkRealType())) {
                name = connector.getConfig().get("dbServerName");
            }
            PromQuery promQuery = PromQuery.withoutMethod("debezium_metrics_MilliSecondsSinceLastEvent", PromFactor.factor("name", name));
            try {
                QueryRequest request = QueryRequest.build(promQuery, System.currentTimeMillis() / 1000);
                Response resp = promClient.query(request);
                PromResult promResult = resp.readEntity(PromResult.class);
                if (promResult!=null && "success".equals(promResult.getStatus())) {
                    PromResult.CommonData data = promResult.getData();
                    if (data.getResult()!=null && data.getResult().size()>0) {
                        for (PromResult.Result result : data.getResult()) {
                            long connectorMs = Long.parseLong((String) result.getValue().get(1));
                            if (connectorMs == -1){
                                continue;
                            }
                            ms = Math.min(ms, connectorMs);
                        }
                        updateLastEventTime(connector, LocalDateTime.now().minus(ms, ChronoUnit.MILLIS));
                    }
                }
            } catch (Exception e) {
                logger.error("Job:{} get last event time error:{}", job.getName(), e.getMessage(), e);
            }
        }
        if (ms < Long.MAX_VALUE) {
            return LocalDateTime.now().minus(ms, ChronoUnit.MILLIS);
        }
        return null;
    }
    
    @Transactional
    public List<Job> loadJob() {
        List<Job> jobList = jobRepository.findByDeployStatus(Job.DeployStatus.DEPLOYED);
        List<ServiceInstance> instanceList = serviceInstanceRepository.findAll();
        instanceList = instanceList.stream().sorted(Comparator.comparing(o -> o.getId())).collect(Collectors.toList());
        
        int instanceIndex = 0;
        for (int i = 0; i < instanceList.size(); i++) {
            ServiceInstance instance = instanceList.get(i);
            if (instance.host.equals(InstanceRegister.instance.host)
                    && instance.port.equals(InstanceRegister.instance.port)) {
                instanceIndex = i;
            }
        }
        int instanceSize = instanceList==null || instanceList.size()==0 ? 1 : instanceList.size();
        Iterator<Job> iterator = jobList.iterator();
        while (iterator.hasNext()) {
            Job job = iterator.next();
            if (job.getId().hashCode() % instanceSize!= instanceIndex) {
                iterator.remove();
                continue;
            }
            jobService.findSourceConnectors(job);
        }
        logger.info("last event time refresh Job count {}", jobList.size());
        
        return jobList;
    }
    
    @Transactional
    public void updateLastEventTime(Job job, LocalDateTime lastEventTime){
        job = jobRepository.findById(job.getId()).get();
        job.setLastEventTime(lastEventTime);
        jobRepository.saveAndFlush(job);
    }
    
    @Transactional
    public void updateLastEventTime(Connector connector, LocalDateTime lastEventTime){
        connector = connectorRepository.findById(connector.getId()).get();
        connector.setLastEventTime(lastEventTime);
        connectorRepository.saveAndFlush(connector);
    }
}
