package com.info.infomover.quartz;

import com.info.infomover.entity.Alert;
import com.info.infomover.entity.AlertRule;
import com.info.infomover.entity.Job;
import com.info.infomover.entity.ServiceInstance;
import com.info.infomover.repository.AlertRepository;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.repository.ServiceInstanceRepository;
import com.info.infomover.service.JobService;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


@ApplicationScoped
public class JobAlertManager {
    
    private static final Logger logger = LoggerFactory.getLogger(JobAlertManager.class);

    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private ServiceInstanceRepository serviceInstanceRepository;
    
    @ConfigProperty(name = "connector.alert.fire.pause.minutes", defaultValue = "5")
    private int pauseMinutes;
    
    private static AtomicBoolean running = new AtomicBoolean(false);
    
    private LocalDateTime startTime = LocalDateTime.now();
    
    public static final Map<Long, AlertRule> ruleMap = new HashMap<>();
    public static Set<String> pauseAlertConnector ;
    
    @Inject
    JobAlertWorker worker;
    
    @Scheduled(cron = "{connector.alert.interval.cron}")
    public void execute() {
        if (running.get()) {
            return;
        }
        
        synchronized (JobAlertManager.class) {
            if (running.get()) {
                return;
            }
            running.set(true);
        }
        logger.info("alert manager action at {}", startTime);
        try {
            ruleMap.clear();
            startTime = LocalDateTime.now();
            pauseAlertConnector = loadAlert(startTime);
            List<Job> jobs = loadJob(startTime);
            if (jobs == null || jobs.size() == 0) {
                return;
            }
            for (Job job : jobs) {
                //worker.execute(job);
            }
        } catch (Throwable e) {
            logger.error("alert manager error ", e);
        } finally {
            running.set(false);
        }
        
        logger.info("alert manager end at {}", LocalDateTime.now());
    }
    
    @Transactional
    public Set<String> loadAlert(LocalDateTime now){

        //queryFactory.select(QAlert.c)
        LocalDateTime minus = now.minus(pauseMinutes, ChronoUnit.MINUTES);
        /*List<Alert> alerts = Alert.find("updateTime>?1 and status in (?2, ?3) and type=?4",
                Sort.by("connector", "jobId", "clusterId"),
                now.minus(pauseMinutes, ChronoUnit.MINUTES),
                Alert.AlertStatus.resolved, Alert.AlertStatus.ignore, Alert.Type.Job).list();*/
        List<Alert> alerts = new ArrayList<>();
        return alerts.stream()
                .map(alert -> alert.connector + "_" + alert.job + "_" + alert.category.name() + "_" + alert.cluster)
                .collect(Collectors.toSet());
    }
    
    @Transactional
    public List<Job> loadJob(LocalDateTime startTime) {
        /*List<Job> jobList = Job.find("deployStatus=?1 and updateTime<?2",
                Job.DeployStatus.DEPLOYED,
                startTime.minus(pauseMinutes, ChronoUnit.MINUTES))
                .list();*/
        List<Job> jobList = new ArrayList<>();
        List<ServiceInstance> instanceList = serviceInstanceRepository.findAll();
        instanceList = instanceList.stream().sorted(Comparator.comparing(o -> o.getId())).collect(Collectors.toList());
        
        int instanceIndex = 0;
        for (int i = 0; i < instanceList.size(); i++) {
            ServiceInstance instance = instanceList.get(i);
            if (instance.getId().equals(InstanceRegister.instance.getId())) {
                instanceIndex = i;
            }
        }
        int instanceSize = instanceList==null || instanceList.size()==0 ? 1 : instanceList.size();
        Iterator<Job> iterator = jobList.iterator();
        while (iterator.hasNext()) {
            Job job = iterator.next();
            if (job.getId().hashCode() % instanceSize!= instanceIndex) {
                iterator.remove();
            }
            jobService.findSourceConnectors(job);
        }
        logger.info("alert manager job count {}", jobList.size());
    
        return jobList;
    }
    
}
