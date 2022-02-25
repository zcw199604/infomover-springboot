package com.info.infomover.quartz;

import com.info.infomover.entity.ServiceInstance;
import com.info.infomover.repository.ServiceInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Component
public class InstanceRegister {
    
    private static final Logger logger = LoggerFactory.getLogger(InstanceRegister.class);


    @Autowired
    private ServiceInstanceRepository serviceInstanceRepository;

    @Value("${quarkus.http.host:localhost}")
    private String host;
    
    @Value("${quarkus.http.port:9090}")
    private Integer port;
    
    @Value("${infomover.service.keepTime:5}")
    private Integer keepMinute;
    
    public static ServiceInstance instance;

    @Transactional
    @PostConstruct
    public void start(){
        logger.info("server start at {}", LocalDateTime.now());
        init();
    }

    @Transactional
    @PreDestroy
    public void shutdown(){
        logger.info("server shutdown at {}", LocalDateTime.now());
        destroy();
    }
    

    public void init () {
        instance = new ServiceInstance();
        instance.host = host;
        instance.port = port;
        instance.createTime = LocalDateTime.now();
        instance.updateTime = LocalDateTime.now();

        serviceInstanceRepository.saveAndFlush(instance);
        logger.info("instance id is {}", instance.getId());
    }
    

    public void destroy () {
        serviceInstanceRepository.deleteById(instance.getId());
    }

    @Transactional
    @Scheduled(cron = "${connector.instance.interval.cron}")
    public void refresh() {
        List<ServiceInstance> instanceList = serviceInstanceRepository.findAll();
        for (ServiceInstance serviceInstance : instanceList) {
            if (serviceInstance.getId().compareTo(instance.getId()) == 0) {
                serviceInstance.updateTime = LocalDateTime.now();
                serviceInstanceRepository.saveAndFlush(serviceInstance);
                logger.info("instance {} host:{} port:{} refresh", serviceInstance.getId(), serviceInstance.host, serviceInstance.port);
            } else {
                if (serviceInstance.updateTime.plus(keepMinute, ChronoUnit.MINUTES).isBefore(LocalDateTime.now())) {
                    logger.info("instance {} host:{} port:{} timeout", serviceInstance.getId(), serviceInstance.host, serviceInstance.port);
                    serviceInstanceRepository.deleteById(serviceInstance.getId());
                }
            }
        }
    }
}
