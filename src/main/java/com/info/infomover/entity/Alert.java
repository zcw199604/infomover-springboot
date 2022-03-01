package com.info.infomover.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import javax.ws.rs.DefaultValue;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "infomover_alert")
public class Alert extends AbstractPersistable<Long> implements Serializable {
    
    public String name;
    
    public String description;
    
    public Long jobId;
    public String job;
    
    public String plugin;
    
    public String connector;
    
    @Enumerated(EnumType.STRING)
    public Connector.Category category;
    
    public String cluster;
    public Long clusterId;
    
    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime createTime;
    
    @Enumerated(EnumType.STRING)
    public AlertStatus status;
    
    public Long ruleId;
    
    public String ruleName;
    
    public Long creatorId;
    public String creator;
    
    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime updateTime;
    public Long lastModifierId;
    public String lastModifier;
    
    @DefaultValue("'Job'")
    @Enumerated(EnumType.STRING)
    public Type type;


    
    public enum AlertStatus {
        firing, resolved, pending, ignore
    }
    
    public enum Type {
        Cluster, Job
    }
    

    /*

    public static boolean exists(Alert alert) {
        List<Alert> alertList = Alert.findByAlert(alert);

        return alertList!=null && alertList.size() > 0;
    }

    public static long deleteByJobId(Long jobId) {
        return Alert.delete("jobId", jobId);
    }

    public static List<Alert> findByAlert(Alert alert){
        Map<String, Object> paramMap = new HashMap<>();
        List<String> query = new ArrayList<>();
    
        if (alert.name !=null) {
            query.add("name=:name");
            paramMap.put("name", alert.name);
        }
        if (StringUtils.isNotBlank(alert.connector)) {
            query.add("connector=:connector");
            paramMap.put("connector", alert.connector);
        }
        if (alert.jobId!=null) {
            query.add("jobId=:jobId");
            paramMap.put("jobId", alert.jobId);
        }
        if (StringUtils.isNotBlank(alert.job)) {
            query.add("job=:job");
            paramMap.put("job", alert.job);
        }
        if (StringUtils.isNotBlank(alert.cluster)) {
            query.add("cluster=:cluster");
            paramMap.put("cluster", alert.cluster);
        }
        if (StringUtils.isNotBlank(alert.plugin)) {
            query.add("plugin=:plugin");
            paramMap.put("plugin", alert.plugin);
        }
        if (alert.category!=null) {
            query.add("category=:category");
            paramMap.put("category", alert.category);
        }
    
        if (alert.status!=null) {
            query.add("status=:status");
            paramMap.put("status", alert.status);
        }
        
        if (alert.type!=null) {
            query.add("type =: type");
            paramMap.put("type", alert.type);
        }
    
        return find(String.join(" and ", query), paramMap).list();
    }*/
}
