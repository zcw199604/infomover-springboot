package com.info.infomover.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.info.infomover.prom.constants.MetricMethod;
import com.info.infomover.prom.constants.StepDuration;
import lombok.Data;
import org.hibernate.annotations.ColumnDefault;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "infomover_alert_rule")
public class AlertRule extends AbstractPersistable<Long> implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,generator="hibernate_sequence")
    @SequenceGenerator(name="hibernate_sequence", sequenceName="seq_hibernate")
    private Long id;

    public String name;
    
    @Enumerated(EnumType.STRING)
    public MetricMethod method;
    
    public String metric;
    
    public String operator;
    
    public Double threshold;
    
    public Integer keepTime;
    
    @Enumerated(EnumType.STRING)
    public StepDuration duration;
    
    public String description;
    
    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime createTime;
    
    public Long creatorId;
    
    public String creator;
    
    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime updateTime;
    
    public Long lastModifierId;
    
    public String lastModifier;
    
    @Enumerated(EnumType.STRING)
    @ColumnDefault("'Job'")
    public Target target;
    
    @Transient
    public Integer priority;
    
    @Enumerated(EnumType.STRING)
    @ColumnDefault("'custom'")
    public RuleType type;
    
    public enum Target {
        Job, Cluster
    }
    
    public enum RuleType{
        builtin, builtinKafka, custom
    }
}
