package com.info.infomover.entity;

import lombok.Data;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "infomover_job_alert_rule")
public class JobAlertRule extends AbstractPersistable<Long> implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,generator="hibernate_sequence")
    @SequenceGenerator(name="hibernate_sequence", sequenceName="seq_hibernate")
    private Long id;

    public Long jobId;
    
    public Long ruleId;
    public String ruleName;
    
    public Integer priority;
    
    public Long creatorId;
    
    public String creator;
    
    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime createTime;

    
    /*public static Long deleteByJobId(Long jobId){
        return JobAlertRule.delete("jobId", jobId);
    }*/
}
