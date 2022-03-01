package com.info.infomover.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@Table(name = "infomover_instance")
public class ServiceInstance  extends AbstractPersistable<Long> implements Serializable {
    
    public String host;
    
    public Integer port;
    
    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime createTime;
    
    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime updateTime;
}
