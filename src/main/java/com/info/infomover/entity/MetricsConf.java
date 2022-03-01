package com.info.infomover.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @Author: haijun
 * @Date: 2021/10/21 14:47
 */
@Entity
@Table(name = "infomover_metrics_conf")
public class MetricsConf extends AbstractPersistable<Long> implements Serializable {
    private static final long serialVersionUID = 7724521030565864088L;

    @NotBlank
    public String connectorType;//mysql/oracle/postgres
    @NotBlank
    public String urlPrefix;//http://192.168.1.83:3300/d-solo/7HrWpmEiz/debezium?orgId=1
    @NotBlank
    @Column(length = 600)
    //panelId:width:height:frameborder
    public String panelIds;//14:450:200:0,36:450:200:0,37:450:200:0,38:450:200:0,40:450:200:0,18:450:200:0,16:450:200:0

    @Column(length = 600)
    public String trashPanelIds;

    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime updateTime;


    /*public static MetricsConf findByConnectorType(String connectorType) {
        return find("connectorType", connectorType).firstResult();
    }*/
}
