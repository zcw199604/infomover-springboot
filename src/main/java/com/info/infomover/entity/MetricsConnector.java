package com.info.infomover.entity;

import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @author zcw
 */
@Entity
@Table(name = "metrics_connector")
public class MetricsConnector extends AbstractPersistable<Long> implements Serializable {

    private static final long serialVersionUID = 7797250955316291521L;

    @NotBlank
    //mysql/oracle/postgres
    public String connector;

    @NotBlank
    public String urlPrefix;//http://192.168.1.83:3300/d-solo/7HrWpmEiz/debezium?orgId=1

    @NotBlank
    //panelId:width:height:frameborder
    public String panelIds;//14:450:200:0,36:450:200:0,37:450:200:0,38:450:200:0,40:450:200:0,18:450:200:0,16:450:200:0
    @Column(columnDefinition = "DATETIME")
    public LocalDateTime updateTime;

    /*public static PanacheQuery<PanacheEntityBase> findByConnector(String connector) {
        return find("connector", connector);
    }*/

}
