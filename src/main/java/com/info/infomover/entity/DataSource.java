package com.info.infomover.entity;

import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

@Entity
@Table(name = "infomover_datasource")
public class DataSource extends AbstractPersistable<Long> implements Serializable {

    @Column(unique = true)
    public String name;

    public String description;

    public String type;

    public DataSourceState state;

    public Long creatorId;

    public String creatorChineseName;

    public Long lastModifierId;

    public String lastModifierName;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(10) default 'ALL'")
    public DataSourceCategory category;

    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime createTime;

    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    public LocalDateTime updateTime;

    @ElementCollection(fetch = FetchType.EAGER)
    @MapKeyColumn(name = "configKey")
    @CollectionTable(name = "datasource_config", joinColumns = @JoinColumn(name = "datasource_id"))
    public Map<String, String> config;


    /*public static DataSource findByName(String name) {
        return find("name", name).firstResult();
    }*/

    public enum DataSourceState {
        AVAILABLE,
        UNAVAILABLE
    }


    public enum DataSourceCategory {
        SOURCE,
        SINK,
        ALL
    }


}
