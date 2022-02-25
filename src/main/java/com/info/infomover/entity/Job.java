package com.info.infomover.entity;

import com.info.infomover.common.convrter.LinkToStringConverter;
import com.info.infomover.common.convrter.SetStringConverter;
import com.info.infomover.common.convrter.StepToStringConverter;
import lombok.Data;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

@Entity
@Data
@Table(name = "infomover_job", uniqueConstraints = {@UniqueConstraint(columnNames = {"name", "creatorId"})})
public class Job extends AbstractPersistable<Long> implements Serializable {
    private static final long serialVersionUID = 7717258955316291521L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY,generator="hibernate_sequence")
    @SequenceGenerator(name="hibernate_sequence", sequenceName="seq_hibernate")
    private Long id;

    @NotBlank
    private String name;

    private String note;

    private Long creatorId;

    private String creatorChineseName;

    private String lastModifier;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_type")
    private JobType jobType;

    @Lob
    @Convert(converter = StepToStringConverter.class)
    @Column(columnDefinition = "longtext")
    private List<StepDesc> steps;

    @Lob
    @Convert(converter = LinkToStringConverter.class)
    @Column(columnDefinition = "text")
    private List<LinkDesc> links;

    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;

    @Column(columnDefinition = "DATETIME")
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime updateTime;

    @OneToMany(
            targetEntity = Connector.class,fetch = FetchType.LAZY,
            cascade = {CascadeType.ALL},
            orphanRemoval = true
    )
    @JoinColumn(name = "job_id")
    private List<Connector> connectors;

    @Enumerated(EnumType.STRING)
    private DeployStatus deployStatus;

    @Enumerated(EnumType.STRING)
    private RecoveryStatus recoveryStatus;

    @Enumerated(EnumType.STRING)
    private RecollectStatus recollectStatus;

    @Column(name = "deploy_cluster")
    private String deployCluster;

    @Column(name = "deploy_cluster_id")
    private Long deployClusterId;

    @Column(columnDefinition = "int default '0'")
    private int schemaChangedCount;

    @Column(name = "key_word", columnDefinition = "text")
    private String keyWord;

    @Column(name = "project")
    private String project;

    private String theme;

    private String owner;

    /**
     * 是否启动快照监控
     */
    @Column(columnDefinition = "boolean default false")
    private boolean snapshot = false;

    @Enumerated(EnumType.STRING)
    @Column(name = "snapshot_status")
    private SnapshotStatus snapshotStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "sink_real_type")
    private SinkRealType sinkRealType;

    /**
     * 已经发布过的topic名称
     */
    @Lob
    @Convert(converter = SetStringConverter.class)
    @Column(name = "all_deploy_topics")
    private Set<String> allDeployTopics;

    @Column(name = "source_category")
    private String sourceCategory;

    @Column(name = "sink_category")
    private String sinkCategory;

    
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @Column(name = "last_event_time")
    private LocalDateTime lastEventTime;


    @Transient
    private long alertCount;

    @Transient
    private String topicPrefix;

    @Transient
    private RunningStatus runningStatus;

    public enum DeployStatus {
        UN_DEPLOYED,
        DEPLOYED,
        PAUSED,
        ERROR
    }

    public enum RunningStatus {
        NOT_AVAILABLE,
        FULL_SUCCESS,
        PARTIAL_SUCCESS,
        FAILED
    }

    public enum RecoveryStatus{
        RECOVERYING, // 恢复中
        RECOVERYFAILED, // 恢复失败
        RECOVERYSUCCESS, // 恢复成功
        RECOVERYTIMEOUT
    }

    public enum RecollectStatus{
        RECOLLECTING, // 重采中
        RECOLLECTFAILED, // 重采失败
        RECOLLECTSUCCESS, // 重采成功
        RECOLLECTTIMEOUT
    }

    public enum JobType {
        COLLECT,
        SYNC
    }

    public enum SnapshotStatus {
        IN_PROGRESS,
        COMPLETED,
        FAILED
    }

    public enum StepType{
        sources,
        sinks,
        clusters
    }

    public enum SinkRealType{
        NONE,//非流式采集
        INTERNAL,//内部
        EXTERNAL//外部
    }
}
