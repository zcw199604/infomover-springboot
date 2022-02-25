package com.info.infomover.entity;

import com.info.infomover.common.convrter.TableMappingConverter;
import com.io.debezium.configserver.model.ConnectorStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Entity
@Table(name = "infomover_connector")
@Getter
@Setter
public class Connector extends AbstractPersistable<Long> implements Serializable {
    private static final long serialVersionUID = 7797258955316291521L;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,generator="hibernate_sequence")
    @SequenceGenerator(name="hibernate_sequence", sequenceName="seq_hibernate")
    private Long id;

    @Column(unique = true)
    public String name;

    @Enumerated(EnumType.STRING)
    public Category category;

    public String connectorType;

    public String fullQualifiedName;

    @ElementCollection(fetch = FetchType.EAGER)
    @MapKeyColumn(name = "configKey")
    @CollectionTable(name = "connector_config", joinColumns = @JoinColumn(name = "connectId"))
    public Map<String, String> config;

    @Column(name = "job_id")
    public Long jobId;

    @Column(name = "latest_snapshot_file")
    private String latestSnapshotFile;

    @Column(name = "connect_table_name")
    private String connectTableName;

    @Column(name = "connect_topic_name")
    private String connectTopicName;

    @Lob
    @Convert(converter = TableMappingConverter.class)
    @Column(name = "table_mapping")
    private List<ConfigObject> tableMapping;
    
    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @Column(name = "last_event_time")
    private LocalDateTime lastEventTime;

    private ConnectorStatus.State connectorStatus;



    /*public String getLatestSnapshotFile() {
        return latestSnapshotFile;
    }

    public static Connector findByName(String name) {
        return find("name", name).firstResult();
    }

    public void setLatestSnapshotFile(String workingDir, String latestSnapshotFile) {
        *//*
         * delete old snapshot file
         * *//*
        if (StringUtils.isNotBlank(latestSnapshotFile)) {
            if (StringUtils.isNotBlank(this.latestSnapshotFile)) {
                log.info("Delete old snapshot file {}", this.latestSnapshotFile);
                File file = new File(workingDir + File.separator + this.latestSnapshotFile);
                if (file.exists()) {
                    file.delete();
                }
            }
            this.latestSnapshotFile = latestSnapshotFile;
        }
    }
*/
    public enum Category {
        Source,
        Sink
    }
}
