package com.info.infomover.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.data.jpa.domain.AbstractPersistable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@Table(name = "infomover_schema_change")
public class SchemaChange extends AbstractPersistable<Long> implements Serializable {

    private static final long serialVersionUID = 7797258955316291521L;

    public Long jobId;

    public String changeLogDiffFile;

    @Column(columnDefinition = "TEXT")
    public String changeLogDiffSQL;

    //@JsonbDateFormat(value = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    @Column(columnDefinition = "DATETIME")
    public LocalDateTime createTime;

    public String sourceConnector;

    public String sourceTable;

    public String sinkConnector;

    public String sinkTable;

    public boolean applied;

    public static long deleteByJobId(String workingDir, long jobId) {
        /*List<SchemaChange> schemaChanges = find("jobId", jobId).list();
        for (SchemaChange schemaChange : schemaChanges) {
            String changeLogDiffFile = schemaChange.changeLogDiffFile;
            if (StringUtils.isNotBlank(changeLogDiffFile)) {
                File file = new File(workingDir + File.separator + changeLogDiffFile);
                if (file.exists()) {
                    file.delete();
                }
            }
            schemaChange.delete();
        }
        return schemaChanges.size();*/
        return 0;
    }

}
