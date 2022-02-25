package com.info.infomover.repository;

import com.info.infomover.entity.SchemaChange;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SchemaChangeRepository extends JpaRepository<SchemaChange, Long> {
/*
    @Inject
    private JobRepository jobRepository;

    @Transactional
    public void saveSchemaChange(Long jobId,
                                 Connector sourceConnector,
                                 String sourceTable,
                                 Connector sinkConnector,
                                 String sinkTable,
                                 String workingDir,
                                 String changeLogOutFileName) {
        SchemaChange schemaChange = new SchemaChange();
        schemaChange.sourceConnector = sourceConnector.name;
        schemaChange.sourceTable = sourceTable;
        schemaChange.sinkConnector = sinkConnector.name;
        schemaChange.sinkTable = sinkTable;
        schemaChange.changeLogDiffFile = changeLogOutFileName;
        String changeLogOutFile = workingDir + File.separator + changeLogOutFileName;
        schemaChange.changeLogDiffSQL = FileUtil.load(changeLogOutFile);
        schemaChange.jobId = jobId;
        schemaChange.createTime = LocalDateTime.now();
        persistAndFlush(schemaChange);

        Job job = jobRepository.findById(jobId);
        job.schemaChangedCount = job.schemaChangedCount + 1;
        jobRepository.persistAndFlush(job);
    }*/

}
