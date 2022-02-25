package com.info.infomover.schemachange.liquibase;//package com.info.infomover.schemachange.liquibase;
//
//import com.info.infomover.entity.Connector;
//import com.info.infomover.entity.Job;
//import com.info.infomover.entity.SchemaChange;
//import com.info.infomover.schemachange.monitor.SchemaChangeProcessor;
//import com.info.infomover.util.FileUtil;
//import lombok.extern.slf4j.Slf4j;
//
//import javax.enterprise.context.ApplicationScoped;
//import javax.inject.Inject;
//import javax.transaction.Transactional;
//import java.io.File;
//import java.time.LocalDateTime;
//
//@ApplicationScoped
//@Transactional
//@Slf4j
//public class LiquibaseJobManager {
//
//    @Inject
//    private SchemaChangeProcessor processor;
//
//    public Job findJobByIdAndLoadConnector(Object jobId) {
//        Job job = Job.findById(jobId);
//        job.connectors.size();//懒加载
//        return job;
//    }
//
//    public void saveJobSnapshot(long jobId, Job.SnapshotStatus snapshotStatus) {
//        log.info("Update job snapshot {}", snapshotStatus);
//        Job job = Job.findById(jobId);
//        if (job != null) {
//            job.snapshotStatus = snapshotStatus;
//            job.persistAndFlush();
//        } else {
//            log.error("Not found job by id: {}", jobId);
//        }
//    }
//
//    public void saveConnectorLatest(long connectorId, String latestSnapshotFile) {
//        log.info("Update connector latestSnapshotFile to {}", latestSnapshotFile);
//        Connector connector = Connector.findById(connectorId);
//        if (connector != null) {
//            connector.setLatestSnapshotFile(processor.getWorkingDir(), latestSnapshotFile);
//            connector.persistAndFlush();
//        } else {
//            log.error("Not found connector by id: {}", connectorId);
//        }
//    }
//
//    public void saveSchemaChange(Long jobId,
//                                 Connector sourceConnector,
//                                 String sourceTable,
//                                 Connector sinkConnector,
//                                 String sinkTable,
//                                 String workingDir,
//                                 String changeLogOutFileName) {
//        SchemaChange schemaChange = new SchemaChange();
//        schemaChange.sourceConnector = sourceConnector.name;
//        schemaChange.sourceTable = sourceTable;
//        schemaChange.sinkConnector = sinkConnector.name;
//        schemaChange.sinkTable = sinkTable;
//        schemaChange.changeLogDiffFile = changeLogOutFileName;
//        String changeLogOutFile = workingDir + File.separator + changeLogOutFileName;
//        schemaChange.changeLogDiffSQL = FileUtil.load(changeLogOutFile);
//        schemaChange.jobId = jobId;
//        schemaChange.createTime = LocalDateTime.now();
//        schemaChange.persistAndFlush();
//
//        Job job = Job.findById(jobId);
//        job.schemaChangedCount = job.schemaChangedCount + 1;
//        job.persist();
//    }
//
//    public Connector findConnectorByName(String connectorName) {
//        Connector sourceConnector = Connector.findByName(connectorName);
//        return sourceConnector;
//    }
//}
