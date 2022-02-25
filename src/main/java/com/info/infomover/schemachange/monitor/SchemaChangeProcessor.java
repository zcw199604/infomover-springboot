package com.info.infomover.schemachange.monitor;

import com.info.infomover.repository.ConnectorRepository;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.repository.SchemaChangeRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.io.File;

@Slf4j
public class SchemaChangeProcessor {

    private static final String WORKING_DIR = "infomover.schema.change.working.dir";

    private String workingDir;

    @Inject
    SchemaChangeRepository schemaChangeRepository;

    @Inject
    JobRepository jobRepository;

    @Inject
    ConnectorRepository connectorRepository;

    public SchemaChangeProcessor() {
        String workingDir = System.getProperty(WORKING_DIR);
        if (StringUtils.isBlank(workingDir)) {
            throw new RuntimeException("Schema change monitor's working directory must be set. " +
                    "Property is " + WORKING_DIR);
        } else {
            File file = new File(workingDir);
            if (!file.exists()) {
                file.mkdirs();
            }
        }
        this.workingDir = workingDir;
    }

    /*public synchronized void onSchemaChange(SchemaChangeEvent event) {
        *//*
         * 发现schema change调用liquibase脚本比对数据库，生成新的比对文件
         * *//*
        Source source = event.getSource();
        String businessKey = source.getBusinessKey();
        String connectorName = businessKey;
        String databaseName = event.getDatabaseName();
        String schemaName = event.getSchemaName();
        String table = source.getTableKey();
        if (StringUtils.isBlank(connectorName)) {
            log.warn("Connector name is empty.");
            return;
        }
        if (StringUtils.isBlank(table)) {
            log.warn("Table name is empty.");
            return;
        }
        String tableWithCatalog = StringUtils.isBlank(schemaName) ?
                databaseName.concat(".").concat(table) :
                schemaName.concat(".").concat(table);
        log.info("Found connector {} schema changed on table {}", connectorName, tableWithCatalog);
        Connector sourceConnector = connectorRepository.findConnectorByName(connectorName);
        if (sourceConnector == null) {
            log.warn("Cannot found connector {}", connectorName);
            return;
        }
        Long jobId = sourceConnector.jobId;
        String latestSnapshotFile = sourceConnector.getLatestSnapshotFile();
        String sourceConnectorType = sourceConnector.connectorType;
        Map<String, String> configs = sourceConnector.config;
        String sourceUser = configs.get(SOURCE_USER);
        String sourcePassword = configs.get(SOURCE_PASSWD);
        String sourceTableWithSchema = configs.get(SOURCE_TABLE);
        if (!sourceTableWithSchema.equals(tableWithCatalog)) {
            log.warn("Skip schema change message, because of the table {} is not in monitoring sequence [{}]",
                    tableWithCatalog,
                    sourceTableWithSchema);
            return;
        }
        String sourceUrl = SchemaChangeUtil.generateJDBCUrl(sourceConnectorType, configs);

        Job job = jobRepository.findJobByIdAndLoadConnector(jobId);
        Job.SnapshotStatus snapshotStatus = job.snapshotStatus;
        if (snapshotStatus != Job.SnapshotStatus.COMPLETED) {
            log.info("Skip schema change diff, " +
                    "because of the snapshot status is {} for job: {}", snapshotStatus, jobId);
            return;
        }
        List<Connector> sinkConnectors = job.findSinkConnectors();
        Connector sinkConnector = sinkConnectors.get(0);
        Map<String, String> sinkConnectConfigs = sinkConnector.config;
        String sinkUrl = sinkConnectConfigs.get(SINK_URL);
        String sinkTable = sinkConnectConfigs.get(SinkConnectorKeyword.SINK_TABLE);

        *//*
         * 1. diff schema
         * *//*
        String sinkConnectorType;
        if ("kafka".equals(sinkConnector.connectorType)) {
            sinkConnectorType = SchemaChangeUtil.identifyJDBCUrl(sourceUrl);
        } else {
            sinkConnectorType = SchemaChangeUtil.identifyJDBCUrl(sinkUrl);
        }
        int lastIndex = sourceTableWithSchema.lastIndexOf(".");
        String sourceTable = sourceTableWithSchema.substring(lastIndex + 1);

        if (StringUtils.isBlank(latestSnapshotFile)) {
            log.warn("The snapshot file is not exist for connector {}.", connectorName);
            return;
        }
        File snapshotFile = new File(workingDir + File.separator + latestSnapshotFile);
        if (!snapshotFile.exists()) {
            throw new IllegalArgumentException("Snapshot file does not exist, connector is " + connectorName);
        }

        String offlineUrl = generateOfflineUrl(sourceConnectorType, latestSnapshotFile);
        String changeLogOutFileName = SchemaChangeUtil.generateChangeLogOutFileName(connectorName, sinkConnectorType);
        int existCode = LiquibaseRunner.diffChangeLog(changeLogOutFileName, sourceUrl, sourceUser,
                sourcePassword, sourceTable, offlineUrl, workingDir);
        log.info("Different database change exist code {}.", existCode);

        //TODO filter empty changelog sql
        filerEmptyChangelog(workingDir, changeLogOutFileName);

        if (existCode == 0) {
            *//*
             * 2. show diff sql
             * *//*
            schemaChangeRepository.saveSchemaChange(jobId, sourceConnector, sourceTable, sinkConnector, sinkTable,
                    workingDir, changeLogOutFileName);

            if ("kafka".equals(sinkConnector.connectorType)) {
                log.info("Skip schema change diff, " +
                        "because of the sink connector is kafka for job: {}", jobId);
                return;
            }

            *//*
             * 3. pause connector
             * *//*
            try {
                KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(job.getDeployClusterId().intValue());
                Response response = kafkaConnectClient.pauseConnector(sinkConnector.name);
                log.info("pause connector {} response: {}.", sinkConnector.name, response);
            } catch (WebApplicationException | IOException e) {
                throw new RuntimeException("pause connector " + sinkConnector.name + " failed,error message: " + e.getMessage());
            } catch (KafkaConnectException e) {
                throw new RuntimeException("get KafkaConnectClient error: " + e.getMessage());
            }
            log.info("Schema change on database {} and pause connector {}.", databaseName, sinkConnector.name);
        } else {
            log.warn("Can't different database change.");
        }
    }*/

    private void filerEmptyChangelog(String workingDir, String changeLogOutFileName) {

    }

    public String getWorkingDir() {
        return workingDir;
    }

    //@NotNull
    private String generateOfflineUrl(String connectorType, String snapshotFile) {
        String snapshotFileName = snapshotFile;
        if (snapshotFile.contains(File.separator)) {
            int index = snapshotFile.lastIndexOf(File.separator);
            snapshotFileName = snapshotFile.substring(index + 1);
        }
        String offlineUrl = String.format("offline:%s?snapshot=%s", connectorType, snapshotFileName);
        return offlineUrl;
    }
}
