package com.info.infomover.schemachange.liquibase;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;

@Slf4j
public class LiquibaseSnapshotLauncher //implements Runnable
{

    /*private Job job;

    private String workingDir;

    private final JobRepository jobRepository;

    private final ConnectorRepository connectorRepository;

    public LiquibaseSnapshotLauncher(Job job, String workingDir,
                                     JobRepository jobRepository,
                                     ConnectorRepository connectorRepository) {
        this.job = job;
        this.workingDir = workingDir;
        this.jobRepository = jobRepository;
        this.connectorRepository = connectorRepository;
    }

    @Override
    public void run() {
        log.info("Starting to snapshot......");
        try {
            *//**
            * 激活repository, 否则会抛出 `javax.enterprise.context.ContextNotActiveException`
            * 由于当前线程不受容器管理,所以需要此步
            * *//*
            Arc.container().requestContext().activate();

            List<ActionStatus> actionStatuses = new ArrayList<>();
            for (Connector con : job.findSourceConnectors()) {
                doSnapshot(con, actionStatuses);
            }
            Connector sourceConnector = job.findSourceConnectors().get(0);// 只能有一个source
            List<Connector> sinkConnectors = job.findSinkConnectors();
            if (sinkConnectors != null && !sinkConnectors.isEmpty()) {
                for (Connector connector : sinkConnectors) {
                    if (!"kafka".equals(connector.connectorType)) {
                        try {
                            createSinkTableByLiquibase(sourceConnector, connector, actionStatuses);
                        } catch (IOException | SQLException | ClassNotFoundException e) {
                            log.error("create connector {} failed,error message: {}", connector.name, e.getMessage());
                            throw new RuntimeException("create connector: " + connector.name + " failed, " + e.getMessage());
                        }
                    }
                }
            }
            jobRepository.saveJobSnapshot(job.id, Job.SnapshotStatus.COMPLETED);
            log.info("Snapshot successfully.");
        } catch (Throwable t) {
            jobRepository.saveJobSnapshot(job.id, Job.SnapshotStatus.FAILED);
            log.error("Snapshot failed.", t);
        }
    }

    *//**
     * 验证Sink Table是否存在,不存在则用Liquibase生成
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     *//*
    private void createSinkTableByLiquibase(Connector sourceConnector, Connector sinkConnector, List<ActionStatus> actionStatuses)
            throws SQLException, ClassNotFoundException, IOException {
        *//* source connector configuration *//*
        String sourceConnType = sourceConnector.connectorType;
        Map<String, String> sourceConfigs = sourceConnector.config;
        String sourceUrl = SchemaChangeUtil.generateJDBCUrl(sourceConnType, sourceConfigs);
        String sourceUser = sourceConfigs.get(SOURCE_USER);
        String sourcePasswd = sourceConfigs.get(SOURCE_PASSWD);
        String sourceTableWithSchema = sourceConfigs.get(SOURCE_TABLE);
        int lastIndex = sourceTableWithSchema.lastIndexOf(".");
        String sourceTable = sourceTableWithSchema.substring(lastIndex + 1);

        *//* sink connector configuration *//*
        Map<String, String> sinkConfigs = sinkConnector.config;
        String sinkUrl = sinkConfigs.get(SinkConnectorKeyword.SINK_URL);
        String sinkUser = sinkConfigs.get(SinkConnectorKeyword.SINK_USER);
        String sinkPasswd = sinkConfigs.get(SinkConnectorKeyword.SINK_PASSWD);
        String sinkTable = sinkConfigs.get(SinkConnectorKeyword.SINK_TABLE);

        *//* init sink table *//*
        String driver = SchemaChangeUtil.identifyJDBCDriver(sinkUrl);
        Connection connection = DataBaseUtil.getConnection(
                driver, sinkUrl, sinkUser, sinkPasswd, new Properties(), null);
        boolean tableExist = DataBaseUtil.tableExist(connection, sinkTable);
        if (!tableExist) {
            String sinkConnectorType = SchemaChangeUtil.identifyJDBCUrl(sinkUrl);
            String changeLogOutFileName = SchemaChangeUtil.generateChangeLogOutFileName(
                    sourceConnector.name, sinkConnectorType);
            *//* generate changelog from source table *//*
            int init = LiquibaseRunner.generateChangeLog(changeLogOutFileName, sourceUrl,
                    sourceUser, sourcePasswd, sourceTable, workingDir);
            if (init == 0) {
                actionStatuses.add(new ActionStatus("init", "completed"));
                *//* replace sink table name *//*
                SchemaChangeUtil.replaceTableName(workingDir, changeLogOutFileName, sourceTable, sinkTable);
                *//* init sink table from changelog*//*
                int update = LiquibaseRunner.update(changeLogOutFileName, sinkUrl, sinkUser, sinkPasswd, workingDir);
                if (update == 0) {
                    actionStatuses.add(new ActionStatus("update", "completed"));
                } else {
                    actionStatuses.add(new ActionStatus("update", "failed"));
                }
            } else {
                actionStatuses.add(new ActionStatus("init", "failed"));
            }
        } else {
            actionStatuses.add(new ActionStatus("init", "completed"));
        }
    }

    *//**
     * 快照
     *
     * @return
     *//*
    private int doSnapshot(Connector connector, List<ActionStatus> actionStatuses) throws Exception {
        *//*
         * snapshot
         * *//*
        String name = connector.name;
        Map<String, String> configs = connector.config;
        String sourceConnectorType = connector.connectorType;
        String snapshotOutFileName = SchemaChangeUtil.generateSnapshotOutFileName(name);
        String jdbcUrl = SchemaChangeUtil.generateJDBCUrl(sourceConnectorType, configs);
        String sourceUser = configs.get(SOURCE_USER);
        String sourcePassword = configs.get(SOURCE_PASSWD);
        int snapshot = LiquibaseRunner.snapshot(snapshotOutFileName, jdbcUrl, sourceUser, sourcePassword, workingDir);
        if (snapshot == 0) {
            connectorRepository.saveConnectorLatest(connector.id, workingDir, snapshotOutFileName);
            actionStatuses.add(new ActionStatus("snapshot", "completed"));
        } else {
            actionStatuses.add(new ActionStatus("snapshot", "failed"));
        }
        return snapshot;
    }*/
}
