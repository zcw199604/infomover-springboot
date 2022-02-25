package com.info.infomover.util;

import java.io.File;
import java.util.Map;

import static com.info.infomover.util.SourceConnectorKeyword.*;

public class SchemaChangeUtil {


    public static String generateSnapshotOutFileName(String connectorName) {
        return connectorName + "_snapshot_" + System.currentTimeMillis() + ".json";
    }

    /**
     *
     * @param sourceConnectorType
     * @param configs
     * @return
     */
    public static String generateJDBCUrl(String sourceConnectorType, Map<String, String> configs) {
        String hostname = configs.get(HOSTNAME);
        String port = configs.get(PORT);
        String database = configs.get(DATABASES);
        String sourceUrl;
        switch (sourceConnectorType) {
            case "mysql":
                sourceUrl = String.format("jdbc:mysql://%s:%s/%s", hostname, port, database);
                break;
            case "postgres":
                sourceUrl = String.format("jdbc:postgresql://%s:%s/%s", hostname, port, database);
                break;
            case "oracle":
                String databaseOracle = configs.get(DATABASES_4_ORACLE);
                sourceUrl = String.format("jdbc:oracle:thin:@//%s:%s/%s", hostname, port, databaseOracle);
                break;
            default:
                throw new IllegalArgumentException("Only supported mysql,postgres and oracle types. " +
                        "But source connector type is " + sourceConnectorType);
        }
        return sourceUrl;
    }

    public static String identifyJDBCUrl(String url) {
        if (url.startsWith("jdbc:mysql:")) {
            return "mysql";
        } else if (url.startsWith("jdbc:postgresql:")) {
            return "postgresql";
        } else if (url.startsWith("jdbc:oracle:")) {
            return "oracle";
        } else {
            throw new IllegalArgumentException("Unknown jdbc type, jdbc url is " + url);
        }
    }

    public static String identifyJDBCDriver(String url) {
        if (url.startsWith("jdbc:mysql:")) {
            return "com.mysql.jdbc.Driver";
        } else if (url.startsWith("jdbc:postgresql:")) {
            return "org.postgresql.Driver";
        } else if (url.startsWith("jdbc:oracle:")) {
            return "oracle.jdbc.driver.OracleDriver";
        } else {
            throw new IllegalArgumentException("Unknown jdbc type, jdbc url is " + url);
        }
    }

    public static String generateChangeLogOutFileName(String connectorName, String targetDBType) {
        return connectorName + "_changeLog_" + System.currentTimeMillis() + "." + targetDBType + ".sql";
    }


    /**
     * 替换sink table的名称,因为source和sink的表名称可能不一致
     * @param workingDir
     * @param changeLogDiffFile
     * @param sourceTable
     * @param sinkTable
     */
    public static void replaceTableName(String workingDir, String changeLogDiffFile, String sourceTable, String sinkTable) {
        String sqlFile = workingDir + File.separator + changeLogDiffFile;
        File file = new File(sqlFile);
        if (file.exists()) {
            String sqlContent = FileUtil.load(sqlFile);
            String sqlReplaced = sqlContent.replace("TABLE " + sourceTable, "TABLE " + sinkTable);
            file.delete();
            FileUtil.save(sqlFile, sqlReplaced);
        } else {
            throw new RuntimeException("The sql file is not exist: " + sqlFile);
        }
    }


}
