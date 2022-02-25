package com.info.infomover.schemachange.liquibase;

import com.info.infomover.schemachange.monitor.ConsoleProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;


@Slf4j
public class LiquibaseRunner {

    private static final String ACTION_LIQUIBASE = "liquibase";
    private static final String ACTION_GENERATE_CHANGELOG = "generateChangeLog";
    private static final String ACTION_SNAPSHOT = "snapshot";
    private static final String ACTION_UPDATE = "update";
    private static final String ACTION_DIFF_CHANGELOG = "diffChangeLog";

    private static final String PARAM_URL = "--url";
    private static final String PARAM_USERNAME = "--username";
    private static final String PARAM_PASSWORD = "--password";
    private static final String PARAM_REFERENCE_URL = "--referenceUrl";
    private static final String PARAM_REFERENCE_USERNAME = "--referenceUsername";
    private static final String PARAM_REFERENCE_PASSWORD = "--referencePassword";
    private static final String PARAM_INCLUDE_OBJECTS = "--includeObjects";
    private static final String PARAM_SNAPSHOT_FORMAT = "--snapshotFormat";

    private static final String PARAM_CHANGELOG_FILE = "--changeLogFile";
    private static final String PARAM_OUTPUT_FILE = "--outputFile";

    private static final String PARAM_DELIMITER = "=";

    /**
     * <p>生成初始changeLog</p>
     *
     * <code>
     * liquibase generateChangeLog \
     * --changeLogFile=test.postgresql.sql \
     * --includeObjects="table:url_click" \
     * --url=jdbc:mysql://192.168.1.17:3306/test \
     * --username=root \
     * --password=123456
     * </code>
     */
    public static int generateChangeLog(String outputFile,
                                        String url,
                                        String username,
                                        String password,
                                        String table,
                                        String workingDir) throws IOException {
        ArrayList<Object> cmd = new ArrayList<>();
        cmd.add(getLiquibaseAction());
        cmd.add(ACTION_GENERATE_CHANGELOG);

        cmd.add(PARAM_CHANGELOG_FILE.concat(PARAM_DELIMITER).concat(outputFile));
        cmd.add(PARAM_INCLUDE_OBJECTS.concat(PARAM_DELIMITER).concat(table));
        cmd.add(PARAM_URL.concat(PARAM_DELIMITER).concat(url));
        cmd.add(PARAM_USERNAME.concat(PARAM_DELIMITER).concat(username));
        cmd.add(PARAM_PASSWORD.concat(PARAM_DELIMITER).concat(password));

        return runCmd(cmd, workingDir);
    }

    /**
     * <p>对数据库做快照</p>
     * <p>
     * liquibase snapshot \
     * --includeObjects="table:url_click" \
     * --outputFile=mysql_snapshot2.json \
     * --url=jdbc:mysql://192.168.1.17:3306/test \
     * --username=root \
     * --password=123456 \
     * --snapshotFormat=json
     */
    public static int snapshot(String outputFile,
                               String url,
                               String username,
                               String password,
                               String workingDir) {
        ArrayList<Object> cmd = new ArrayList<>();
        cmd.add(getLiquibaseAction());
        cmd.add(ACTION_SNAPSHOT);
        cmd.add(PARAM_OUTPUT_FILE.concat(PARAM_DELIMITER).concat(outputFile));
        cmd.add(PARAM_URL.concat(PARAM_DELIMITER).concat(url));
        cmd.add(PARAM_USERNAME.concat(PARAM_DELIMITER).concat(username));
        cmd.add(PARAM_PASSWORD.concat(PARAM_DELIMITER).concat(password));
        cmd.add(PARAM_SNAPSHOT_FORMAT.concat(PARAM_DELIMITER).concat("json"));

        return runCmd(cmd, workingDir);
    }

    /**
     * <p>更新表结构</p>
     * <p>
     * liquibase update \
     * --changelog-file test.postgresql.sql \
     * --url=jdbc:postgresql://192.168.1.17:5432/test \
     * --username=debezium_cdc \
     * --password=123456
     */
    public static int update(String outputFile,
                             String url,
                             String username,
                             String password,
                             String snapshotDir) {
        ArrayList<Object> cmd = new ArrayList<>();
        cmd.add(getLiquibaseAction());
        cmd.add(ACTION_UPDATE);

        cmd.add(PARAM_CHANGELOG_FILE.concat(PARAM_DELIMITER).concat(outputFile));
        cmd.add(PARAM_URL.concat(PARAM_DELIMITER).concat(url));
        cmd.add(PARAM_USERNAME.concat(PARAM_DELIMITER).concat(username));
        cmd.add(PARAM_PASSWORD.concat(PARAM_DELIMITER).concat(password));

        return runCmd(cmd, snapshotDir);
    }


    /**
     * <p>对比表结构并生成changeLog文件</p>
     * <p>
     * liquibase diffChangeLog \
     * --changeLogFile=diff1.postgresql.sql \
     * --referenceUrl=jdbc:mysql://192.168.1.17:3306/test \
     * --referenceUsername=root \
     * --referencePassword=123456 \
     * --url=offline:mysql?snapshot=mysql_snapshot1.json
     */
    public static int diffChangeLog(String changeLogFile,
                                    String url,
                                    String username,
                                    String password,
                                    String table,
                                    String snapshotFileUrl,
                                    String snapshotDir) {
        ArrayList<Object> cmd = new ArrayList<>();
        cmd.add(getLiquibaseAction());
        cmd.add(ACTION_DIFF_CHANGELOG);

        cmd.add(PARAM_CHANGELOG_FILE.concat(PARAM_DELIMITER).concat(changeLogFile));
        cmd.add(PARAM_REFERENCE_URL.concat(PARAM_DELIMITER).concat(url));
        cmd.add(PARAM_REFERENCE_USERNAME.concat(PARAM_DELIMITER).concat(username));
        cmd.add(PARAM_REFERENCE_PASSWORD.concat(PARAM_DELIMITER).concat(password));
        cmd.add(PARAM_URL.concat(PARAM_DELIMITER).concat(snapshotFileUrl));
        cmd.add(PARAM_INCLUDE_OBJECTS.concat(PARAM_DELIMITER).concat(table));

        return runCmd(cmd, snapshotDir);
    }

    /**
     * 指定目录下执行命令行
     *
     * @param cmd 待执行的命令
     * @return 状态码 0代表正常执行完成
     */
    private static int runCmd(ArrayList<Object> cmd, String commandDirectory) {
        if (StringUtils.isBlank(commandDirectory)) {
            throw new IllegalArgumentException("Command directory is empty.");
        }
        File directory = new File(commandDirectory);
        if (!directory.exists()) {
            throw new IllegalArgumentException("Command directory " + commandDirectory + " is not exist.");
        }
        String[] command = cmd.toArray(new String[cmd.size()]);
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        log.info("Directory to {}", commandDirectory);
        pb.directory(directory);
        int exitCode = -1;
        try {
            Process process = pb.start();
            Runnable consoleRunnable = new ConsoleProcessor(process);
            Thread consoleThread = new Thread(consoleRunnable);
            consoleThread.start();
            consoleThread.join();
            exitCode = process.waitFor();
        } catch (Throwable t) {
            log.error("Execute command {} throw exception",
                    StringUtils.join(command, " "), t);
        }
        log.info("Exit code {} for execution {}", exitCode, StringUtils.join(command, " "));
        return exitCode;
    }
    
    private static String getLiquibaseAction() {
        //String liquibaseHome = System.getProperty(LIQUIBASE_HOME);
        //return liquibaseHome.concat(File.separator).concat(ACTION_LIQUIBASE);
        return "";
    }

}
