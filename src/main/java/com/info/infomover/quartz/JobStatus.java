package com.info.infomover.quartz;

import com.info.baymax.common.utils.DataBaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class JobStatus {

    private static Logger logger = LoggerFactory.getLogger(JobStatus.class);


    @Value("${spring.datasource.username}")
    private String datasourceUserName;
    @Value("${spring.datasource.password}")
    private String datasourceUserPassword;
    @Value("${spring.datasource.url}")
    private String datasourceUrl;

    private String driver = "com.mysql.jdbc.Driver";


    @PostConstruct
    public void updateJobStatus() {

        try {
            Connection connection = DataBaseUtil.getConnection(driver, datasourceUrl,
                    datasourceUserName, datasourceUserPassword, null, null);
            Statement statement = connection.createStatement();
            statement.execute("update infomover_job set recollectStatus = 'RECOLLECTFAILED' where recollectStatus = 'RECOLLECTING';");
            statement.execute("update infomover_job set recoveryStatus = 'RECOVERYFAILED' where recoveryStatus = 'RECOVERYING';");
            statement.close();
            connection.close();
            logger.info("update job recoveryStatus & recollectStatus finish");
        } catch (SQLException e) {
            logger.warn("update job recoveryStatus & recollectStatus error {}", e.getMessage());
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
