package com.info.infomover.datasource;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.baymax.common.utils.DataBaseUtil;
import com.info.infomover.common.setting.DataSource;
import com.info.infomover.common.setting.Setting;
import com.info.infomover.util.AesUtils;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@DataSource(type = "mysql")
@Getter
@Setter
@DataSourceScope(value = {DataSourceScopeType.All})
public class MysqlDataSource extends AbstractJDBCDataSource {

    public MysqlDataSource() {
        super("mysql");
    }

    public com.info.infomover.entity.DataSource.DataSourceCategory dataSourcecategory;

    @Setting(description = "端口", defaultValue = "3306")
    @JsonAlias("database.port")
    private String port;

    @Setting(description = "驱动", defaultValue = "com.mysql.jdbc.Driver", values = {"com.mysql.jdbc.Driver"}, required = true, advanced = true)
    @JsonAlias({"database.jdbc.driver"})
    private String driver;

    @Setting(description = "版本", defaultValue = "5.7.x", values = {"5.6.x", "5.7.x", "8.0.x"})
    @JsonAlias("database.jdbc.version")
    private String version;

    @Setting(description = "数据库时区", defaultValue = "UTC", values = {"UTC", "GMT+8"})
    @JsonAlias("database.connectionTimeZone")
    private String dbZone;

    @Override
    public String generateJDBCUrl() {
        if ("GMT+8".equals(dbZone)) {
            dbZone = dbZone.replace("+", "%2B");
        }
        return String.format("jdbc:mysql://%s:%s/%s?serverTimezone=%s", hostname, port, database, dbZone);
    }


    @Override
    public List<DataSourceVerification.DataSourceVerificationDetail> validaConnection() {

        List<DataSourceVerification.TYPE> types = new ArrayList<>();
        Statement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        boolean isSink = this.dataSourcecategory == com.info.infomover.entity.DataSource.DataSourceCategory.SINK;
        try {
            Properties properties = new Properties();
            connection = DataBaseUtil.getConnection(
                    getDriver(),
                    generateJDBCUrl(),
                    getUser(),
                    AesUtils.wrappeDaesDecrypt(getPassword()),
                    properties,
                    null);
            statement = connection.createStatement();
            types.add(DataSourceVerification.TYPE.CONNECTION);
            types.add(DataSourceVerification.TYPE.USERNAME_PASSWORD);
            if (isSink) {
                return DataSourceVerification.builder().isSink(isSink).build().updateIsSuccess(this.getType(),types.toArray(new DataSourceVerification.TYPE[]{}));
            }

            if(getVersion() == null || this.compareVersion(statement, resultSet, "SELECT VERSION() AS VER", "VER", getVersion().substring(0,3))) {
                types.add(DataSourceVerification.TYPE.VERSION);
            }
            this.listDatabase();
            types.add(DataSourceVerification.TYPE.MODEL);



            statement = connection.createStatement();
            resultSet = statement.getResultSet();
            statement = connection.createStatement();
            if (this.executeEqual(statement, resultSet, "show variables like 'log_bin'", "Value", "ON")
            && this.executeEqual(statement, resultSet, "show variables like '%binlog_format%'", "Value", "ROW")
            && this.executeEqual(statement, resultSet, "show variables like '%binlog_row_image%'", "Value", "FULL")) {
                types.add(DataSourceVerification.TYPE.CDC);
            }
        } catch (SQLException | ClassNotFoundException e) {
            //throw new RuntimeException("Failed to connect database", e);
            return DataSourceVerification.builder().isSink(isSink).build().updateIsSuccess(this.getType(),types.toArray(new DataSourceVerification.TYPE[]{}));
        }finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException sqlException) {
            }

        }
        return DataSourceVerification.builder().isSink(isSink).build().updateIsSuccess(this.getType(),types.toArray(new DataSourceVerification.TYPE[]{}));
    }
    static {
        sysTable.add("sys");
        sysTable.add("mysql");
        sysTable.add("information_schema");
        sysTable.add("performance_schema");
    }
}
