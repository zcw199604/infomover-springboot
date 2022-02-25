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

@DataSource(type = "tidb")
@Getter
@Setter
@DataSourceScope(value = {DataSourceScopeType.Sink})
public class TidbDataSource extends AbstractJDBCDataSource{

    public TidbDataSource() {
        super("tidb");
    }

    public com.info.infomover.entity.DataSource.DataSourceCategory dataSourcecategory;

    @Setting(description = "端口", defaultValue = "4000")
    @JsonAlias("database.port")
    private String port;

    @Setting(description = "驱动", defaultValue = "com.mysql.jdbc.Driver", values = {"com.mysql.jdbc.Driver"}, required = true, advanced = true)
    @JsonAlias({"database.jdbc.driver"})
    private String driver;

    @Setting(description = "版本", defaultValue = "4.0.10", values = {"4.0.10"})
    @JsonAlias("database.jdbc.version")
    private String version;

    @Setting(description = "数据库时区", defaultValue = "UTC", values = {"UTC", "GMT+8"})
    @JsonAlias("database.connectionTimeZone")
    private String dbZone;

    @Override
    public String generateJDBCUrl() {
        return String.format("jdbc:mysql://%s:%s/%s", hostname, port, database);
    }


    @Override
    public List<DataSourceVerification.DataSourceVerificationDetail> validaConnection() {

        List<DataSourceVerification.TYPE> types = new ArrayList<>();
        Statement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        // TiDB目前只作为Sink
        boolean isSink = true;
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
            if(getVersion() == null || this.compareVersion(statement, resultSet, "SELECT VERSION() AS VER", "VER", getVersion())) {
                types.add(DataSourceVerification.TYPE.VERSION);
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
        sysTable.add("INFORMATION_SCHEMA");
        sysTable.add("METRICS_SCHEMA");
        sysTable.add("mysql");
        sysTable.add("PERFORMANCE_SCHEMA");
    }
}
