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

@DataSource(type = "postgres")
@Getter
@Setter
@DataSourceScope(value = {DataSourceScopeType.All})
public class PostgresqlDataSource extends AbstractJDBCDataSource {


    public com.info.infomover.entity.DataSource.DataSourceCategory dataSourcecategory;

    public PostgresqlDataSource() {
        super("postgres");
    }

    @Setting(description = "端口", defaultValue = "5432")
    @JsonAlias("database.port")
    private String port;

    @Setting(description = "驱动", defaultValue = "org.postgresql.Driver", values = {"org.postgresql.Driver"}, required = true, advanced = true)
    @JsonAlias({"database.jdbc.driver"})
    private String driver;

    /*@Setting(description = "数据库名称")
    @JsonAlias("database.dbname")
    private String database;*/

    @Setting(description = "版本", defaultValue = "9.6.x", values = {"9.6.x", "10.x", "11.x", "12.x", "13.x", "14.x"})
    @JsonAlias("database.jdbc.version")
    private String version;

    @Override
    public String generateJDBCUrl() {
        return String.format("jdbc:postgresql://%s:%s/%s?stringtype=unspecified", hostname, port, database);
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

            types.add(DataSourceVerification.TYPE.CONNECTION);
            types.add(DataSourceVerification.TYPE.USERNAME_PASSWORD);

            if (isSink) {
                return DataSourceVerification.builder().isSink(isSink).build().updateIsSuccess(this.getType(),types.toArray(new DataSourceVerification.TYPE[]{}));
            }

            statement = connection.createStatement();
            if(this.compareVersion(statement, resultSet, "SELECT VERSION() AS VER", "VER", getVersion().substring(0,3))) {
                types.add(DataSourceVerification.TYPE.VERSION);
            }
            this.listDatabase();
            types.add(DataSourceVerification.TYPE.MODEL);

            if (this.executeEqual(statement, resultSet, "select name,setting from pg_settings where name  = 'wal_level'", "setting", "logical")) {
                types.add(DataSourceVerification.TYPE.LOGICAL);
            }

        } catch (SQLException | ClassNotFoundException e) {
            return DataSourceVerification.builder().isSink(isSink).build().updateIsSuccess(this.getType(),types.toArray(new DataSourceVerification.TYPE[]{}));
        }finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException sqlException) {
            }
        }
        return DataSourceVerification.builder().isSink(isSink).build().updateIsSuccess(this.getType(),types.toArray(new DataSourceVerification.TYPE[]{}));
    }
    static {
        sysTable.add("information_schema");
        sysTable.add("pg_catalog");
        sysTable.add("public$");
    }
}
