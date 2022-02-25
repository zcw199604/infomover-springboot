package com.info.infomover.datasource;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.baymax.common.utils.DataBaseUtil;
import com.info.infomover.common.setting.DataSource;
import com.info.infomover.common.setting.Setting;
import com.info.infomover.util.AesUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@DataSource(type = "oracle")
@Getter
@Setter
@DataSourceScope(value = {DataSourceScopeType.All})
public class OracleDataSource extends AbstractJDBCDataSource{


    public com.info.infomover.entity.DataSource.DataSourceCategory dataSourcecategory;

    public OracleDataSource() {
        super("oracle");
    }

    @Setting(description = "端口", defaultValue = "1521")
    @JsonAlias("database.port")
    private String port;

    @Setting(description = "驱动", defaultValue = "oracle.jdbc.driver.OracleDriver", values = {"oracle.jdbc.driver.OracleDriver"}, required = true, advanced = true)
    @JsonAlias("database.jdbc.driver")
    private String driver;

    @Setting(description = "版本", defaultValue = "11g.x", values = {"11g.x", "12c.x", "19c.x"})
    @JsonAlias("database.jdbc.version")
    private String version;

    @Setting(description = "PDB名称(配合CDB使用)", required = false)
    @JsonAlias("database.pdb.name")
    private String pdb;

    @Override
    public String generateJDBCUrl() {
        if(StringUtils.isNotBlank(pdb)){
            return String.format("jdbc:oracle:thin:@//%s:%s/%s", hostname, port, pdb);
        }else {
            return String.format("jdbc:oracle:thin:@//%s:%s/%s", hostname, port, database);
        }
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

            this.listDatabase();
            types.add(DataSourceVerification.TYPE.MODEL);
            statement = connection.createStatement();
            if(this.compareVersion(statement, resultSet, "SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle Database%'", "BANNER", getVersion().substring(0,3))) {
                types.add(DataSourceVerification.TYPE.VERSION);
            }
            types.add(DataSourceVerification.TYPE.CDC);

            if (this.executeEqual(statement, resultSet, "select log_mode,force_logging from v$database", "LOG_MODE", "ARCHIVELOG")) {
                types.add(DataSourceVerification.TYPE.ARCHIVE);
            }

            /*if (this.executeEqual(statement, resultSet, "SELECT supplemental_log_data_all allc FROM v$database", "ALLC", "YES")) {
                types.add(DataSourceVerification.TYPE.SUPPLEMENTAL);
            }*/
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
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


    boolean executeEg(Statement statement, ResultSet resultSet, String sql, String label, int value) {
        try {
            statement.execute(sql);
            resultSet = statement.getResultSet();
            while (resultSet.next()) {
                int v = resultSet.getInt(label);
                return v > value;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    static {
        sysTable.add("SYS");
        sysTable.add("SYSTEM");
        sysTable.add("SYSMAN");
        sysTable.add("\"XS$NULL\"");
        sysTable.add("\"SYSTEM\"");
        sysTable.add("AUDSYS");
        sysTable.add("GSMUSER");
        sysTable.add("SPATIAL_WFS_ADMIN_USR");
        sysTable.add("SPATIAL_CSW_ADMIN_USR");
        sysTable.add("APEX_PUBLIC_USER");
        sysTable.add("SYSDG");
        sysTable.add("DIP");
        sysTable.add("SYSBACKUP");
        sysTable.add("MDDATA");
        sysTable.add("GSMCATUSER");
        sysTable.add("XS$NULL");
        sysTable.add("SYSKM");
        sysTable.add("OJVMSYS");
        sysTable.add("ORACLE_OCM");
        sysTable.add("OLAPSYS");
        sysTable.add("SI_INFORMTN_SCHEMA");
        sysTable.add("DVSYS");
        sysTable.add("ORDPLUGINS");
        sysTable.add("XDB");
        sysTable.add("ANONYMOUS");
        sysTable.add("CTXSYS");
        sysTable.add("ORDDATA");
        sysTable.add("GSMADMIN_INTERNAL");
        sysTable.add("APPQOSSYS");
        sysTable.add("APEX_040200");
        sysTable.add("WMSYS");
        sysTable.add("DBSNMP");
        sysTable.add("ORDSYS");
        sysTable.add("MDSYS");
        sysTable.add("DVF");
        sysTable.add("FLOWS_FILES");
        sysTable.add("OUTLN");
        sysTable.add("LBACSYS");
    }
}
