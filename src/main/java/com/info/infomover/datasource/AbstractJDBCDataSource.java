package com.info.infomover.datasource;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.baymax.common.utils.DataBaseUtil;
import com.info.infomover.common.setting.Setting;
import com.info.infomover.crawler.CrawlerUtils;
import com.info.infomover.util.AesUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.schema.Catalog;
import schemacrawler.schema.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@Getter
@Setter
public abstract class AbstractJDBCDataSource extends AbstractDataSource {
    private static Logger logger = LoggerFactory.getLogger(AbstractJDBCDataSource.class);

    public AbstractJDBCDataSource(String type) {
        super(type);
    }

    @Setting(description = "主机名或IP地址")
    @JsonAlias("database.hostname")
    protected String hostname;

    @Setting(description = "用户名")
    @JsonAlias("database.user")
    protected String user;

    @Setting(description = "密码", format = "password", required = false)
    @JsonAlias({"database.password"})
    protected String password;

    @Setting(description = "数据库名称")
    @JsonAlias("database.dbname")
    protected String database;

    @Setting(description = "是否过滤系统schema", defaultValue = "true", values = {"true", "false"})
    @JsonAlias("database.filtersysschemas")
    protected boolean filterSysSchemas;

    /* ------------------------  SSL Configurations -------------------------------*/
    /*

    @Setting(description = "加密方式", values = {"disabled", "preferred", "required", "verify_ca", "verify_identity"}, advanced = true, required = false)
    @JsonAlias("database.ssl.mode")
    protected String sslMode;

    @Setting(description = "keystore文件路径", advanced = true, required = false)
    @JsonAlias("database.ssl.keystore")
    protected String sslKeystore;

    @Setting(description = "keystore文件密码", format = "password", advanced = true, required = false)
    @JsonAlias("database.ssl.keystore.password")
    protected String sslKeystorePassword;

    @Setting(description = "truststore文件路径", advanced = true, required = false)
    @JsonAlias("database.ssl.truststore")
    protected String sslTruststore;

    @Setting(description = "truststore文件密码", format = "password", advanced = true, required = false)
    @JsonAlias("database.ssl.truststore.password")
    private String sslTruststorePassword;
    */

    private int category;

    protected static HashSet<String> sysTable = new HashSet<>();

    private Properties properties = new Properties();

    public abstract String getPort();

    public abstract String getDriver();

    @Override
    public boolean isJDBCSource() {
        return true;
    }

    @Override
    public List<DataSourceVerification.DataSourceVerificationDetail> validaConnection() {
        try {
            Properties properties = new Properties();
            DataBaseUtil.connect(
                    getDriver(),
                    generateJDBCUrl(),
                    getUser(),
                    AesUtils.wrappeDaesDecrypt(getPassword()),
                    properties,
                    null);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to connect databasead", e);
        }
        return new ArrayList<>();
    }

    @Override
    public List<String> listDatabase() {
        try {
            Connection connection = DataBaseUtil.getConnection(getDriver(), generateJDBCUrl(),
                    user, AesUtils.wrappeDaesDecrypt(getPassword()), getProperties(), null);
            Catalog catalog = CrawlerUtils.crawlCatalogOnlySchemas(connection, null);
            List<String> databaseList = catalog.getSchemas()
                    .stream()
                    // step to connect 时 处理 schema 特殊字符
                    .map(schema -> schema.getFullName())
                    .collect(Collectors.toList());
            if (this.filterSysSchemas) {
                return databaseList.stream()
                        .filter(schema -> !sysTable.contains(schema)).collect(Collectors.toList());
            }
            return databaseList;
        } catch (Exception e) {
            throw new RuntimeException("Fetch database failed.", e);
        }
    }

    private String escape(String string) {
        if (StringUtils.isBlank(string)) {
            return string;
        }
        StringBuilder stringBuilder = new StringBuilder();
        char[] chars = string.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char aChar = chars[i];
            if (aChar == '$' || aChar == '(' || aChar == ')'|| aChar == '*'|| aChar == '+'|| aChar == '.'|| aChar == '['|| aChar == '?'
                    || aChar == '\\'
                    || aChar == '^'
                    || aChar == '{'
                    || aChar == '|') {
                stringBuilder.append("\\");
            }
            stringBuilder.append(aChar);
        }
        return stringBuilder.toString();
    }

    @Override
    public List<String> listTable(String schema) {
        try {
            Connection connection = DataBaseUtil.getConnection(getDriver(), generateJDBCUrl(),
                    user, AesUtils.wrappeDaesDecrypt(getPassword()), getProperties(), null);
            String parse = escape(schema);
            Catalog catalog = CrawlerUtils.crawlCatalogOnlySchemasAndTables(connection, parse);
            List<String> tableList = catalog.getTables()
                    .stream()
                    // step to connect 时 处理 schema 特殊字符
                    .map(table -> table.getName())
                    .collect(Collectors.toList());
            return tableList;
        } catch (Exception e) {
            throw new RuntimeException("Fetch table failed for database " + schema, e);
        }
    }

    @Override
    public List<String> listColumn(String schema, String table) {
        try {
            String driver = getDriver();
            String jdbcUrl = generateJDBCUrl();
            Connection connection = DataBaseUtil.getConnection(
                    driver, jdbcUrl,
                    user, AesUtils.wrappeDaesDecrypt(getPassword()),
                    properties, null);
            schema = escape(schema);
            Catalog catalog = CrawlerUtils.crawlCatalog(
                    connection,
                    schema, schema.concat(".").concat(table));
            Collection<Table> tables = catalog.getTables();
            if (tables.size() > 0) {
                Table[] tablesArray = tables.toArray(new Table[]{});
                Table dTable = tablesArray[0];
                List<String> columnList = dTable.getColumns()
                        .stream()
                        .flatMap(column ->
                                Arrays.asList(
                                                column.getName(),
                                                column.getType().getName())
                                        .stream()
                        ).collect(Collectors.toList());
                if (columnList.isEmpty()) {
                    return Collections.EMPTY_LIST;
                } else {
                    return columnList;
                }
            }
            return Collections.EMPTY_LIST;
        } catch (Exception e) {
            throw new RuntimeException("Fetch column failed for database " + schema, e);
        }
    }


    public static boolean executeEqual(Statement statement, ResultSet resultSet, String sql, String label, String value) {
        try {
            statement.execute(sql);
            resultSet = statement.getResultSet();
            while (resultSet.next()) {
                String string = resultSet.getString(label);
                return value.equals(string);
            }
        } catch (Exception e) {
            return false;
        } finally {

        }
        return false;
    }

    public boolean compareVersion(Statement statement, ResultSet resultSet, String sql, String label, String versionPrefix) {
        try {
            statement.execute(sql);
            resultSet = statement.getResultSet();
            while (resultSet.next()) {
                String value = resultSet.getString(label);
                return value.contains(versionPrefix);
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    /**
     * 1 清理成功
     * 0. table不存在
     * -1 失败
     */
    @Override
    public int clearRecords(String table) {
        Statement statement = null;
        Connection connection = null;
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
            String sql = null;
            if (this.getType().equals("postgres")) {
                sql = "truncate table \"" + table + "\"";
            } else {
                sql = "truncate table " + table;
            }
            boolean res = statement.execute(sql);//清除成功后返回为false
            return !res ? 1 : -1;
        }catch (Exception e){
            logger.error("truncate table error {}", table, e);//不存在的报table ''  doesn't exist
            if (e.getMessage().contains("doesn't exist") ||
                    // oracle 表或视图不存在
                    e.getMessage().contains("ORA-00942")
                    // pg
                    || e.getMessage().contains("关系 \"" + table + "\" 不存在")) {
                return 0;
            }
            return -1;
        }finally {
            try {
                if(statement != null){
                    statement.close();
                }
                if(connection!=null){
                    connection.close();
                }
            }catch (Exception e){
            }

        }
    }

}
