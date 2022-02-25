package com.info.infomover.datasource;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.*;
import java.util.stream.Collectors;

@Getter
@Setter
@Builder
public class DataSourceVerification {

    public enum TYPE {
        CONNECTION, USERNAME_PASSWORD, VERSION, MODEL, CDC, ARCHIVE, SUPPLEMENTAL,LOGICAL
    }

    private boolean isSink;


    @Builder
    @Getter
    @Setter
    @ToString
    public static class DataSourceVerificationDetail {
        private String displayName;

        private boolean success;

        private String description;
    }


    public static   Map<String, Map<TYPE, DataSourceVerificationDetail>> verificationList = new HashMap<>();
    public static  Map<String, Map<TYPE, DataSourceVerificationDetail>> sinkVerificationList = new HashMap<>();

    {
        Map<TYPE, DataSourceVerificationDetail> mysqlSource = new LinkedHashMap<>();
        Map<TYPE, DataSourceVerificationDetail> mysqlSink = new LinkedHashMap<>();

        mysqlSource.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        mysqlSource.put(TYPE.USERNAME_PASSWORD, DataSourceVerificationDetail.builder().displayName("检查用户名密码是否正确").success(false).description("请检查用户名或密码是否正确").build());
        mysqlSink.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        mysqlSink.put(TYPE.USERNAME_PASSWORD, DataSourceVerificationDetail.builder().displayName("检查用户名密码是否正确").success(false).description("请检查用户名或密码是否正确").build());


        //sinkVerificationList.put("mysql", mysqlSource.getOrDefault());

        mysqlSource.put(TYPE.VERSION, DataSourceVerificationDetail.builder().displayName("检查数据源版本信息是否可用").success(false).description("数据源版本与DB不匹配").build());
        mysqlSource.put(TYPE.MODEL, DataSourceVerificationDetail.builder().displayName("加载模型").success(false).description("请检查该用户是否有对应权限").build());
        mysqlSource.put(TYPE.CDC, DataSourceVerificationDetail.builder().displayName("检查CDC同步所需的权限是否授权").success(false).description("请检查binlog是否正确开启,binlog_format是否为ROW,binlog_row_image是否为FULL").build());

        sinkVerificationList.put("mysql", mysqlSink);
        verificationList.put("mysql", mysqlSource);

        Map<TYPE, DataSourceVerificationDetail> oracleSource = new LinkedHashMap<>();
        Map<TYPE, DataSourceVerificationDetail> oracleSink = new LinkedHashMap<>();
        oracleSource.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        oracleSource.put(TYPE.USERNAME_PASSWORD, DataSourceVerificationDetail.builder().displayName("检查用户名密码是否正确").success(false).description("请检查用户名或密码是否正确").build());

        oracleSink.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        oracleSink.put(TYPE.USERNAME_PASSWORD, DataSourceVerificationDetail.builder().displayName("检查用户名密码是否正确").success(false).description("请检查用户名或密码是否正确").build());


        oracleSource.put(TYPE.VERSION, DataSourceVerificationDetail.builder().displayName("检查数据源版本信息是否可用").success(false).description("数据源版本与DB不匹配").build());
        oracleSource.put(TYPE.MODEL, DataSourceVerificationDetail.builder().displayName("加载模型").success(false).description("请检查该用户是否有对应权限").build());
        oracleSource.put(TYPE.CDC, DataSourceVerificationDetail.builder().displayName("检查CDC同步所需的权限是否授权").success(false).description("请检查该用户是否有对应权限").build());
        oracleSource.put(TYPE.ARCHIVE, DataSourceVerificationDetail.builder().displayName("检查archive log是否开启").success(false).description("请检查数据库配置是否正确").build());

        //oracleSource.put(TYPE.SUPPLEMENTAL, DataSourceVerification.DataSourceVerificationDetail.builder().displayName("检查supplemental log模式是否正确").success(false).description("请检查数据库配置是否正确").build());

        sinkVerificationList.put("oracle", oracleSink);
        verificationList.put("oracle", oracleSource);

        Map<TYPE, DataSourceVerificationDetail> postgresSource = new LinkedHashMap<>();
        Map<TYPE, DataSourceVerificationDetail> postgresSink = new LinkedHashMap<>();
        postgresSource.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        postgresSource.put(TYPE.USERNAME_PASSWORD, DataSourceVerificationDetail.builder().displayName("检查用户名密码是否正确").success(false).description("请检查用户名或密码是否正确").build());

        postgresSink.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        postgresSink.put(TYPE.USERNAME_PASSWORD, DataSourceVerificationDetail.builder().displayName("检查用户名密码是否正确").success(false).description("请检查用户名或密码是否正确").build());


        postgresSource.put(TYPE.VERSION, DataSourceVerificationDetail.builder().displayName("检查数据源版本信息是否可用").success(false).description("数据源版本与DB不匹配").build());
        postgresSource.put(TYPE.MODEL, DataSourceVerificationDetail.builder().displayName("加载模型").success(false).description("请检查该用户是否有对应权限").build());
        postgresSource.put(TYPE.LOGICAL, DataSourceVerificationDetail.builder().displayName("是否开启逻辑复制").success(false).description("请检查wal_level是否为logical").build());


        verificationList.put("postgres", postgresSource);
        sinkVerificationList.put("postgres", postgresSink);

        Map<TYPE, DataSourceVerificationDetail> kafka = new LinkedHashMap<>();
        kafka.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        sinkVerificationList.put("kafka", kafka);

        Map<TYPE, DataSourceVerificationDetail> tidb = new LinkedHashMap<>();
        tidb.put(TYPE.CONNECTION, DataSourceVerificationDetail.builder().displayName("检查服务连接是否可用").success(false).description("请检查服务器地址是否正确").build());
        tidb.put(TYPE.VERSION, DataSourceVerificationDetail.builder().displayName("检查数据源版本信息是否可用").success(false).description("数据源版本与DB不匹配").build());

        sinkVerificationList.put("tidb", tidb);

    }

    public List<DataSourceVerificationDetail> updateIsSuccess(String dbType, TYPE... types) {
        if (types == null || types.length == 0) {
            if (isSink) {
                return new ArrayList<>(sinkVerificationList.get(dbType).values());
            }
            return new ArrayList<>(verificationList.get(dbType).values());
        }

        List<DataSourceVerificationDetail> returnList = new ArrayList<>();
        for (TYPE type : types) {
            DataSourceVerificationDetail detail = null;
            if (isSink) {
                detail = sinkVerificationList.get(dbType).get(type);
            }else {
                detail = verificationList.get(dbType).get(type);
            }

            if (detail == null) {
                continue;
            }
            detail.setSuccess(true);
            detail.setDescription(null);
            returnList.add(detail);
        }
        if (isSink) {
            return returnList;
        }
        return new ArrayList<>(verificationList.get(dbType).values());
    }


}
