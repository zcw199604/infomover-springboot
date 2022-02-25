package com.info.infomover.common.setting;

import com.info.infomover.datasource.AbstractDataSource;
import com.info.infomover.datasource.DataSourceScope;
import com.info.infomover.datasource.DataSourceScopeType;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DataSourceLoader {

    private static Map<String, Class<? extends AbstractDataSource>> dataSources = new HashMap<>();
    private static Map<DataSourceScopeType, List<String>> dataSourceScope = new ConcurrentHashMap<>();

    static {
        try (ScanResult scanResult = new ClassGraph()
                //.verbose() //If you want to enable logging to stderr
//                .acceptJars("infomover-*.jar")
                .acceptPackages("com.info.infomover")
                //.overrideClassLoaders(Thread.currentThread().getContextClassLoader())
                .enableAnnotationInfo()
                .scan()) {

            ClassInfoList classInfoList = scanResult.getClassesWithAnnotation(DataSource.class);
            for (ClassInfo stepClassInfo : classInfoList) {
                Class<?> aClass = stepClassInfo.loadClass();
                try {
                    DataSource dataSource = aClass.getAnnotation(DataSource.class);
                    String type = dataSource.type();
                    dataSources.put(type.toLowerCase(), (Class<? extends AbstractDataSource>) aClass);
                    DataSourceScope annotation = aClass.getAnnotation(DataSourceScope.class);
                    if (annotation != null) {
                        DataSourceScopeType[] value = annotation.value();
                        if (value != null) {
                            for (DataSourceScopeType dataSourceScopeType : value) {
                                if (dataSourceScopeType == DataSourceScopeType.All) {
                                    List<String> Source = dataSourceScope.getOrDefault(DataSourceScopeType.Source, new ArrayList<>(16));
                                    Source.add(type);
                                    List<String> Sink = dataSourceScope.getOrDefault(DataSourceScopeType.Sink, new ArrayList<>(16));
                                    Sink.add(type);
                                    dataSourceScope.put(DataSourceScopeType.Source, Source);
                                    dataSourceScope.put(DataSourceScopeType.Sink, Sink);
                                } else {
                                    List<String> scopeType = dataSourceScope.getOrDefault(dataSourceScopeType, new ArrayList<>(16));
                                    scopeType.add(type);
                                    dataSourceScope.put(dataSourceScopeType, scopeType);
                                }
                            }
                        }
                    }
                    log.info("Loaded datasource class : " + type);
                } catch (Exception e) {
                    log.error("Load datasource " + aClass.getName() + " failed: " + e.getMessage() + ", ignored.", e);
                }
            }
        }
    }

    public static Class<? extends AbstractDataSource> getDataSourceClass(String stepType) {
        return dataSources.get(stepType.toLowerCase());
    }

    public static Set<String> getAllSupportDataSourceType() {
        return dataSources.keySet();
    }


    public static Set<DataSourceScopeType> getDataSourceScope() {
        return dataSourceScope.keySet();
    }

    public static List<String> getDataSourceScopeType(DataSourceScopeType dataSourceScopeType) {
        return dataSourceScope.get(dataSourceScopeType);
    }

}
