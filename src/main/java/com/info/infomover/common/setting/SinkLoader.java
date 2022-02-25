package com.info.infomover.common.setting;

import com.info.infomover.datasource.AbstractDataSource;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class SinkLoader {

    private static Map<String, Class<? extends AbstractDataSource>> sinks = new HashMap<>();

    static {
        try (ScanResult scanResult = new ClassGraph()
                //.verbose() //If you want to enable logging to stderr
//                .acceptJars("infomover-*.jar")
                .acceptPackages("com.info.infomover")
                //.overrideClassLoaders(Thread.currentThread().getContextClassLoader())
                .enableAnnotationInfo()
                .scan()) {

            ClassInfoList classInfoList = scanResult.getClassesWithAnnotation(Sink.class);
            for (ClassInfo stepClassInfo : classInfoList) {
                Class<?> aClass = stepClassInfo.loadClass();
                try {
                    Sink sink = aClass.getAnnotation(Sink.class);
                    String type = sink.type();
                    sinks.put(type.toLowerCase(), (Class<? extends AbstractDataSource>) aClass);
                    log.info("Loaded Sink class : " + type);
                } catch (Exception e) {
                    log.error("Load Sink " + aClass.getName() + " failed: " + e.getMessage() + ", ignored.", e);
                }
            }
        }
    }

    public static Class<? extends AbstractDataSource> getSinkClass(String stepType) {
        return sinks.get(stepType.toLowerCase());
    }

    public static Set<String> getAllSupportSinkType() {
        return sinks.keySet();
    }

}
