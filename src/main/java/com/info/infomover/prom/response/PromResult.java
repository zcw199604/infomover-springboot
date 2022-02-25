package com.info.infomover.prom.response;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@ToString
public class PromResult implements Serializable {
    
    private String status;
    private CommonData data;
    
    @Getter
    @Setter
    @ToString
    public static class CommonData {
        private String resultType;
        private List<Result> result;
    }
    
    @Getter
    @Setter
    @ToString
    public static class Result {
        private Metric metric;
        private List<Object> value;
        private List<List<Object>> values;
    }
    
    @Setter
    @Getter
    @ToString
    public static class Metric{
        private String instance;
        private String plugin;
        private String __name__;
        private String context;
        private String name;
        private String job;
    }
}
