package com.info.infomover.resources.response;

import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * create by pengchuan.chen on 2021/11/10
 */
@Getter
@Setter
public class ConnectorLog {

    private Long totalCount;

    private List<LogEntity> content = new ArrayList<>();

    private List<SearchHit> tmp;

    public void addLog(LogEntity logEntity) {
        this.content.add(logEntity);
    }

    @Getter
    @Setter
    public static class LogEntity {

        private String id;

        private String type;

        private long timestamp;

        private String method;

        private String source_host;

        private String path;

        private String thread_name;

        private String message;

        private String file;

        private String connector_name;

        private String level;

        private String logger_name;

        private String line_number;

        private Map<String, Object> mdc;

        private Map<String, Object> exception;
    }
}
