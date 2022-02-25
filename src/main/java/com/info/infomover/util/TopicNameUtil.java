package com.info.infomover.util;

import org.apache.commons.lang3.StringUtils;

public class TopicNameUtil {
    public static String parseTopicName(Long id, String topicPrefix, String tableName) {
        return String.format("%s_%d_%s", topicPrefix, id, tableName).replace("#", "_")
                .replace("\"","").replace(".", "_");
    }

    public static String parseHistoryTopicName(Long id, String topicPrefix) {
        return StringUtils.join(new String[]{topicPrefix, String.valueOf(id), "history_schema"}, "_");
    }

    public static String parseInternalTopicName(Long id, String topicPrefix) {
        return  topicPrefix+"_" + id + "_internal_schema";
    }
}
