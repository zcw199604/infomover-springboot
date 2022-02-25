package com.info.infomover.param;

import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.search.sort.SortOrder;

@Getter
@Setter
public class DownloadLogParam {
    private int offset;
    private int limit;
    private String level;
    private String searchWord;
    private int type;
    private Long startTime;
    private Long endTime;
    private SortOrder sort;
}
