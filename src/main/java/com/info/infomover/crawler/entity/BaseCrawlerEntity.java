package com.info.infomover.crawler.entity;

import lombok.Data;

import java.util.Date;

@Data
public class BaseCrawlerEntity {
    private static final long serialVersionUID = -8688918053472143035L;

    protected String name;

    protected String fullName;

    protected String remarks;

    private Date crawlTime;


    private RelativeState relativeState;

    public static enum RelativeState {
        ADDED, // 新建
        UPDATED, // 更新
        DELETED// 删除
    }
}
