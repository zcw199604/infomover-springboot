package com.info.infomover.crawler.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 元数据抓取主键信息
 *
 * @author jingwei.yang
 * @date 2021年11月1日 上午10:23:08
 */
@Data
public class CrawlerPrimaryKey implements Serializable {
    private static final long serialVersionUID = -7714204752331746270L;

    protected String name;

    protected String fullName;

    protected String remarks;

    private String shortName;

    private String definition;

    private List<String> constraintColumns;
}
