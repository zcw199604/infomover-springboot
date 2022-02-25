package com.info.infomover.crawler.entity;

import com.info.baymax.common.utils.ICollections;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 元数据抓取目录信息表
 *
 * @author jingwei.yang
 * @date 2021年11月1日 上午10:23:08
 */

@Data
public class CrawlerCatalog extends BaseCrawlerEntity {
    private static final long serialVersionUID = -2804908559856867294L;

    public static final String TABLE_NAME = "schema_crawler_catalog";

    private CrawlerSetting setting;

    private Boolean crawlResult;

    private List<CrawlerSchema> schemas;

    private Integer schemaCount;

    private Integer tableCount;

    public Integer getSchemaCount() {
        if (ICollections.hasElements(this.schemas)) {
            this.schemaCount = this.schemas.size();
        }
        if (this.schemaCount == null) {
            this.schemaCount = 0;
        }
        return this.schemaCount;
    }

    public Integer getTableCount() {
        if (ICollections.hasElements(this.schemas)) {
            this.tableCount = this.schemas.stream().collect(Collectors.summingInt(CrawlerSchema::getTableCount));
        }
        if (this.tableCount == null) {
            this.tableCount = 0;
        }
        return this.tableCount;
    }
}
