package com.info.infomover.crawler.entity;

import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * 元数据抓取表信息表
 *
 * @author jingwei.yang
 * @date 2021年11月1日 上午10:23:08
 */
@Data
public class CrawlerTable extends BaseCrawlerEntity {
	private static final long serialVersionUID = -8923513135746805343L;
	public static final String TABLE_NAME = "schema_crawler_table";

	private String crawlerSchemaId;

	private String crawlerSchemaName;

	private String definition;

	private String tableType;

	private CrawlerPrimaryKey primaryKey;

	private List<CrawlerForeignKey> foreignKeys;

	private List<CrawlerColumn> columns;

	private Boolean isSync;

	private Date syncTime;

	private String schemaId;

	private String schemaName;

	private Integer schemaVersion;

	private String datasetId;

	private String datasetName;

}
