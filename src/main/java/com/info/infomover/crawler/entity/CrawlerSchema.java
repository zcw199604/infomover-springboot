package com.info.infomover.crawler.entity;

import com.info.baymax.common.utils.ICollections;
import lombok.Data;

import java.util.List;

/**
 * 元数据抓取概要信息表
 *
 * @author jingwei.yang
 * @date 2021年11月1日 上午10:23:08
 */
@Data
public class CrawlerSchema extends BaseCrawlerEntity {
	private static final long serialVersionUID = -6316971850637408441L;

	private String catalogId;

	private String catalogName;

	private List<CrawlerTable> tables;

	private Integer tableCount;

	public int getTableCount() {
		if (ICollections.hasElements(this.tables)) {
			this.tableCount = this.tables.size();
		}
		if (this.tableCount == null) {
			this.tableCount = 0;
		}
		return this.tableCount;
	}
}
