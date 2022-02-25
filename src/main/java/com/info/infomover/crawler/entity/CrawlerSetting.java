package com.info.infomover.crawler.entity;

import com.info.baymax.common.utils.ICollections;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 元数据爬虫配置
 *
 * @author jingwei.yang
 * @date 2021年11月1日 上午10:23:08
 */
@Setter
@Getter
public class CrawlerSetting implements Serializable {
	private static final long serialVersionUID = 4275612112788361453L;

	private String schemaInclusion = ".*";

	private List<String> schemas;

	public String getSchemaInclusion() {
		if (ICollections.hasElements(this.schemas)) {
			this.schemaInclusion = schemaInclusion(this.schemas);
		}
		return this.schemaInclusion;
	}

	public static String schemaInclusion(List<String> schemas) {
		if (ICollections.hasElements(schemas)) {
			return schemas.stream().collect(Collectors.joining("|"));
		}
		return null;
	}
}
