package com.info.infomover.crawler.entity;

import lombok.Data;

/**
 * 元数据抓取列信息表
 *
 * @author jingwei.yang
 * @date 2021年11月1日 上午10:23:08
 */

@Data
public class CrawlerColumn extends BaseCrawlerEntity {
	private static final long serialVersionUID = 1171796657247512725L;

	private String tableId;

	private String tableName;

	private String columnDataType;

	private String javaSqlType;

	private Integer decimalDigits;

	private Integer ordinalPosition;

	private Integer size;

	private String width;

	private Boolean isColumnDataTypeKnown;

	private Boolean isNullable;

	private String shortName;

	private Boolean isParentPartial;

	private String defaultValue;

	private Boolean isAutoIncremented;

	private Boolean isGenerated;

	private Boolean isHidden;

	private Boolean isPartOfForeignKey;

	private Boolean isPartOfIndex;

	private Boolean isPartOfPrimaryKey;

	private Boolean isPartOfUniqueIndex;
}
