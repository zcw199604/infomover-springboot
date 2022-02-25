package com.info.infomover.crawler.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import java.io.Serializable;
import java.util.List;

/**
 * 元数据抓取外键信息表
 *
 * @author jingwei.yang
 * @date 2021年11月1日 上午10:23:08
 */
@ApiModel
@Data
public class CrawlerForeignKey extends BaseCrawlerEntity {
    private static final long serialVersionUID = -8278277012405477794L;

    private String tableId;

    private String tableName;

    private String shortName;

    private String specificName;

    private String definition;

    private List<String> constraintColumns;

    private String primaryKeyTable;

    private String referencedTable;

    private String referencingTable;

    private List<CrawlerColumnReference> columnReferences;

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CrawlerColumnReference implements Serializable {
        private static final long serialVersionUID = 8727277179073668222L;

        @ApiModelProperty(value = "主键列")
        private String primaryKeyColumn;

        @ApiModelProperty(value = "外键列")
        private String foreignKeyColumn;

        @ApiModelProperty(value = "键序列")
        private int keySequence;

    }
}
