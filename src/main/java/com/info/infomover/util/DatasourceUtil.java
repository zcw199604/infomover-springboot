package com.info.infomover.util;

import com.info.infomover.datasource.IDatasource;
import com.info.infomover.entity.DataSource;

import java.util.Map;

/**
 * @Author: haijun
 * @Date: 2021/12/5 0005 22:10
 */
public class DatasourceUtil {
    private static final JsonBuilder jsonBuilder = JsonBuilder.getInstance();
    /**
     * 将{@link DataSource}转成{@link IDatasource}
     * 主要包含参数的转换,暂时借用{@link JsonBuilder}完成
     * @param dataSource
     * @return
     */
    public static IDatasource transform2IDatasource(DataSource dataSource) {
        String type = dataSource.type;
        Map<String, String> configs = dataSource.config;
        if (!configs.containsKey("type")) {
            configs.put("type", type);
        }
        if (dataSource.category != null) {
            configs.put("dataSourcecategory", dataSource.category.toString());
        }
        String toJson = jsonBuilder.toJson(configs);
        return jsonBuilder.fromJson(toJson, IDatasource.class);
    }
}
