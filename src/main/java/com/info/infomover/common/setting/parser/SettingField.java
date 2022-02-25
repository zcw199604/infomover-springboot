package com.info.infomover.common.setting.parser;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.info.infomover.common.setting.Select;
import com.info.infomover.common.setting.SelectType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by P0007 on 2019/8/15.
 */
@Setter
@Getter
@NoArgsConstructor
public class SettingField implements Comparable<SettingField> {

    private String name;

    private String displayName;

    private Object type;

    private Object defaultValue;

    private String description;//默认是中文

    private String tips;//默认是中文

    private String category;

    private Object[] values;

    private boolean required;

    private boolean advanced;
    
    private boolean hidden;

    private String scope;

    private Object[] bind;

    private Select select;

    private SelectType selectType;

    private String format;

    private boolean disabled;

    @JsonIgnore
    private int order;

    @JsonIgnore
    private boolean isSettingsContainer;

    public SettingField(String name, Object type, boolean required) {
        this.name = name;
        this.type = type;
        this.required = required;
    }

    public SettingField(String name, Object type, Object defaultValue, Object[] values, boolean required) {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
        this.values = values;
        this.required = required;
    }

    @Override
    public int compareTo(SettingField o) {
        return this.order - o.order;
    }
}
