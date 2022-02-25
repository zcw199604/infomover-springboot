package com.info.infomover.common.convrter;

import com.info.infomover.entity.ConfigObject;
import com.info.infomover.util.JsonBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.AttributeConverter;
import javax.persistence.Convert;
import java.util.ArrayList;
import java.util.List;

@Convert
public class TableMappingConverter implements AttributeConverter<List<ConfigObject>,String> {
    private static final JsonBuilder jsonBuilder = JsonBuilder.getInstance();

    @Override
    public String convertToDatabaseColumn(List<ConfigObject> attribute) {
        if (attribute == null) {
            attribute = new ArrayList<>();
        }
        return jsonBuilder.toJson(attribute);
    }

    @Override
    public List<ConfigObject> convertToEntityAttribute(String dbData) {
        if (StringUtils.isBlank(dbData)) {
            dbData = "[]";
        }
        return jsonBuilder.fromJson(dbData, List.class);
    }
}
