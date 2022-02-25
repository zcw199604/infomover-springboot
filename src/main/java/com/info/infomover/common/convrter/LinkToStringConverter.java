package com.info.infomover.common.convrter;


import com.fasterxml.jackson.core.type.TypeReference;
import com.info.infomover.entity.LinkDesc;
import com.info.infomover.util.JsonBuilder;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.util.ArrayList;
import java.util.List;

@Converter
public class LinkToStringConverter implements AttributeConverter<List<LinkDesc>,String> {

    private static final JsonBuilder jsonBuilder = JsonBuilder.getInstance();

    @Override
    public String convertToDatabaseColumn(List<LinkDesc> attribute) {
        if(attribute == null) {
            attribute = new ArrayList<>();
        }
        return jsonBuilder.toJson(attribute);
    }

    @Override
    public List<LinkDesc> convertToEntityAttribute(String dbData) {
        if(dbData == null) {
            return new ArrayList<>();
        }
        return jsonBuilder.fromJson(dbData, new TypeReference<List<LinkDesc>>(){});
    }
}
