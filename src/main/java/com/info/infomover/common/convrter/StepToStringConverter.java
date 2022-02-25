package com.info.infomover.common.convrter;


import com.fasterxml.jackson.core.type.TypeReference;
import com.info.infomover.entity.StepDesc;
import com.info.infomover.util.JsonBuilder;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.util.ArrayList;
import java.util.List;

@Converter
public class StepToStringConverter implements AttributeConverter<List<StepDesc>,String> {

    private static final JsonBuilder jsonBuilder = JsonBuilder.getInstance();

    @Override
    public String convertToDatabaseColumn(List<StepDesc> attribute) {
        if(attribute == null) {
            attribute = new ArrayList<>();
        }
        return jsonBuilder.toJson(attribute);
    }

    @Override
    public List<StepDesc> convertToEntityAttribute(String dbData) {
        if(dbData == null) {
            return new ArrayList<>();
        }
        return jsonBuilder.fromJson(dbData, new TypeReference<List<StepDesc>>(){});
    }
}
