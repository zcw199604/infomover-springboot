package com.info.infomover.common.convrter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.info.infomover.util.JsonBuilder;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Converter
public class SetStringConverter implements AttributeConverter<Set<String>,String> {

    private static final JsonBuilder jsonBuilder = JsonBuilder.getInstance();

    @Override
    public String convertToDatabaseColumn(Set<String> attribute) {
        if(attribute == null) {
            attribute = new HashSet<>();
        }
        return jsonBuilder.toJson(attribute);
    }

    @Override
    public Set<String> convertToEntityAttribute(String dbData) {
        if(dbData == null) {
            return new HashSet<>();
        }
        return jsonBuilder.fromJson(dbData, new TypeReference<Set<String>>(){});
    }
}
