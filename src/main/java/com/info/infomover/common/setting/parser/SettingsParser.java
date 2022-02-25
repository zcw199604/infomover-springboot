package com.info.infomover.common.setting.parser;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.infomover.common.setting.Select;
import com.info.infomover.common.setting.SelectType;
import com.info.infomover.common.setting.Setting;
import com.info.infomover.common.setting.SettingsContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by P0007 on 2019/12/23.
 * <p>
 * 提取{@link Setting}标识的可配置参数
 */
@Slf4j
public class SettingsParser {

  public static List<SettingField> extractRtcSettingTypeAsList(Class<?> aClass) {
    List<SettingField> settingFields = new ArrayList<>();

    Class<?> superclass = aClass.getSuperclass();
    while (superclass != null) {
      extractRtcSettingTypeAsList(superclass, settingFields);
      superclass = superclass.getSuperclass();
    }

    extractRtcSettingTypeAsList(aClass, settingFields);
    Collections.sort(settingFields);
    return settingFields;
  }

  private static void extractRtcSettingTypeAsList(Class<?> aClass, List<SettingField> settingFields) {
    Field[] declaredFields = aClass.getDeclaredFields();
    for (Field field : declaredFields) {
      try {
        SettingField settingField = new SettingField();
        field.setAccessible(true);
        String fieldName = field.getName();

        /*
         * 判断是否是@JsonAlias注解, 并提取参数
         * */
        JsonAlias jsonAliasAnn = field.getAnnotation(JsonAlias.class);
        if (jsonAliasAnn != null) {
          String[] alias = jsonAliasAnn.value();
          settingField.setName(alias[0]);
        } else {
          settingField.setName(fieldName);
        }
        Class<?> fieldType = field.getType();
        if (fieldType.isArray() && !fieldType.getComponentType().isAssignableFrom(String.class)) {
          Class<?> claz = fieldType.getComponentType();
          List<SettingField> subSettingFields = extractRtcSettingTypeAsList(claz);
          settingField.setType(subSettingFields);
        } else if (fieldType.isArray() && fieldType.getComponentType().isInstance(String.class)) {
          List<String> subSettingFields = new ArrayList<>();
          settingField.setType(subSettingFields);
        } else if (!fieldType.isArray() && fieldType.isAnnotationPresent(SettingsContainer.class)) {
          Map<String, SettingField> settingFieldMap = extractRtcSettingTypeAsMap(fieldType);
          settingField.setType(settingFieldMap);
        } else if (fieldType.isArray() && fieldType.isAnnotationPresent(SettingsContainer.class)) {
          /*
           * SettingsContainer注解表示该类还包含下层settings, 获取其包含的相关参数
           * */
          List<SettingField> subSettingFields = extractRtcSettingTypeAsList(fieldType);
          settingField.setType(subSettingFields);
        } else {
          settingField.setType(fieldType.getSimpleName());
        }

        /*
         * 判断是否是Step Setting, 并提取参数
         * */
        Setting annotation = field.getAnnotation(Setting.class);
        if (annotation != null) {
          extractRtcSettings(settingField, annotation);
          settingFields.add(settingField);
        }
      } catch (Exception e) {
        throw new RuntimeException("parse class '" + aClass.getSimpleName() + "' setting field '" +
                field.getName() + "' throw exception.", e);
      }
    }
  }

  /**
   * 提取{@link Setting}的相关参数
   *
   * @param settingField
   * @param annotation
   */
  private static void extractRtcSettings(SettingField settingField, Setting annotation) {
    String defaultValue = annotation.defaultValue();
    String displayName = annotation.displayName();
    String[] values = annotation.values();
    String description = annotation.description();
    String tips = annotation.tips();
    String category = annotation.category();
    boolean advanced = annotation.advanced();
    boolean required = annotation.required();
    boolean hidden = annotation.hidden();
    int order = annotation.order();
    boolean disabled = annotation.disabled();
    String format = annotation.format();
    String scope = annotation.scope();
    String scopeType = annotation.scopeType();
    String[] binds = annotation.bind();
    Select select = annotation.select();
    SelectType selectType = annotation.selectType();
    String fieldType = settingField.getType().toString();
    settingField.setDefaultValue(convert(defaultValue, fieldType));
    settingField.setFormat(StringUtils.isNoneEmpty(format) ? format : null);
    settingField.setDescription(StringUtils.isNoneEmpty(description) ? description : null);
    settingField.setDisplayName(StringUtils.isNoneEmpty(displayName) ? displayName : null);
    settingField.setTips(StringUtils.isNoneEmpty(tips) ? tips : settingField.getDescription());
    settingField.setCategory(StringUtils.isNotEmpty(category) ? category : null);
    settingField.setValues(ArrayUtils.isNotEmpty(values) ? values : null);
    settingField.setScope(StringUtils.isNoneEmpty(scope) ? scope : null);
    Object[] convertedBinds = Arrays.stream(binds).map(bind -> convert(bind, scopeType)).toArray();
    settingField.setBind(ArrayUtils.isNotEmpty(binds) ? convertedBinds : null);
    settingField.setDisabled(disabled);
    settingField.setOrder(order);
    settingField.setAdvanced(advanced);
    settingField.setRequired(required);
    settingField.setHidden(hidden);
    settingField.setSelect(select);
    settingField.setSelectType(selectType);
  }

  private static Map<String, SettingField> extractRtcSettingTypeAsMap(Class<?> aClass) {
    Map<String, SettingField> settingFieldsMap = new LinkedHashMap<>();
    List<SettingField> settingFields = new ArrayList<>();

    Class<?> superclass = aClass.getSuperclass();
    while (superclass != null) {
      extractRtcSettingTypeAsList(superclass, settingFields);
      superclass = superclass.getSuperclass();
    }

    extractRtcSettingTypeAsList(aClass, settingFields);
    Collections.sort(settingFields);
    for (SettingField settingField : settingFields) {
      String name = settingField.getName();
      settingFieldsMap.put(name, settingField);
    }
    return settingFieldsMap;
  }

  public static Object convert(String value, String dataType) {
    if (StringUtils.isEmpty(value)) {
      return null;
    }
    switch (dataType.toLowerCase()) {
      case "char":
      case "string":
        return value.trim();
      case "integer":
      case "int":
        return Integer.parseInt(value);
      case "bigint":
      case "long":
        return Long.parseLong(value);
      case "float":
        return Float.parseFloat(value);
      case "double":
        return Double.parseDouble(value);
      case "boolean":
        return Boolean.parseBoolean(value);
      case "date":
        return Date.valueOf(value);
      case "timestamp":
        return Timestamp.valueOf(value);
      case "binary":
        return Byte.parseByte(value);
      case "decimal":
        return new BigDecimal(value);
      default:
        return value;
    }
  }
}
