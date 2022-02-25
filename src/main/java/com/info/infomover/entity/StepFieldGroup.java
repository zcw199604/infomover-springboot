package com.info.infomover.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by DebugSy on 2019/8/14.
 */
public class StepFieldGroup extends HashMap<String, List<FieldDesc>> implements Serializable {
    private static final long serialVersionUID = 4429764033104848827L;

    public static StepFieldGroup newInstance() {
        return new StepFieldGroup();
    }

    public StepFieldGroup add(String key) {
        return add(key, new ArrayList<FieldDesc>());
    }

    public StepFieldGroup add(String key, List<FieldDesc> fields) {
        put(key, fields);
        return this;
    }

    public StepFieldGroup add(String key, FieldDesc... fields) {
        return add(key, Arrays.asList(fields));
    }

    public StepFieldGroup add(String key, FieldDesc field) {
        List<FieldDesc> fields = getOrDefault(key, new ArrayList<FieldDesc>());
        fields.add(field);
        return add(key, fields);
    }

}
