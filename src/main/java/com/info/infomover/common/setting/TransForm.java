package com.info.infomover.common.setting;

import com.io.debezium.configserver.model.ConnectorProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class TransForm {
    public String transform;
    public boolean enabled;
    public List<ConnectorProperty> configurationOptions;

    public TransForm(String transform, boolean enabled, List<ConnectorProperty> configurationOptions) {
        this.transform = transform;
        this.enabled = enabled;
        this.configurationOptions = configurationOptions;
    }
}
