package com.info.infomover.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class StepDesc implements Serializable {

    private static final long serialVersionUID = -598725136723181311L;

    public static final String STORAGE_TYPE = "type";

    public static final String STORAGE_TYPE_SEPARATOR = "_";

    private String id;

    private String name;

    private String type;

    private String scope;

    private String sourceName;

    private String rootSource;

    private String rootSink;

    private int x;

    private int y;

    private ConfigObject otherConfigurations = new ConfigObject();

    private StepFieldGroup inputConfigurations = new StepFieldGroup();

    private StepFieldGroup outputConfigurations = new StepFieldGroup();

    private ConfigObject transform = new ConfigObject();

    private ConfigObject filter = new ConfigObject();

    private ConfigObject uiConfigurations = new ConfigObject();

    private List<ConfigObject> tableMapping = new ArrayList<>();

    @ApiModelProperty("库信息")
    private List<String> libs;

    public StepDesc() {
        this("", "", "", null, null, null);
    }

    public StepDesc(String id, String name, String type, ConfigObject otherConfigurations,
                        StepFieldGroup inputConfigurations, StepFieldGroup outputConfigurations) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.otherConfigurations = otherConfigurations;
        this.inputConfigurations = inputConfigurations;
        this.outputConfigurations = outputConfigurations;
    }

    public StepDesc(String id, String name, StepDesc stepDef, ConfigObject otherConfigurations,
                        StepFieldGroup inputConfigurations, StepFieldGroup outputConfigurations) {
        this.id = id;
        this.name = name;
        this.type = stepDef.getType();
        this.libs = stepDef.getLibs();
        this.otherConfigurations = otherConfigurations;
        this.inputConfigurations = inputConfigurations;
        this.outputConfigurations = outputConfigurations;
    }

    /**
     * 判断是不是复合类型的Step
     *
     * @return
     */
    @JsonIgnore
    public boolean isCompositeStep() {
        if ("source".equals(this.type) ||
                "sink".equals(this.type) ||
                "lookup".equals(this.type)) {
            return true;
        }
        return false;
    }
}
