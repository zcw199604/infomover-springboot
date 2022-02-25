package com.info.infomover.sink;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.info.infomover.common.setting.Setting;

public class KafkaCluster {

    @Setting(displayName = "cluster的配置Id", description = "cluster的配置Id", hidden = true)
    @JsonAlias("id")
    private String clusterId;

    @Setting(displayName = "cluster的配置名称", description = "cluster的配置名称", hidden = true)
    @JsonAlias("name")
    private String clusterName;

    @Setting(description = "bootstrap地址", displayName = "bootstrap地址", disabled = true)
    @JsonAlias("brokerList")
    private String bootStrapServer;

    @Setting(description = "connector地址", displayName = "connector地址", disabled = true)
    @JsonAlias("connectorUrl")
    private String connectorUrl;
}
