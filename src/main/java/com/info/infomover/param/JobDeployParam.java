package com.info.infomover.param;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobDeployParam {

    private String clusterId;

    private Long[] jobIds;
}
