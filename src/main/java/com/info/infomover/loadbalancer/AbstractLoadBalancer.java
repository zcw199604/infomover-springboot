package com.info.infomover.loadbalancer;

import com.info.infomover.entity.ConnectDistribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public abstract class AbstractLoadBalancer {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static ConcurrentMap<Long, List<ConnectDistribute>> clusterMap;

    public abstract URI choose(Long clusterId) throws URISyntaxException;

}
