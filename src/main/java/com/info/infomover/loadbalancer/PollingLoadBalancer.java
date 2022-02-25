package com.info.infomover.loadbalancer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.info.infomover.entity.Cluster;
import com.info.infomover.entity.ConnectDistribute;
import com.info.infomover.repository.ClusterRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class PollingLoadBalancer extends AbstractLoadBalancer {

    @Autowired
    private ClusterRepository clusterRepository;

    private static final long MAXIMUMSIZE = 30;
    private static final int expiration = 300;

    private LoadingCache<Long, Integer> attemptsCache;

    public PollingLoadBalancer() {
        attemptsCache = CacheBuilder.newBuilder().expireAfterWrite(expiration, TimeUnit.SECONDS)
                .expireAfterAccess(expiration, TimeUnit.SECONDS).maximumSize(MAXIMUMSIZE)
                .build(new CacheLoader<Long, Integer>() {
                    @Override
                    public Integer load(Long key) {
                        return 0;
                    }
                });
    }

    @Override
    public URI choose(Long clusterId) throws URISyntaxException {
        String url;
        if (clusterMap != null && clusterMap.containsKey(clusterId)) {
            List<ConnectDistribute> onLineDistributes = clusterMap.get(clusterId).stream()
                    .filter(c -> c.getStatus() == ConnectDistribute.ConnectDistributeStatus.ONLINE)
                    .collect(Collectors.toList());
            if (onLineDistributes != null && onLineDistributes.size() > 0) {
                if (onLineDistributes.size() == 1) {
                    url = onLineDistributes.get(0).getUrl();
                } else {
                    synchronized (attemptsCache) {
                        int index;
                        try {
                            index = attemptsCache.get(clusterId);
                        } catch (ExecutionException e) {
                            logger.warn("polling choose ConnectDistribute for cluster: {} failed," +
                                    "and it will always send to url: {}", clusterId, onLineDistributes.get(0).getUrl());
                            logger.warn("polling choose ConnectDistribute error message: ", e);
                            index = 0;
                        }
                        if (index >= onLineDistributes.size()) {
                            index = 0;
                            logger.debug("cluster {} onLineDistributes size mybay is error , restart selection ,update index -> 0", clusterId);
                        }
                        url = onLineDistributes.get(index).getUrl();
                        attemptsCache.put(clusterId, (index + 1) % onLineDistributes.size());
                    }
                }
            } else {
                Cluster byId = clusterRepository.findById(clusterId).get();
                throw new IllegalArgumentException("this cluster has no available connections, Please check cluster status cluster " + byId.name);
            }
        } else {
            Cluster byId = clusterRepository.findById(clusterId).get();
            throw new IllegalArgumentException("this cluster has no available connections, Please check cluster status cluster " + byId.name);
        }
        logger.debug("choose connectDistribute url result: {}", url);
        return new URI(url);
    }
}
