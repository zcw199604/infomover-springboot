package com.info.infomover.loadbalancer;

import com.info.infomover.entity.Cluster;
import com.info.infomover.entity.ConnectDistribute;
import com.info.infomover.repository.ClusterRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Component
public class RandomLoadBalancer extends AbstractLoadBalancer {

    @Autowired
    private ClusterRepository clusterRepository;
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
                    int randomIndex = ThreadLocalRandom.current().nextInt(onLineDistributes.size());
                    url = onLineDistributes.get(randomIndex).getUrl();
                }
            } else {
                Cluster byId = clusterRepository.findById(clusterId).get();
                throw new IllegalArgumentException("this cluster has no available connections, Please check cluster status cluster " + byId.name);
            }
        } else {
            Cluster byId = clusterRepository.findById(clusterId).get();
            throw new IllegalArgumentException("this cluster has no available connections, Please check cluster status cluster " + byId.name);
        }
        logger.info("choose connectDistribute url result: {}", url);
        return new URI(url);
    }
}
