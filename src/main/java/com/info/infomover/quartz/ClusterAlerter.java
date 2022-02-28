package com.info.infomover.quartz;

import com.info.infomover.entity.*;
import com.info.infomover.repository.AlertRepository;
import com.info.infomover.repository.ClusterRepository;
import com.info.infomover.repository.ServiceInstanceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.info.infomover.loadbalancer.AbstractLoadBalancer.clusterMap;

@Component
public class ClusterAlerter {


    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private AlertRuleInitializer alertRuleInitializer;

    @Autowired
    private ServiceInstanceRepository serviceInstanceRepository;
    public void execute() {
        if (clusterMap == null) {
            return;
        }
        LocalDateTime executeTime = LocalDateTime.now();
        Map<Long, Cluster> enableClusterMap = loadEnableCluster();
        for (Map.Entry<Long, List<ConnectDistribute>> entry : clusterMap.entrySet()) {
            Cluster cluster = enableClusterMap.get(entry.getKey());
            if (cluster == null) {
                continue;
            }
            List<ConnectDistribute> distributeList = entry.getValue();
            for (ConnectDistribute connectDistribute : distributeList) {
                if (ConnectDistribute.ConnectDistributeStatus.ONLINE.equals(connectDistribute.getStatus())) {
                    resolveOrIgnore(cluster, connectDistribute.getUrl(), executeTime);
                } else {
                    fireAlert(cluster, connectDistribute.getUrl(), executeTime);
                }
            }
        }
    }
    
    @Transactional
    public void fireAlert(Cluster cluster, String url, LocalDateTime executeTime){
        AlertRule clusterNodeDownRule = alertRuleInitializer.getRule(AlertRuleInitializer.clusterDown, AlertRule.RuleType.builtinKafka);
        Alert alert;
        alert = findAlert(cluster.getId(), url, Alert.AlertStatus.pending);
        if (alert!=null) {
            if (alert.createTime
                    .plus(clusterNodeDownRule.keepTime*clusterNodeDownRule.duration.ms, ChronoUnit.MILLIS)
                    .isBefore(executeTime)) {
                alert.status = Alert.AlertStatus.firing;
            }
            alert.updateTime = executeTime;
        } else {
            alert = findAlert(cluster.getId(), url, Alert.AlertStatus.firing);
            if (alert!=null) {
                alert.updateTime = executeTime;
            } else {
                alert = new Alert();
                alert.name = "ClusterNodeDown";
                alert.clusterId = cluster.getId();
                alert.cluster = cluster.name;
                alert.status = Alert.AlertStatus.pending;
                alert.createTime = executeTime;
                alert.creatorId = cluster.creatorId;
                alert.creator = cluster.creatorChineseName;
                alert.updateTime = executeTime;
                alert.lastModifierId = cluster.creatorId;
                alert.lastModifier = cluster.creatorChineseName;
                alert.description = url;
                alert.type = Alert.Type.Cluster;
                alert.ruleName = "ClusterNodeDown";
            }
        }
        alertRepository.saveAndFlush(alert);
    }
    
    @Transactional
    public void resolveOrIgnore(Cluster cluster, String url, LocalDateTime executeTime) {
        Alert alert;
        alert = findAlert(cluster.getId(), url, Alert.AlertStatus.firing);
        if (alert!=null) {
            alert.status = Alert.AlertStatus.resolved;
            alert.updateTime = executeTime;
            alert.lastModifier = cluster.creatorChineseName;
            alert.lastModifierId = cluster.creatorId;
        } else {
            alert = findAlert(cluster.getId(), url, Alert.AlertStatus.pending);
            if (alert != null) {
                alert.status = Alert.AlertStatus.ignore;
                alert.updateTime = executeTime;
                alert.lastModifier = cluster.creatorChineseName;
                alert.lastModifierId = cluster.creatorId;
            }
        }
        if (alert!=null) {
            alertRepository.saveAndFlush(alert);
        }
    }
    
    @Transactional
    public Alert findAlert(Long clusterId, String url, Alert.AlertStatus status) {

        List<Alert> byClusterIdAndTypeAndStatusAndDescription = alertRepository.findByClusterIdAndTypeAndStatusAndDescription(clusterId, Alert.Type.Cluster, status, url);
        return byClusterIdAndTypeAndStatusAndDescription.get(0);
    }
    
    @Transactional
    public Map<Long, Cluster> loadEnableCluster() {
        List<ServiceInstance> instanceList = serviceInstanceRepository.findAll();
        instanceList = instanceList.stream().sorted(Comparator.comparing(o -> o.getId())).collect(Collectors.toList());
        
        int instanceIndex = 0;
        for (int i = 0; i < instanceList.size(); i++) {
            ServiceInstance instance = instanceList.get(i);
            if (instance.getId().equals(InstanceRegister.instance.getId())) {
                instanceIndex = i;
            }
        }
        int instanceSize = instanceList == null || instanceList.size() == 0 ? 1 : instanceList.size();
        List<Cluster> clusterList = clusterRepository.findByEnabled(1);

        Map<Long, Cluster> result = new HashMap<>();
        
        for (Cluster cluster : clusterList) {
            if (cluster.getId().hashCode() % instanceSize == instanceIndex) {
                result.put(cluster.getId(), cluster);
            }
        }
        return result;
    }
}
