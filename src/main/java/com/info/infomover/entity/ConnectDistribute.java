package com.info.infomover.entity;

public class ConnectDistribute {
    private Long cluster;
    private String url;
    private ConnectDistributeStatus status;
    private String kafkaClusterId;
    private String version;
    private String commit;
    private long checkedTime = 0L;

    public ConnectDistribute() {
    }

    public ConnectDistribute(Long clusterId, String uri) {
        this.cluster = clusterId;
        this.url = uri;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public ConnectDistributeStatus getStatus() {
        return status;
    }

    public void setStatus(ConnectDistributeStatus status) {
        this.status = status;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCommit() {
        return commit;
    }

    public void setCommit(String commit) {
        this.commit = commit;
    }

    public Long getCluster() {
        return cluster;
    }

    public void setCluster(Long cluster) {
        this.cluster = cluster;
    }

    public String getKafkaClusterId() {
        return kafkaClusterId;
    }

    public void setKafkaClusterId(String kafkaClusterId) {
        this.kafkaClusterId = kafkaClusterId;
    }

    public long getCheckedTime() {
        return checkedTime;
    }

    public void setCheckedTime(long checkedTime) {
        this.checkedTime = checkedTime;
    }

    public enum ConnectDistributeStatus {
        ONLINE,
        OFFLINE
    }
}
