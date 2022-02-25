package com.info.infomover.service;

import com.io.debezium.configserver.model.ConnectorStatus;

import java.util.Map;

public interface ConnectorService {
    long countByJobIdAndConnectorStatus(Long jobId, ConnectorStatus.State state);

    public void saveConnectorLatest(long connectorId, String workingDir, String latestSnapshotFile);

    public void saveConnectorConfig(long connectorId, Map<String, String> config);

    public void updateConnectName(long connectorId, String connectName);

    public void updateConnectorStatus(String name, ConnectorStatus.State state);

    public long updateConnectorStatus(Long jobId, ConnectorStatus.State state);
}
