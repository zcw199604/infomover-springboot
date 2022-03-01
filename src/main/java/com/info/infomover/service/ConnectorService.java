package com.info.infomover.service;

import com.info.infomover.entity.Connector;
import com.io.debezium.configserver.model.ConnectorStatus;

public interface ConnectorService {
    long countByJobIdAndConnectorStatus(Long jobId, ConnectorStatus.State state);

    public void saveConnectorLatest(long connectorId, String workingDir, String latestSnapshotFile);

    public void saveConnectorConfig(Connector connector);

    public void updateConnectName(long connectorId, String connectName);

    public void updateConnectorStatus(String name, ConnectorStatus.State state);

    public long updateConnectorStatus(Long jobId, ConnectorStatus.State state);
}
