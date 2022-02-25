package com.info.infomover.repository;

import com.info.infomover.entity.Connector;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ConnectorRepository extends JpaRepository<Connector, Long> {

    Connector findByName(String connectorName);

    List<Connector> findByJobId(Long jobId);

    /*@Transactional
    public Connector findConnectorByName(String connectorName) {
        return find("name", connectorName).firstResult();
    }

    @Transactional
    public void saveConnectorLatest(long connectorId, String workingDir, String latestSnapshotFile) {
        log.info("Update connector latestSnapshotFile to {}", latestSnapshotFile);
        Connector connector = findById(connectorId);
        if (connector != null) {
            connector.setLatestSnapshotFile(workingDir, latestSnapshotFile);
            persistAndFlush(connector);
        } else {
            log.error("Not found connector by id: {}", connectorId);
        }
    }

    @Transactional
    public void saveConnectorConfig(long connectorId, Map<String,String> config) {
        Connector connector = findById(connectorId);
        if (connector != null) {
            Map<String, String> config1 = connector.getConfig();
            config1.clear();
            config1.putAll(config);
            persistAndFlush(connector);
        } else {
            log.error("Not found connector by id: {}", connectorId);
        }
    }

    @Transactional
    public void updateConnectName(long connectorId, String connectName) {
        Connector connector = findById(connectorId);
        if (connector != null) {
            connector.setName(connectName);
            persistAndFlush(connector);
        } else {
            log.error("Not found connector by id: {}", connectorId);
        }
    }
    @Transactional
    public void updateConnectorStatus(String name, ConnectorStatus.State state) {
        this.update("connectorStatus = ?1 where name = ?2", state, name);
    }*/
}
