package com.info.infomover.service.impl;

import com.info.infomover.entity.Connector;
import com.info.infomover.entity.QConnector;
import com.info.infomover.repository.ConnectorRepository;
import com.info.infomover.service.ConnectorService;
import com.io.debezium.configserver.model.ConnectorStatus;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;

@Service
@Slf4j
public class ConnectorServiceImpl implements ConnectorService {
    @Autowired
    private JPAQueryFactory jpaQueryFactory;

    @Autowired
    private ConnectorRepository connectorRepository;

    @Override
    public long countByJobIdAndConnectorStatus(Long jobId, ConnectorStatus.State state) {
        QConnector connector = QConnector.connector;

        return jpaQueryFactory.select(connector).from(connector).
                where(connector.jobId.eq(jobId), connector.connectorStatus.ne(state)).fetchCount();
    }

    @Override
    @Transactional
    public void saveConnectorLatest(long connectorId, String workingDir, String latestSnapshotFile) {
        Connector connector = connectorRepository.findById(connectorId).get();
        if (connector != null) {

            /*
             * delete old snapshot file
             * */
            if (StringUtils.isNotBlank(latestSnapshotFile)) {
                if (StringUtils.isNotBlank(connector.getLatestSnapshotFile())) {
                    log.info("Delete old snapshot file {}", connector.getLatestSnapshotFile());
                    File file = new File(workingDir + File.separator + connector.getLatestSnapshotFile());
                    if (file.exists()) {
                        file.delete();
                    }
                }
                connector.setLatestSnapshotFile(latestSnapshotFile);
            }

            connectorRepository.saveAndFlush(connector);
        } else {
            log.error("Not found connector by id: {}", connectorId);
        }
    }

    @Override
    @Transactional
    public void saveConnectorConfig(Connector connector) {
        if (connector != null) {
            connectorRepository.saveAndFlush(connector);
        } else {
            log.error("connect is empty");
        }
    }

    @Override
    @Transactional
    public void updateConnectName(long connectorId, String connectName) {
        Connector connector = connectorRepository.findById(connectorId).get();
        if (connector != null) {
            connector.setName(connectName);
            connectorRepository.saveAndFlush(connector);
        } else {
            log.error("Not found connector by id: {}", connectorId);
        }
    }

    @Override
    @Transactional
    public void updateConnectorStatus(String name, ConnectorStatus.State state) {
        Connector byName = connectorRepository.findByName(name);
        if (byName != null) {
            byName.setConnectorStatus(state);
            connectorRepository.saveAndFlush(byName);
        } else {
            log.error("Not found connector by name: {}", name);
        }
    }

    @Override
    @Transactional
    public long updateConnectorStatus(Long jobId, ConnectorStatus.State state) {
        QConnector connector = QConnector.connector;
        long execute = jpaQueryFactory.update(connector).set(connector.connectorStatus, state).where(connector.jobId.eq(jobId)).execute();
        log.info("update connector status , jobId :{} ,state :{},rows :{}", jobId, state, execute);
        return execute;
    }
}
