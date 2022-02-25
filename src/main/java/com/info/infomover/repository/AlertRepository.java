package com.info.infomover.repository;

import com.info.infomover.entity.Alert;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AlertRepository extends JpaRepository<Alert, Long> {

    public long deleteByJobId(Long jobId);

    List<Alert> findByClusterIdAndTypeAndStatusAndDescription(Long clusterId, Alert.Type type, Alert.AlertStatus status, String url);

}
