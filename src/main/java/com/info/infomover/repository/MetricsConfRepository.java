package com.info.infomover.repository;

import com.info.infomover.entity.MetricsConf;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MetricsConfRepository extends JpaRepository<MetricsConf, Long> {

    List<MetricsConf> findByConnectorType(String connectorType);

}
