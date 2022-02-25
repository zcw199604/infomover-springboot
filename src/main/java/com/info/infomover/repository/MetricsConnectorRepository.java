package com.info.infomover.repository;

import com.info.infomover.entity.MetricsConnector;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MetricsConnectorRepository extends JpaRepository<MetricsConnector, Long> {

}
