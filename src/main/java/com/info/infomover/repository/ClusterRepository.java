package com.info.infomover.repository;

import com.info.infomover.entity.Cluster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ClusterRepository extends JpaRepository<Cluster, Long> {

    List<Cluster> findByEnabled(Integer enabled);

    long countByName(String clusterName);
}
