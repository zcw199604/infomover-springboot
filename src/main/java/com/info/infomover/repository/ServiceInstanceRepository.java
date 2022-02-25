package com.info.infomover.repository;

import com.info.infomover.entity.ServiceInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ServiceInstanceRepository  extends JpaRepository<ServiceInstance, Long> {
}
