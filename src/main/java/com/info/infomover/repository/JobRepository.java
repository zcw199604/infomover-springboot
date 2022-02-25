package com.info.infomover.repository;

import com.info.infomover.entity.Job;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<Job, Long> {


    List<Job> findByDeployStatus(Job.DeployStatus deployStatus);

    List<Job> findByCreatorId(Long creatorId);

    long countByName(String name);
}
