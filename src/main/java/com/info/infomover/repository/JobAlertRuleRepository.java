package com.info.infomover.repository;

import com.info.infomover.entity.JobAlertRule;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobAlertRuleRepository extends JpaRepository<JobAlertRule, Long> {
    List<JobAlertRule> findByJobId(Long jobId);

    List<JobAlertRule> findByRuleId(Long ruleId);

    long deleteByJobId(Long jobId);

}
