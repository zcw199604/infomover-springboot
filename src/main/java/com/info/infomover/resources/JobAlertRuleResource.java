package com.info.infomover.resources;

import com.info.infomover.entity.AlertRule;
import com.info.infomover.entity.JobAlertRule;
import com.info.infomover.entity.User;
import com.info.infomover.repository.*;
import com.info.infomover.util.UserUtil;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.util.List;

@RequestMapping("/api/jobAlertRule")
@ResponseBody
@Controller
//@Tag(name = "infomover job alert rule", description = "任务关联的")
public class JobAlertRuleResource {
    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private DataSourceRepository dataSourceRepository;

    @Autowired
    private AlertRuleRepository alertRuleRepository;

    @Autowired
    private JobAlertRuleRepository jobAlertRuleRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    @PostMapping("/{jobId}")
    @Transactional
    @RolesAllowed({"User", "Admin"})
    public Response createOrUpdate( @PathVariable("jobId") Long jobId, @RequestBody List<JobAlertRule> jars) {
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }
        if (jobId == null) {
            throw new RuntimeException("job id is empty ");
        }
        LocalDateTime createTime = LocalDateTime.now();
        jobAlertRuleRepository.deleteByJobId(jobId);
        for (JobAlertRule jar : jars) {
            if (jar.getId() != null) {
                jobAlertRuleRepository.deleteById(jar.getId());
            }
            jar.setId(null);
            jar.jobId = jobId;
            jar.creatorId = user.getId();
            jar.creator = user.name;
            jar.createTime = createTime;

            AlertRule rule = alertRuleRepository.findById(jar.ruleId).get();
            jar.ruleName = rule.name;
        }

        jobAlertRuleRepository.saveAll(jars);

        return Response.ok(jars).build();
    }

    @GetMapping("/list")
    public Response list( @RequestParam(value = "jobId",required = false)  Long jobId) {
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }
        if (jobId == null) {
            throw new RuntimeException("job id is empty ");
        }

        List<JobAlertRule> jars = jobAlertRuleRepository.findByJobId(jobId);

        return Response.ok(jars).build();
    }

}
