package com.info.infomover.resources;

import com.info.infomover.entity.*;
import com.info.infomover.prom.constants.MetricMethod;
import com.info.infomover.quartz.AlertRuleInitializer;
import com.info.infomover.repository.AlertRepository;
import com.info.infomover.repository.AlertRuleRepository;
import com.info.infomover.repository.JobAlertRuleRepository;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.util.UserUtil;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.core.Response;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequestMapping("/api/alert/rule")
@ResponseBody
@Controller
//@Tag(name = "infomover alert rule", description = "告警信息规则")
public class AlertRuleResource {
    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private AlertRuleRepository alertRuleRepository;

    @Autowired
    private JobAlertRuleRepository jobAlertRuleRepository;

    @Autowired
    private JPAQueryFactory queryFactory;


    @GetMapping("/list")
    /*@Operation(description = "任务列表")
    @Parameters({
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
            @Parameter(name = "metric", description = "指标名称", in = ParameterIn.QUERY),
            @Parameter(name = "after", description = "创建时间大于", in = ParameterIn.QUERY),
            @Parameter(name = "before", description = "创建时间小于", in = ParameterIn.QUERY),
            @Parameter(name = "sortby", description = "排序字段", in = ParameterIn.QUERY),
            @Parameter(name = "sortdirection", description = "排序方向", in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response list( @RequestParam(value = "pageNum",defaultValue = "1",required = false)  int page, @RequestParam(value = "pageSize",defaultValue = "10",required = false)  int limit,
                          @RequestParam(value = "metric",required = false)  String metric, @RequestParam(value = "after",required = false)  String createAfter,
                          @RequestParam(value = "before",required = false)  String createBefore,
                          @RequestParam(value = "sortby",defaultValue = "updateTime",required = false)  String sortBy,
                          @RequestParam(value = "sortdirection",defaultValue = "DESC",required = false)  Sort.Direction sortDirection){

        QAlertRule alertRule = QAlertRule.alertRule;
        JPAQuery<AlertRule> from = queryFactory.select(alertRule).from(alertRule);
        if (UserUtil.getUserRole().equals(User.Role.User.name())) {
            User user = userRepository.findByName(UserUtil.getUserName());
            if (user == null) {
                throw new RuntimeException("no user found with name " + UserUtil.getUserName());
            }
            from.where(alertRule.creatorId.eq(user.getId()));
        }

        if (StringUtils.isNotBlank(createAfter)) {
            from.where(alertRule.createTime.after(LocalDate.parse(createAfter).atStartOfDay()));
        }
        if (StringUtils.isNotBlank(createBefore)) {
            from.where(alertRule.createTime.before(LocalDate.parse(createBefore).atStartOfDay()));
        }

        Order order = sortDirection.isAscending() ? Order.ASC : Order.DESC;
        Path<Object> fieldPath = com.querydsl.core.types.dsl.Expressions.path(Object.class, alertRule, sortBy);
        from.orderBy(new OrderSpecifier(order, fieldPath));
        QueryResults<AlertRule> alertRuleQueryResults = from.offset(Page.getOffest(page, limit)).limit(limit).fetchResults();


        return Response.ok(Page.of(alertRuleQueryResults.getResults(), alertRuleQueryResults.getTotal(),alertRuleQueryResults.getLimit())).build();
    }


    @PostMapping
    @Transactional
    @RolesAllowed({"User", "Admin"})
    public Response create(@RequestBody AlertRule rule) {
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }
        rule.setId(null);
        rule.creator = user.name;
        rule.creatorId = user.getId();
        rule.createTime = LocalDateTime.now();
        rule.updateTime = rule.createTime;
        rule.type = AlertRule.RuleType.custom;
        alertRuleRepository.saveAndFlush(rule);

        return Response.ok(rule).build();
    }

    @PutMapping
    @Transactional
    @RolesAllowed({"User", "Admin"})
    public Response update(@RequestBody AlertRule rule){
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }

        if (AlertRule.RuleType.custom.equals(rule.type)) {
            if (MetricMethod.Common_Absent.equals(rule.method) || MetricMethod.None.equals(rule.method)) {
                rule.keepTime = 0;
            }
            rule.lastModifier=user.name;
            rule.lastModifierId=user.getId();
            rule.updateTime=LocalDateTime.now();
            alertRuleRepository.saveAndFlush(rule);
        } else {
            AlertRule existRule = alertRuleRepository.findById(rule.getId()).get();
            existRule.keepTime = rule.keepTime;
            existRule.duration = rule.duration;
            existRule.description = rule.description;
            existRule.updateTime = LocalDateTime.now();
            existRule.lastModifierId = user.getId();
            existRule.lastModifier = user.name;
            alertRuleRepository.saveAndFlush(existRule);
            if (AlertRule.RuleType.builtin.equals(existRule.type) || AlertRule.RuleType.builtinKafka.equals(existRule.type)) {
                AlertRuleInitializer.builtinRuleMap.put(existRule.name, existRule);
            }
        }
        return Response.ok(rule).build();
    }


    @DeleteMapping("delete")
    @Transactional
    @RolesAllowed({"User", "Admin"})
    public Response delete( @RequestBody Long[] ids){
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }

        for (Long id : ids) {
            AlertRule rule = alertRuleRepository.findById(id).get();
            List<JobAlertRule> jars = jobAlertRuleRepository.findByRuleId(id);
            if (jars!=null && jars.size()>0) {
                Map<String, String> err = new HashMap<>();
                err.put("message", String.format("%s is used by %d job ", rule.name, jars.size()));
                err.put("detail", String.format("%s is used by %d job ", rule.name, jars.size()));
                return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
            }
            if (AlertRule.RuleType.builtin.equals(rule.type) || AlertRule.RuleType.builtinKafka.equals(rule.type)){
                Map<String, String> err = new HashMap<>();
                err.put("message", String.format("%s是内置规则，不能删除", rule.name));
                err.put("detail", String.format("%s是内置规则，不能删除", rule.name));
                return Response.status(Response.Status.BAD_REQUEST).entity(err).build();
            }
            alertRuleRepository.delete(rule);
        }

        return Response.ok().build();
    }
}
