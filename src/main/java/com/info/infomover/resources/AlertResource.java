package com.info.infomover.resources;

import com.info.infomover.entity.Alert;
import com.info.infomover.entity.Page;
import com.info.infomover.entity.QAlert;
import com.info.infomover.entity.User;
import com.info.infomover.repository.AlertRepository;
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

@RequestMapping("/api/alert")
@ResponseBody
@Controller
public class AlertResource {

    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    @GetMapping("/{id}")
    /*@Operation(description = "告警详情")
    @Parameters({
            @Parameter(name = "id", description = "告警ID", required = true, in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response get(@PathVariable("id") Long id){
        return Response.ok(alertRepository.findById(id).get()).build();
    }

    @GetMapping("/list")
    /*@Operation(description = "任务列表")
    @Parameters({
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
            @Parameter(name = "ruleName", description = "告警规则名称", in = ParameterIn.QUERY),
            @Parameter(name = "creator", description = "创建人中文名", in = ParameterIn.QUERY),
            @Parameter(name = "clusterName", description = "集群名称", in = ParameterIn.QUERY),
            @Parameter(name = "type", description = "类型", in = ParameterIn.QUERY),
            @Parameter(name = "connector", description = "连接器名称", in = ParameterIn.QUERY),
            @Parameter(name = "job", description = "任务名称", in = ParameterIn.QUERY),
            @Parameter(name = "after", description = "创建时间大于", in = ParameterIn.QUERY),
            @Parameter(name = "before", description = "创建时间小于", in = ParameterIn.QUERY),
            @Parameter(name = "sortby", description = "排序字段", in = ParameterIn.QUERY),
            @Parameter(name = "sortdirection", description = "排序方向", in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response list(@RequestParam(value = "pageNum",defaultValue = "1",required = false)  int page, @RequestParam(value = "pageSize",defaultValue = "10",required = false)  int limit,
                         @RequestParam(value = "ruleName",required = false)  String ruleName, @RequestParam(value = "type",required = false)  Alert.Type type, @RequestParam(value = "clusterName",required = false)  String clusterName ,
                         @RequestParam(value = "creator",required = false)  String creatorName, @RequestParam(value = "connector",required = false)  String connector,
                         @RequestParam(value = "job",required = false)  String job, @RequestParam(value = "status",required = false)  Alert.AlertStatus status,
                         @RequestParam(value = "after",required = false)  String createAfter, @RequestParam(value = "before",required = false)  String createBefore,
                         @RequestParam(value = "sortby",defaultValue = "createTime",required = false)  String sortBy,
                         @RequestParam(value = "sortdirection",defaultValue = "DESC",required = false)  Sort.Direction sortDirection){

        QAlert alert = QAlert.alert;
        JPAQuery<Alert> select = queryFactory.select(alert).from(alert);
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user != null && "admin".equalsIgnoreCase(user.getRole())) {
            if (user == null) {
                throw new RuntimeException("no user found with name " + UserUtil.getUserName());
            }
            select.where(alert.creatorId.eq(user.getId()));
        }

        if (StringUtils.isNotBlank(connector)) {
            select.where(alert.connector.like("%" + connector + "%"));
        }

        if (StringUtils.isNotBlank(clusterName )) {
            select.where(alert.cluster.like("%" + clusterName + "%"));
        }

        if (StringUtils.isNotBlank(job)) {
            select.where(alert.job.like("%" + job + "%"));
        }

        if (StringUtils.isNotBlank(ruleName)) {
            select.where(alert.ruleName.like("%" + ruleName + "%"));
        }

        if (status!=null) {
            select.where(alert.status.eq(status));
        }
        if (type!=null) {
            select.where(alert.type.eq(type));
        }

        if (StringUtils.isNotBlank(createAfter)) {
            select.where(alert.createTime.after(LocalDate.parse(createAfter).atStartOfDay()));
        }
        if (StringUtils.isNotBlank(createBefore)) {
            select.where(alert.createTime.before(LocalDate.parse(createBefore).atStartOfDay()));
        }
        Order order = sortDirection.isAscending() ? Order.ASC : Order.DESC;
        Path<Object> fieldPath = com.querydsl.core.types.dsl.Expressions.path(Object.class, alert, sortBy);
        select.orderBy(new OrderSpecifier(order, fieldPath));
        QueryResults<Alert> alertQueryResults = select.offset(Page.getOffest(page,limit)).limit(limit).fetchResults();

        return Response.ok(Page.of(alertQueryResults.getResults(), alertQueryResults.getTotal(),alertQueryResults.getLimit())).build();
    }

    @PutMapping("/resolve")
    @RolesAllowed({"User", "Admin"})
    @Transactional
    public Response resolve(@RequestBody Long[] ids){
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }

        for (Long id : ids) {
            Alert alert = alertRepository.findById(id).get();
            if (alert.creatorId.equals(user.getId()) && !alert.status.equals(Alert.AlertStatus.resolved)) {
                alert.status = Alert.AlertStatus.resolved;
                alert.updateTime = LocalDateTime.now();
                alert.lastModifier = user.name;
                alert.lastModifierId = user.getId();
                alertRepository.saveAndFlush(alert);
            }
        }
        return Response.ok().build();
    }

    @PutMapping("/ignore")
    @RolesAllowed({"User", "Admin"})
    @Transactional
    public Response ignore(@RequestBody Long[] ids){
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }

        for (Long id : ids) {
            Alert alert = alertRepository.findById(id).get();
            if (alert.creatorId.equals(user.getId()) && !alert.status.equals(Alert.AlertStatus.ignore)) {
                alert.status = Alert.AlertStatus.ignore;
                alert.updateTime = LocalDateTime.now();
                alert.lastModifier = user.name;
                alert.lastModifierId = user.getId();
                alertRepository.saveAndFlush(alert);
            }
        }
        return Response.ok().build();
    }

    @DeleteMapping("delete")
    //@Operation(description = "删除告警信息")
    @RolesAllowed({"User", "Admin"})
    @Transactional
    public Response delete(@RequestBody Long[] ids){
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }

        for (Long id : ids) {
            Alert alert = alertRepository.findById(id).get();
            alertRepository.delete(alert);
        }

        return Response.ok().build();
    }

}
