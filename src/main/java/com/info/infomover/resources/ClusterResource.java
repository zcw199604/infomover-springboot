package com.info.infomover.resources;

import com.info.infomover.entity.*;
import com.info.infomover.loadbalancer.LoadBalancerFactory;
import com.info.infomover.repository.ClusterRepository;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.service.JobService;
import com.info.infomover.util.UserUtil;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.stream.Collectors;

@RequestMapping("/api/cluster")
@ResponseBody
@Controller
public class ClusterResource {

    private static Logger logger = LoggerFactory.getLogger(ClusterResource.class);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private JPAQueryFactory queryFactory;



    @PostMapping
    @Transactional
    //@Operation(description = "集群注册")
    @RolesAllowed({"User", "Admin"})
    public Response register(@RequestBody Cluster cluster) throws Exception {
        if (StringUtils.isEmpty(cluster.name) || StringUtils.isEmpty(cluster.connectorUrl)) {
            return Response.status(Response.Status.BAD_REQUEST).entity("connectorUrl: can't be null or empty.").build();
        }

        if (clusterRepository.countByName(cluster.name) > 0) {
            throw new RuntimeException("cluster name " + cluster.name + " is existed");
        }
        String[] connectorUrls = cluster.connectorUrl.split(",");
        for (String url : connectorUrls) {
            if (!url.startsWith("http://")) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("connectorUrl: format should like http://192.168.1.10:8083,http://192.168.1.11:8083")
                        .build();
            }
        }
        //校验sasl参数
        validateSasl(cluster);

        User user = userRepository.findByName(UserUtil.getUserName());
        cluster.setId(null);
        cluster.creatorId = user.getId();
        cluster.creatorChineseName = user.chineseName;
        cluster.lastModifier = user.chineseName;
        cluster.createTime = LocalDateTime.now();
        cluster.updateTime = LocalDateTime.now();
        cluster.enabled = 1;
        clusterRepository.save(cluster);
        return Response.ok(cluster).build();
    }

    @PutMapping
    @Transactional
    //@Operation(description = "集群信息更新")
    @RolesAllowed({"User", "Admin"})
    public Response update(@RequestBody Cluster cluster) {
        if (StringUtils.isEmpty(cluster.name) || StringUtils.isEmpty(cluster.connectorUrl)) {
            return Response.status(Response.Status.BAD_REQUEST).entity("connectorUrl: can't be null or empty.").build();
        }
        String[] connectorUrls = cluster.connectorUrl.split(",");
        for (String url : connectorUrls) {
            if (!url.startsWith("http://")) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("connectorUrl: format should like http://192.168.1.10:8083,http://192.168.1.11:8083")
                        .build();
            }
        }
        User user = userRepository.findByName(UserUtil.getUserName());
        Cluster oldCluster = clusterRepository.findById(cluster.getId()).get();
        if (oldCluster == null) {
            throw new IllegalArgumentException("can't find cluster by id: " + cluster.getId());
        }
        if (User.Role.User.name().equals(user.getRole()) && (user.getId().longValue() != cluster.creatorId.longValue())) {
            throw new RuntimeException("no available cluster for user " + user.chineseName);
        }
        //校验sasl参数
        validateSasl(cluster);

        oldCluster.name = cluster.name;
        oldCluster.connectorUrl = cluster.connectorUrl;
        oldCluster.brokerList = cluster.brokerList;
        oldCluster.securityProtocol = cluster.securityProtocol;
        oldCluster.saslMechanism = cluster.saslMechanism;
        oldCluster.username = cluster.username;
        oldCluster.password = cluster.password;
        oldCluster.serviceName = cluster.serviceName;
        oldCluster.loginModule = cluster.loginModule;
        oldCluster.note = cluster.note;
        oldCluster.lastModifier = user.chineseName;
        oldCluster.updateTime = LocalDateTime.now();
        clusterRepository.saveAndFlush(oldCluster);
        return Response.ok(cluster).build();
    }

    private void validateSasl(Cluster cluster){
        if("SASL_PLAINTEXT".equals(cluster.securityProtocol) && StringUtils.isNotBlank(cluster.saslMechanism)){
            if(StringUtils.isBlank(cluster.username) || StringUtils.isBlank(cluster.password)){
                throw new IllegalArgumentException("username and password cannot be empty when access");
            }
            if(StringUtils.isBlank(cluster.loginModule)){
                throw new IllegalArgumentException("LoginModule cannot be empty");
            }
            if(StringUtils.isBlank(cluster.serviceName)){
                throw new IllegalArgumentException("please choose serviceName for client access");
            }
        }
    }

    @GetMapping("/list")
    /*@Operation(description = "集群列表查询")
    @Parameters({
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
            @Parameter(name = "name", description = "任务名", in = ParameterIn.QUERY),
            @Parameter(name = "creator_name", description = "创建人中文名", in = ParameterIn.QUERY),
            @Parameter(name = "after", description = "创建时间大于", in = ParameterIn.QUERY),
            @Parameter(name = "before", description = "创建时间小于", in = ParameterIn.QUERY),
            @Parameter(name = "enabled", description = "是否启用", in = ParameterIn.QUERY),
            @Parameter(name = "sortby", description = "排序字段", in = ParameterIn.QUERY),
            @Parameter(name = "sortdirection", description = "排序规则", in = ParameterIn.QUERY)
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response list(@RequestParam(value = "pageNum",defaultValue = "1",required = false) int page, @RequestParam(value = "pageSize",defaultValue = "10",required = false) int limit,
                         @RequestParam(value = "name",required = false) String name,
                         @RequestParam(value = "creator_name", required = false) String creatorName,
                         @RequestParam(value = "after",required = false) String updateAfter, @RequestParam(value = "before",required = false) String updateBefore,
                         @RequestParam(value = "sortby",defaultValue = "updateTime",required = false) String sortBy,
                         @RequestParam(value = "sortdirection",defaultValue = "DESC",required = false) Sort.Direction sortDirection, @RequestParam(value = "enabled",required = false) Integer enabled) {


        QCluster cluster = QCluster.cluster;
        JPAQuery<Cluster> from = queryFactory.select(cluster).from(cluster);
        User user = userRepository.findByName(UserUtil.getUserName());
        if (User.Role.User.name().equals(user.getRole())) {
            if (user == null) {
                throw new RuntimeException("no user found with name " + UserUtil.getUserName());
            }
            from.where(cluster.creatorId.eq(user.getId()));
        }
        if (!StringUtils.isEmpty(name)) {
            from.where(cluster.name.like("%" + name + "%"));
        }

        if (!StringUtils.isEmpty(creatorName)) {
            from.where(cluster.name.like("%" + creatorName + "%"));
        }

        if (!StringUtils.isEmpty(updateAfter)) {
            from.where(cluster.updateTime.after(LocalDate.parse(updateAfter).atStartOfDay()));
        }
        if (!StringUtils.isEmpty(updateBefore)) {
            from.where(cluster.updateTime.before(LocalDate.parse(updateBefore).atStartOfDay()));
        }

        if (enabled != null) {
            from.where(cluster.enabled.eq(enabled));
        }

        Order order = sortDirection.isAscending() ? Order.ASC : Order.DESC;
        com.querydsl.core.types.Path<Object> fieldPath = com.querydsl.core.types.dsl.Expressions.path(Object.class, cluster, sortBy);
        from.orderBy(new OrderSpecifier(order, fieldPath));
        QueryResults<Cluster> alertRuleQueryResults = from.offset(page - 1).limit(limit).fetchResults();


        Page<Cluster> list = Page.of(alertRuleQueryResults.getResults(), alertRuleQueryResults.getTotal(),
                alertRuleQueryResults.getLimit());
        list.content.forEach(c -> {
            c.connectDistribute = LoadBalancerFactory.getConnectDistributeForCluster(c.getId());
        });
        return Response.ok(list).build();
    }

    @PostMapping("/enable")
    @Transactional
    @RolesAllowed({"User", "Admin"})
    //@Operation(description = "启用集群")
    public Response enable(@RequestBody Long[] ids) {
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }
        for (Long id : ids) {
            Cluster cluster = clusterRepository.findById(id.longValue()).get();
            if (User.Role.User.name().equals(user.getRole())) {
                if (user.getId().longValue() != cluster.creatorId.longValue()) {
                    throw new RuntimeException("no available cluster for user " + user.chineseName);
                }
            }
            if (cluster.enabled != 1) {
                cluster.enabled = 1;
                clusterRepository.saveAndFlush(cluster);
            }
        }
        return Response.ok().build();
    }

    @PostMapping("/disable")
    @Transactional
    //@Operation(description = "停用集群")
    @RolesAllowed({"User", "Admin"})
    public Response disable(@RequestBody Long[] ids) {
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        User user = userRepository.findByName(UserUtil.getUserName());
        if (user == null) {
            throw new RuntimeException("no user found with name " + UserUtil.getUserName());
        }
        for (Long id : ids) {
            Cluster cluster = clusterRepository.findById(id.longValue()).get();
            if (User.Role.User.name().equals(user.getRole())) {
                if (user.getId().longValue() != cluster.creatorId.longValue()) {
                    throw new RuntimeException("no available cluster for user " + user.chineseName);
                }
            }
            if (cluster.enabled == 1) {
                cluster.enabled = 0;
                clusterRepository.saveAndFlush(cluster);
            }
        }
        return Response.ok().build();
    }

    @Transactional
    @DeleteMapping("/delete")
    //@Operation(description = "删除集群")
    //@Consumes(MediaType.APPLICATION_JSON)
    @RolesAllowed({"User", "Admin"})
    public Response delete( @RequestBody Long[] ids) {
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        List<Job> byClusterIds = jobService.findByClusterIds(ids);

        if (CollectionUtils.isNotEmpty(byClusterIds)) {
            Map<String, List<String>> clusterAndJobName = new HashMap<>();
            Map<String, List<Job>> collect = byClusterIds.stream().collect(Collectors.groupingBy(Job::getDeployCluster));
            for (String item : collect.keySet()) {
                clusterAndJobName.put(item, collect.get(item).stream().map(itemTmp -> itemTmp.getName()).collect(Collectors.toList()));
            }
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(clusterAndJobName).build();
        }

        User user = userRepository.findByName(UserUtil.getUserName());
        for (Long id : ids) {
            Cluster cluster = clusterRepository.findById(id.longValue()).get();
            if (user.getRole().equals(User.Role.User.name().equals(user.getRole()))) {
                if (user == null) {
                    throw new RuntimeException("no user found with name " + UserUtil.getUserName());
                }
                if (user.getId().longValue() != cluster.creatorId.longValue()) {
                    throw new RuntimeException("no available cluster for user " + user.chineseName);
                }
            }
            clusterRepository.delete(cluster);
        }
        return Response.ok().build();
    }

}
