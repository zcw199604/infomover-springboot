package com.info.infomover.resources;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@Controller
@RequestMapping("/api/schemaChange")
@ResponseBody
public class SchemaChangeResource {
/*

    @Inject
    SchemaChangeProcessor schemaChangeProcessor;

    @Inject
    JobRepository jobRepository;

    @Inject
    ConnectorRepository connectorRepository;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    @Autowired
    private UserRepository userRepository;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    */
/**
     * 改到创建任务时指定是否监控元数据变化，并在deploy时开启这个异步线程
     *//*

    @PostMapping("/snapshot/{jobId}")
    //@Operation(description = "数据源快照")
    @RolesAllowed({"User", "Admin"})
    public Response snapshot(@Context SecurityContext ctx, @PathVariable(value = "jobId") Long jobId) {
        Job job = jobRepository.findById(jobId.longValue()).get();
        if (ctx.isUserInRole(User.Role.User.name())) {
            User user = userRepository.findByName(ctx.getUserPrincipal().getName());
            if (user == null) {
                throw new RuntimeException("no user found with name " + ctx.getUserPrincipal().getName());
            }
            if (user.getId().longValue() != job.getCreatorId().longValue()) {
                throw new RuntimeException("no available connector for user " + user.chineseName);
            }
        }
        if (job.isSnapshot()) {
            if (job.getSnapshotStatus() == Job.SnapshotStatus.IN_PROGRESS) {
                throw new IllegalStateException("The job is making a snapshot, please wait");
            }
        } else {
            throw new IllegalStateException("The job snapshot monitor is disabled.");
        }
        String workingDir = schemaChangeProcessor.getWorkingDir();
        job.getConnectors().size();//load connectors
        // TODO quarkus -> springboot 暂时不修改scheamchange内容
        */
/*LiquibaseSnapshotLauncher snapshotLauncher =
                new LiquibaseSnapshotLauncher(job, workingDir, jobRepository, connectorRepository);
        executorService.execute(snapshotLauncher);*//*

        return Response.ok("Submitted").build();
    }

    @GetMapping
    */
/*@Operation(description = "Schema变动列表")
    @Parameters({
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
            @Parameter(name = "jobId", description = "任务ID", in = ParameterIn.QUERY),
            @Parameter(name = "connectorName", description = "连接器名称", in = ParameterIn.QUERY),
            @Parameter(name = "sortby", description = "排序字段", in = ParameterIn.QUERY),
            @Parameter(name = "sortdirection", description = "排序方向", in = ParameterIn.QUERY)
    })*//*

    @RolesAllowed({"User", "Admin"})
    public Response list(@Context SecurityContext ctx,
                         @DefaultValue("1") @RequestParam("pageNum") int page,
                         @DefaultValue("10") @RequestParam("pageSize") int limit,
                         @RequestParam("jobId") Long jobId,
                         @RequestParam("connectorName") String connectorName,
                         @DefaultValue("createTime") @RequestParam("sortby") String sortBy,
                         @DefaultValue("DESC") @RequestParam("sortdirection") Sort.Direction sortDirection) {
        List<String> query = new ArrayList<>();
        Map<String, Object> params = new HashMap<>();
        QSchemaChange schemaChange = QSchemaChange.schemaChange;
        JPAQuery<SchemaChange> from = queryFactory.select(schemaChange).from(schemaChange);
        if (jobId != null) {
            from.where(schemaChange.jobId.eq(jobId));
        }

        */
/*if (StringUtils.isNotBlank(connectorName)) {
            from.where(schemaChange.connect)
            query.add("connector like :connectorName");
            params.put("connectorName", "%" + connectorName + "%");
        }*//*

        if (sortDirection.isAscending()) {
            from.orderBy(schemaChange.createTime.asc());
        } else if (sortDirection.isDescending()) {
            from.orderBy(schemaChange.createTime.desc());
        }
        QueryResults<SchemaChange> schemaChangeQueryResults = from.offset(page - 1).limit(limit).fetchResults();
        String queryStr = query.stream().collect(Collectors.joining(" AND "));
        PanacheQuery<SchemaChange> pq = SchemaChange.find(queryStr, Sort.by(sortBy, sortDirection), params)
                .page(page - 1, limit);

        Page<SchemaChange> list = Page.of(schemaChangeQueryResults.getResults(), schemaChangeQueryResults.getTotal(), pq.pageCount());
        return Response.ok(list).build();
    }

    @PostMapping("/{schemaChangeId}/apply")
    @Transactional
    @RolesAllowed({"User", "Admin"})
    public Response apply(@Context SecurityContext ctx,
                          @PathVariable("schemaChangeId") long schemaChangeId,
                          @RequestBody SchemaChange schemaChange) {
        if (schemaChange == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        String changeLogDiffFile = schemaChange.changeLogDiffFile;
        String sourceTable = schemaChange.sourceTable;
        String sinkConnectorName = schemaChange.sinkConnector;
        String sinkTable = schemaChange.sinkTable;
        Connector sinkConnector = Connector.findByName(sinkConnectorName);
        Map<String, String> sinkConfigs = sinkConnector.config;
        String targetUrl = sinkConfigs.get(SINK_URL);
        String targetUser = sinkConfigs.get(SINK_USER);
        String targetPasswd = sinkConfigs.get(SINK_PASSWD);
        String workingDir = schemaChangeProcessor.getWorkingDir();

        */
/* replace sink table name *//*

        SchemaChangeUtil.replaceTableName(workingDir, changeLogDiffFile, sourceTable, sinkTable);

        int updated = LiquibaseRunner.update(changeLogDiffFile, targetUrl, targetUser, targetPasswd, workingDir);
        List<ActionStatus> response = new ArrayList<>();
        if (updated == 0) {
            response.add(new ActionStatus("update", "completed"));
            Long jobId = schemaChange.jobId;
            LocalDateTime createTime = schemaChange.createTime;
            //TODO 找出previous schemaChange并设置成apply状态
            List<SchemaChange> pq = SchemaChange.find("jobId=?1 and createTime<=?2", jobId, createTime).list();
            pq.stream().forEach(sc -> {
                sc.applied = true;
                sc.persistAndFlush();
            });
            Job job = Job.findById(jobId);
            job.schemaChangedCount = 0;
            job.snapshotStatus = Job.SnapshotStatus.IN_PROGRESS;
            job.persistAndFlush();
            */
/*
             * snapshot
             * *//*

            job.connectors.size();//由于connectors是懒加载，所以调用sie()以保证connectors不为空
            LiquibaseSnapshotLauncher snapshotLauncher =
                    new LiquibaseSnapshotLauncher(job, workingDir, jobRepository, connectorRepository);
            executorService.execute(snapshotLauncher);
        } else {
            response.add(new ActionStatus("update", "failed"));
        }
        return Response.ok(response).build();
    }

    @DELETE
    @Transactional
    @Path("/{schemaChangeId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @RolesAllowed({"User", "Admin"})
    public Response delete(@PathParam("schemaChangeId") long schemaChangeId) {
        boolean delete = SchemaChange.deleteById(schemaChangeId);
        return delete ? Response.ok().build() : Response.noContent().build();
    }

*/

}
