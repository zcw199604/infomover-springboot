package com.info.infomover.resources;

import com.info.infomover.common.Logged;
import com.info.infomover.common.setting.DataSourceLoader;
import com.info.infomover.common.setting.parser.SettingField;
import com.info.infomover.common.setting.parser.SettingsParser;
import com.info.infomover.datasource.AbstractDataSource;
import com.info.infomover.datasource.DataSourceVerification;
import com.info.infomover.datasource.IDatasource;
import com.info.infomover.entity.*;
import com.info.infomover.repository.DataSourceRepository;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.repository.UserRepository;
import com.info.infomover.resources.response.PageResult;
import com.info.infomover.security.SecurityUtils;
import com.info.infomover.util.AesUtils;
import com.info.infomover.util.DataSourceKeyword;
import com.info.infomover.util.DatasourceUtil;
import com.info.infomover.util.JsonBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequestMapping("/api/datasource")
@Controller
@ResponseBody
@Logged
//@Tag(name = "infomover datasource", description = "DataSource管理接口")
public class DataSourceResource {

    private static final JsonBuilder jsonBuilder = JsonBuilder.getInstance();

    private Page<Job> empty = Page.of(Collections.EMPTY_LIST, 0, 0);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private DataSourceRepository dataSourceRepository;

    @Autowired
    private JPAQueryFactory queryFactory;

    @PostMapping
    @Transactional
    //@Operation(description = "注册数据源")
    @RolesAllowed({"User", "Admin"})
    public Response create(@RequestBody DataSource dataSource) {
        String name = dataSource.name;
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("The name of datasource is empty.");
        }else if(dataSourceRepository.findByName(name) != null){
            throw new IllegalArgumentException("The name of datasource is existed.");
        }
        LocalDateTime localDateTime = LocalDateTime.now();
        dataSource.createTime = localDateTime;
        dataSource.updateTime = localDateTime;
        User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        dataSource.creatorId = user.getId();
        dataSource.creatorChineseName = user.chineseName;
        dataSource.lastModifierId = user.getId();
        dataSource.lastModifierName = user.chineseName;
        if(dataSource.category == null){
            dataSource.category = DataSource.DataSourceCategory.ALL;
        }

        /* ------------------------ check datasource state ------------------------ */
        checkDatasourceState(dataSource);

        dataSourceRepository.saveAndFlush(dataSource);
        return Response.ok(dataSource).build();
    }

    @PutMapping
    @Transactional
    //@Operation(description = "更新数据源")
    @Consumes(MediaType.APPLICATION_JSON)
    @RolesAllowed({"User", "Admin"})
    public Response update(@RequestBody DataSource ds) {
        Long id = ds.getId();
        DataSource dataSource = dataSourceRepository.findById(id).get();
        dataSource.name = ds.name;
        dataSource.description = ds.description;
        dataSource.type = ds.type;
        dataSource.category = ds.category;
        User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        dataSource.lastModifierId = user.getId();
        dataSource.lastModifierName = user.chineseName;
        dataSource.updateTime = LocalDateTime.now();
        dataSource.config = ds.config;

        /* ------------------------ check datasource state ------------------------ */
        checkDatasourceState(dataSource);

        dataSourceRepository.saveAndFlush(dataSource);
        return Response.ok(dataSource).build();
    }

    private void checkDatasourceState(DataSource dataSource){
        try {
            dataSource.state = DataSource.DataSourceState.AVAILABLE;
            IDatasource iDatasource = DatasourceUtil.transform2IDatasource(dataSource);
            List<DataSourceVerification.DataSourceVerificationDetail> dataSourceVerificationDetails = iDatasource.validaConnection();
            for(DataSourceVerification.DataSourceVerificationDetail detail : dataSourceVerificationDetails){
                if(!detail.isSuccess()){
                    dataSource.state = DataSource.DataSourceState.UNAVAILABLE;
                    break;
                }
            }
        } catch (Throwable t) {
            dataSource.state = DataSource.DataSourceState.UNAVAILABLE;
        }
    }

    @GetMapping("/list")
    /*@Operation(description = "数据源列表查询")
    @Parameters({
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
            @Parameter(name = "name", description = "任务名", in = ParameterIn.QUERY),
            @Parameter(name = "type", description = "数据源类型", in = ParameterIn.QUERY),
            @Parameter(name = "creator_name", description = "创建人中文名", in = ParameterIn.QUERY),
            @Parameter(name = "createAfter", description = "创建时间大于", in = ParameterIn.QUERY),
            @Parameter(name = "createBefore", description = "创建时间小于", in = ParameterIn.QUERY),
            @Parameter(name = "modifyAfter", description = "修改时间大于", in = ParameterIn.QUERY),
            @Parameter(name = "modifyBefore", description = "修改时间小于", in = ParameterIn.QUERY),
            @Parameter(name = "sortby", description = "排序字段", in = ParameterIn.QUERY),
            @Parameter(name = "sortdirection", description = "排序规则", in = ParameterIn.QUERY),
            @Parameter(name = "category", description = "数据源类别(1:source,2:sink,3:source and sink)", in = ParameterIn.QUERY),

    })*/
    @RolesAllowed({"User", "Admin"})
    public Response list(
            @RequestParam(value = "pageNum",defaultValue = "1",required = false)  int page,
            @RequestParam(value = "pageSize",defaultValue = "10",required = false)  int limit,
            @RequestParam(value = "name",required = false)  String name,
            @RequestParam(value = "type",required = false)  String type,
            @RequestParam(value = "creator_name",required = false)  String creatorName,
            @RequestParam(value = "createAfter",required = false)  String createAfter, @RequestParam(value = "createBefore",required = false)  String createBefore,
            @RequestParam(value = "modifyAfter",required = false)  String modifyAfter, @RequestParam(value = "modifyBefore",required = false)  String modifyBefore,
            @RequestParam(value = "sortby",defaultValue = "updateTime",required = false)  String sortBy,
            @RequestParam(value = "sortdirection",defaultValue = "DESC",required = false)  Sort.Direction sortDirection,
            @RequestParam(value = "category",required = false)  List<DataSource.DataSourceCategory> category,
            @RequestParam(value = "state",required = false)  DataSource.DataSourceState state) {
        QDataSource dataSource = QDataSource.dataSource;
        JPAQuery<DataSource> from = queryFactory.select(dataSource).from(dataSource);
        User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        /*if (user != null && "admin".equalsIgnoreCase(user.getRole())) {
            if (user == null) {
                throw new RuntimeException("no user found with name " + "DESC");
            }
            from.where(dataSource.creatorId.eq(user.getId()));
        }*/

        if (!StringUtils.isEmpty(name)) {
            from.where(dataSource.name.like("%" + name + "%"));
        }

        if (!StringUtils.isEmpty(type)) {
            from.where(dataSource.type.eq(type));
        }

        if (!StringUtils.isEmpty(creatorName)) {
            from.where(dataSource.creatorChineseName.like("%" + creatorName + "%"));
        }

        if (!StringUtils.isEmpty(createAfter)) {
            from.where(dataSource.createTime.after(LocalDate.parse(createAfter).atStartOfDay()));
        }
        if (!StringUtils.isEmpty(createBefore)) {
            from.where(dataSource.createTime.before(LocalDate.parse(createBefore).atStartOfDay()));
        }

        if (!StringUtils.isEmpty(modifyAfter)) {
            from.where(dataSource.updateTime.after(LocalDate.parse(modifyAfter).atStartOfDay()));
        }
        if (!StringUtils.isEmpty(modifyBefore)) {
            from.where(dataSource.updateTime.before(LocalDate.parse(modifyBefore).atStartOfDay()));
        }

        if (category !=null && !category.isEmpty()) {
            from.where(dataSource.category.in(category));
        }

        if(state != null){
            from.where(dataSource.state.eq(state));
        }
        Order order = sortDirection.isAscending() ? Order.ASC : Order.DESC;
        com.querydsl.core.types.Path<Object> fieldPath = com.querydsl.core.types.dsl.Expressions.path(Object.class, dataSource, sortBy);
        from.orderBy(new OrderSpecifier(order, fieldPath));
        QueryResults<DataSource> dataSourceQueryResults = from.offset(Page.getOffest(page,limit)).limit(limit).fetchResults();


        Page<DataSource> list = Page.of(dataSourceQueryResults.getResults(), dataSourceQueryResults.getTotal(),
                dataSourceQueryResults.getLimit());

        return Response.ok(list).build();
    }

    @Transactional
    @DeleteMapping("/delete")
    //@Operation(description = "删除数据源")
    @RolesAllowed({"User", "Admin"})
    public Response delete( @RequestBody Long[] ids) {
        if (ids == null || ids.length == 0) {
            return Response.status(Response.Status.BAD_REQUEST).entity("ids can't be null or empty").build();
        }
        User user = userRepository.findByName(SecurityUtils.getCurrentUserUsername());
        for (Long id : ids) {
            DataSource dataSource = dataSourceRepository.findById(id.longValue()).get();
            if (user != null && "admin".equalsIgnoreCase(user.getRole())) {
                if (user == null) {
                    throw new RuntimeException("no user found with name " + SecurityUtils.getCurrentUserUsername());
                }
                if (user.getId().longValue() != dataSource.creatorId.longValue()) {
                    throw new RuntimeException("no available datasource for user " + user.chineseName);
                }
            }
            dataSourceRepository.delete(dataSource);
        }
        return Response.ok().build();
    }


    /********************* DataSource元数据相关操作 ****************************/

    @PostMapping("/try")
    //@Operation(description = "测试数据源连接")
    public Response tryToConnectURL(@RequestBody DataSource dataSource) {
        wrappeDaesDecrypt(dataSource);
        IDatasource iDatasource = DatasourceUtil.transform2IDatasource(dataSource);
        List<DataSourceVerification.DataSourceVerificationDetail> dataSourceVerificationDetails = iDatasource.validaConnection();
        return Response.ok(dataSourceVerificationDetails).build();
    }

    /**
     * 数据源的password 需要加密
     * @param dataSource
     */
    private void wrappeDaesDecrypt(DataSource dataSource) {
        if ("kafka".equals(dataSource.type)) {
            if (dataSource.config != null && dataSource.config.containsKey(DataSourceKeyword.PASSWORD)) {
                String s = dataSource.config.get(DataSourceKeyword.PASSWORD);
                String aesDecrypt = AesUtils.wrappeDaesDecrypt(s);
                dataSource.config.put(DataSourceKeyword.PASSWORD, aesDecrypt);
            }
        } else {
            if (dataSource.config != null && dataSource.config.containsKey(DataSourceKeyword.DATABASE_PASSWORD)) {
                String s = dataSource.config.get(DataSourceKeyword.DATABASE_PASSWORD);
                String aesDecrypt = AesUtils.wrappeDaesDecrypt(s);
                dataSource.config.put(DataSourceKeyword.DATABASE_PASSWORD, aesDecrypt);
            }
        }
    }

    @GetMapping("/try/{datasourceId}")
    //@Operation(description = "测试数据源连接")
    public Response tryToConnectURL(  @PathVariable("datasourceId") Long datasourceId) {
        DataSource dataSource = dataSourceRepository.findById(datasourceId).get();
        if (dataSource == null) {
            Map<String, String> err = new HashMap<>(){{
                put("err", "datasourceId is not exist");
            }};
            return Response.ok(err).build();
        }
        wrappeDaesDecrypt(dataSource);
        IDatasource iDatasource = DatasourceUtil.transform2IDatasource(dataSource);
        List<DataSourceVerification.DataSourceVerificationDetail> dataSourceVerificationDetails = iDatasource.validaConnection();

        return Response.ok(dataSourceVerificationDetails).build();
    }




    @GetMapping("/database/query")
    /*@Operation(description = "分页查询数据源数据库名称")
    @Parameters({
            @Parameter(name = "id", description = "DataSource ID", in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response getPagedDatabaseList(
            @NotNull @RequestParam(value = "id",required = false)  Long dataSourceId) {
        DataSource dataSource = dataSourceRepository.findById(dataSourceId).get();
        if (dataSource == null) {
            return Response.ok(empty).build();
        } else {
            IDatasource iDataSource = DatasourceUtil.transform2IDatasource(dataSource);
            if (iDataSource.isJDBCSource()) {
                return Response.ok(iDataSource.listDatabase()).build();
            } else if (iDataSource.isKAFKASource()) {
                return Response.ok(iDataSource.listTopic()).build();
            }
        }
        return Response.ok(empty).build();
    }

    @GetMapping("/table/query")
    /*@Operation(description = "分页查询数据源表")
    @Parameters({
            @Parameter(name = "id", description = "DataSource ID", in = ParameterIn.QUERY),
            @Parameter(name = "database", description = "数据库名称", in = ParameterIn.QUERY),
            @Parameter(name = "name", description = "表名称关键字", in = ParameterIn.QUERY),
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response getPagedTableList(
            @NotNull @RequestParam(value = "id",required = false)  Long dataSourceId,
            @RequestParam(value = "database",required = false)  String database,
            @RequestParam(value = "name",required = false)  String tableKeyword,
            @RequestParam(value = "pageNum",defaultValue = "1",required = false)  int page,
            @RequestParam(value = "pageSize",defaultValue = "10",required = false)  int limit) {
        DataSource dataSource = dataSourceRepository.findById(dataSourceId).get();
        if (dataSource == null) {
            return Response.ok(empty).build();
        } else {
            IDatasource iDataSource = DatasourceUtil.transform2IDatasource(dataSource);
            boolean jdbcSource = iDataSource.isJDBCSource();
            if (jdbcSource) {
                List<String> result = iDataSource.listTable(database);
                if (StringUtils.isNotBlank(tableKeyword)) {
                    result = result.stream().filter(t -> t.contains(tableKeyword)).collect(Collectors.toList());
                }
                return getPagedResponse(page, limit, result);
            } else {
                return Response.ok().build();
            }
        }
    }

    @GetMapping("/column/query")
    /*@Operation(description = "分页查询数据源表字段")
    @Parameters({
            @Parameter(name = "id", description = "DataSource ID", in = ParameterIn.QUERY),
            @Parameter(name = "database", description = "库名", in = ParameterIn.QUERY),
            @Parameter(name = "table", description = "表名", in = ParameterIn.QUERY),
            @Parameter(name = "name", description = "字段名称关键字", in = ParameterIn.QUERY),
            @Parameter(name = "pageNum", description = "从第几页开始", in = ParameterIn.QUERY),
            @Parameter(name = "pageSize", description = "每页列表数目", in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response getPagedTableColumnList(
            @NotNull @RequestParam(value = "id",required = false)  Long dataSourceId,
            @NotNull @RequestParam(value = "database",required = false)  String database,
            @NotNull @RequestParam(value = "table",required = false)  String table,
            @RequestParam(value = "name",required = false)  String columnKeyword,
            @RequestParam(value = "pageNum",defaultValue = "1",required = false)  int page,
            @RequestParam(value = "pageSize",defaultValue = "10",required = false)  int limit) {
        DataSource dataSource = dataSourceRepository.findById(dataSourceId).get();
        if (dataSource == null) {
            return Response.ok(empty).build();
        } else {
            IDatasource iDataSource = DatasourceUtil.transform2IDatasource(dataSource);
            boolean jdbcSource = iDataSource.isJDBCSource();
            if (jdbcSource) {
                List<String> result = iDataSource.listColumn(database, table);
                if (StringUtils.isNotBlank(columnKeyword)) {
                    result = result.stream().filter(c -> c.contains(columnKeyword)).collect(Collectors.toList());
                }
                PageResult<List<String>> pageResult = pagingResult(result, page, limit * 2);
                Page<String> pageColumns = Page.of(
                        pageResult.getPagingResult(),
                        pageResult.getTotalSize() / 2,
                        pageResult.getTotalPage());
                return Response.ok(pageColumns).build();
            } else {
                return Response.ok().build();
            }
        }
    }

    @GetMapping("/topic/describe")
    //@Operation(description = "获取支持的数据源类型")
    public Response kafkaTopicDescribe(
            @NotNull @RequestParam(value = "id",required = false)  Long dataSourceId,
            @NotNull @RequestParam(value = "topic",required = false)  String topic) {
        DataSource dataSource = dataSourceRepository.findById(dataSourceId).get();
        if (dataSource == null) {
            return Response.ok(new HashMap<>()).build();
        }
        IDatasource iDataSource = DatasourceUtil.transform2IDatasource(dataSource);
        if (!iDataSource.isKAFKASource()) {
            return Response.ok(new HashMap<>()).build();
        }
        Map<String, Object> stringObjectMap = iDataSource.topicDescribe(topic);
        return Response.ok(stringObjectMap).build();
    }

    @GetMapping("/types")
    //@Operation(description = "获取支持的数据源类型")
    public Response supportDataSourceType() {
        Set<String> allSupportDataSourceType = DataSourceLoader.getAllSupportDataSourceType();
        return Response.ok(allSupportDataSourceType).build();
    }

    @GetMapping("/settings/{type}")
    //@Operation(description = "获取数据源配置项")
    public Response getDataSourceSettings(@NotNull @PathVariable("type") String type) {
        Class<? extends AbstractDataSource> dataSourceClass = DataSourceLoader.getDataSourceClass(type);
        if (dataSourceClass == null) {
            throw new IllegalArgumentException("Can not found datasource class for type: " + type);
        }
        List<SettingField> settingFields = SettingsParser.extractRtcSettingTypeAsList(dataSourceClass);
        return Response.ok(settingFields).build();
    }

    /**
     * 获取分页返回的Response
     *
     * @param page
     * @param limit
     * @param tableList
     * @return
     */
    private Response getPagedResponse(int page, int limit, List<String> tableList) {
        if (tableList.isEmpty()) {
            return Response.ok(empty).build();
        } else {
            PageResult<List<String>> pageResult = pagingResult(tableList, page, limit);
            Page<String> pageTable = Page.of(
                    pageResult.getPagingResult(),
                    pageResult.getTotalSize(),
                    pageResult.getTotalPage());
            return Response.ok(pageTable).build();
        }
    }

    /**
     * 对list分页处理
     *
     * @param list
     * @param page
     * @param limit
     * @param <T>
     * @return
     */
    private <T> PageResult<List<T>> pagingResult(List<T> list, int page, int limit) {
        int totalSize = 0;
        if (list != null) {
            totalSize = list.size();
        }
        int totalPage = totalSize / limit + 1;
        int offset = (page - 1) * limit;
        int resultSize = offset + limit > totalSize ? totalSize : offset + limit;
        List<T> result = list.subList(offset, resultSize);
        PageResult<List<T>> pageResult = new PageResult();
        pageResult.setTotalPage(totalPage);
        pageResult.setTotalSize(totalSize);
        pageResult.setPagingResult(result);
        return pageResult;
    }

}
