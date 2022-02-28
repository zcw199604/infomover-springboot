package com.info.infomover.resources;

import com.info.infomover.entity.MetricsConf;
import com.info.infomover.entity.MetricsConnector;
import com.info.infomover.entity.QMetricsConf;
import com.info.infomover.entity.QMetricsConnector;
import com.info.infomover.repository.MetricsConfRepository;
import com.info.infomover.repository.MetricsConnectorRepository;
import com.querydsl.core.QueryResults;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: haijun
 * @Date: 2021/10/21 15:17
 */
@RequestMapping("/api/metrics")
@Controller
@ResponseBody
//@Tag(name = "metrics conf", description = "指标显示配置")
public class MetricsConfResource {
    private static Logger logger = LoggerFactory.getLogger(MetricsConfResource.class);

    public static Map<String, MetricsConf> allMetricsConf = null;
    @Autowired
    private MetricsConfRepository metricsConfRepository;

    @Autowired
    private MetricsConnectorRepository metricsConnectorRepository;
    @Autowired
    private JPAQueryFactory queryFactory;

    @GetMapping
    //@Operation(description = "指标配置列表查询")
    @RolesAllowed({"User", "Admin"})
    public Response queryList() {
        if (allMetricsConf == null || allMetricsConf.size() == 0) {
            List<MetricsConf> list = metricsConfRepository.findAll();
            allMetricsConf = new HashMap<>();
            for (MetricsConf conf : list) {
                allMetricsConf.put(conf.connectorType, conf);
            }
        }
        return Response.ok(allMetricsConf).build();
    }

    @Transactional
    @RolesAllowed({"User", "Admin"})
    @PutMapping("/update")
    public Response update(@RequestBody MetricsConf conf) {
        try {
            if (conf.updateTime == null) {
                conf.updateTime = LocalDateTime.now();
            }
            List<MetricsConf> byConnectorType = metricsConfRepository.findByConnectorType(conf.connectorType);
            if (byConnectorType.size() > 0) {
                QMetricsConf metricsConf = QMetricsConf.metricsConf;
                queryFactory.update(metricsConf).set(metricsConf.urlPrefix, conf.urlPrefix)
                        .set(metricsConf.panelIds, conf.panelIds)
                        .set(metricsConf.updateTime, conf.updateTime)
                        .set(metricsConf.trashPanelIds, conf.trashPanelIds)
                        .where(metricsConf.connectorType.eq(conf.connectorType)).execute();

            } else {
                metricsConfRepository.save(conf);
            }

            //更新内存
            if (allMetricsConf != null && allMetricsConf.size() > 0) {
                allMetricsConf.put(conf.connectorType, conf);
            }

            return Response.ok().build();
        } catch (Exception e) {
            logger.error("update error", e);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
    }

    @Transactional
    @DeleteMapping("/delete")
    @RolesAllowed({"User", "Admin"})
    public Response delete(@RequestBody List<Long> ids) {
        for (Long id : ids) {
            try {
                MetricsConf metricsConf = metricsConfRepository.findById(id.longValue()).get();
                metricsConfRepository.delete(metricsConf);
            } catch (Exception e) {
                logger.error("delete error", e);
            }
        }
        return Response.ok().build();
    }


    @GET
    //@Operation(description = "指标配置查询根据connectorType")
    @GetMapping("/{connectorType}")
    /*@Parameters({
            @Parameter(name = "connectorType", description = "connector类型", required = true, in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response queryListByconnectorType( @PathVariable("connectorType") String connectorType) {
        List<MetricsConf> byConnectorType = metricsConfRepository.findByConnectorType(connectorType);
        if (byConnectorType == null) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", "connectorType Not Exist");
            return Response.status(Response.Status.BAD_REQUEST).entity("").build();
        }
        return Response.ok(byConnectorType.get(0)).build();
    }

    @GetMapping("/{connectorType}/{connector}")
    /*@Operation(description = "指标配置列表查询")
    @Path("/{connectorType}/{connector}")
    @Parameters({
            @Parameter(name = "connector", description = "connector", required = true, in = ParameterIn.QUERY),
            @Parameter(name = "connectorType", description = "connectorType", required = true, in = ParameterIn.QUERY),
    })*/
    @RolesAllowed({"User", "Admin"})
    public Response queryListByConnector( @PathVariable("connectorType") String connectorType,
                                         @PathVariable("connector") String connector) {
        //MetricsConf byConnectorType = MetricsRe.findByConnectorType(connectorType);
        QMetricsConnector metricsConnector = QMetricsConnector.metricsConnector;
        QueryResults<MetricsConnector> metricsConnectorQueryResults = queryFactory.select(metricsConnector).from(metricsConnector).where(metricsConnector.connector.eq(connector)).fetchResults();
        if (metricsConnectorQueryResults.getResults().size() == 0) {
            Map<String, Object> map = new HashMap<String, Object>();
            if (allMetricsConf.containsKey(connectorType)) {
                return Response.ok(allMetricsConf.get(connectorType)).build();
            }
            map.put("err", "Connector Not Exist");
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
        return Response.ok(metricsConnectorQueryResults.getResults()).build();
    }

    @Transactional
    @RolesAllowed({"User", "Admin"})
    @PutMapping("/connector/update")
    public Response connectorUpdate(@RequestBody MetricsConnector connector) {
        try {
            connector.updateTime = LocalDateTime.now();
            QMetricsConnector metricsConnector = QMetricsConnector.metricsConnector;
            QueryResults<MetricsConnector> metricsConnectorQueryResults = queryFactory.
                    select(metricsConnector).from(metricsConnector).where(metricsConnector.connector.eq(connector.connector)).fetchResults();

            if (metricsConnectorQueryResults.getResults().size() > 0) {
                long execute = queryFactory.update(metricsConnector).set(metricsConnector.urlPrefix, connector.urlPrefix)
                        .set(metricsConnector.panelIds, connector.panelIds)
                        .set(metricsConnector.updateTime, connector.updateTime)
                        .where(metricsConnector.connector.eq(connector.connector)).execute();
            } else {
                metricsConnectorRepository.saveAndFlush(connector);
            }

            return Response.ok().build();
        } catch (Exception e) {
            logger.error("update error", e);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("err", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(map).build();
        }
    }

    @Transactional
    @RolesAllowed({"User", "Admin"})
    @PutMapping("/connector/delete")
    public Response connectorDelete(@RequestBody List<Long> ids) {
        for (Long id : ids) {
            try {
                MetricsConf metricsConf = metricsConfRepository.findById(id.longValue()).get();
                metricsConfRepository.delete(metricsConf);
            } catch (Exception e) {
                logger.error("delete error", e);
            }
        }
        return Response.ok().build();
    }

}
