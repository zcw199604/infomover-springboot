package com.info.infomover.resources;

import com.info.infomover.param.DownloadLogParam;
import com.info.infomover.resources.response.ConnectorLog;
import com.info.infomover.util.CheckUtil;
import com.info.infomover.util.JsonBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * create by pengchuan.chen on 2021/11/10
 */
@RequestMapping("/api/logs")
@Controller
@ResponseBody
//@Tag(name = "infomover log", description = "connector 日志管理接口")
public class LogResource {

    private static Logger logger = LoggerFactory.getLogger(LogResource.class);

    @Value("${connector.log.index:connector_log}")
    private String logIndex;

    @Value("${connector.log.index-date-format:yyyy-ww}")
    private String indexDateFormat;

    @Value("${connector.log.download.path}")
    private String logDownloadPath;

    @Value("${connector.log.tags-max-count:100}")
    private int tagMaxCount;

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @GET
    @GetMapping("/{connectorName}")
    //@Operation(description = "根据connector名称查询对应日志")
    @RolesAllowed({"User", "Admin"})
    public Response findConnectorLog(@PathVariable("connectorName") String connectorName,
                                     @RequestParam(value = "offset",defaultValue = "0",required = false)  int offset,
                                     @RequestParam(value = "limit",required = false)  int limit,
                                     @RequestParam(value = "level",required = false)  String level,
                                     @RequestParam(value = "searchWord",required = false)  String searchWord,
                                     @RequestParam(value = "type",defaultValue = "-1",required = false)  int type,
                                     @RequestParam(value = "startTime",required = false)  Long startTime,
                                     @RequestParam(value = "endTime",required = false)  Long endTime,
                                     @RequestParam(value = "sort",required = false)  SortOrder sort) throws IOException {

        if (StringUtils.isEmpty(level)) {
            return Response.serverError().status(Response.Status.BAD_REQUEST)
                    .entity("level can't be null or empty.").build();
        }
        if (sort == null) {
            sort = SortOrder.DESC;
            logger.warn("sort is null or empty,and used default value {}.", sort);
        }
        String[] connectorLogIndexs = getLogsIndex();
        ConnectorLog connectorLog = this.search(connectorLogIndexs, connectorName, level, offset, limit, startTime, endTime, searchWord, type, sort);
        connectorLog.setContent(parserHitsToConnectorLog(connectorLog.getTmp()));
        connectorLog.setTmp(null);
        return Response.ok().entity(connectorLog).build();
    }

    @PostMapping("/download/{connectorName}")
    //@Operation(description = "根据connector名称查询对应日志")
    @RolesAllowed({"User", "Admin"})
    public Response downloadConnectLog(@PathVariable("connectorName") String connectorName, @RequestBody DownloadLogParam downloadLogParam) throws IOException {


        CheckUtil.checkTrue(downloadLogParam.getStartTime() == null, "start time is empty");
        CheckUtil.checkTrue(downloadLogParam.getEndTime() == null, "end time is empty");
        CheckUtil.checkTrue(StringUtils.isEmpty(logDownloadPath), "connector.log.download.path is empty,Please check the configuration file");

        if (StringUtils.isEmpty(downloadLogParam.getLevel())) {
            downloadLogParam.setLevel("all");
        }
        if (downloadLogParam.getLimit() == 0) {
            downloadLogParam.setLimit(-1);
        }

        if (downloadLogParam.getSort() == null) {
            downloadLogParam.setSort(SortOrder.DESC);
            logger.warn("sort is null or empty,and used default value {}.", downloadLogParam.getSort());
        }
        String[] connectorLogIndexs = getLogsIndex();

        ConnectorLog connectorLog = this.search(connectorLogIndexs, connectorName, downloadLogParam.getLevel(), downloadLogParam.getOffset(),
                downloadLogParam.getLimit(), downloadLogParam.getStartTime(), downloadLogParam.getEndTime(), downloadLogParam.getSearchWord(),
                downloadLogParam.getType(), downloadLogParam.getSort());

        List<SearchHit> tmp = connectorLog.getTmp();

        File dir = null;
        try {
            dir = new File(logDownloadPath + File.separator + "tmpFile");
            if (!dir.exists()) {
                boolean mkdir = dir.mkdirs();
                if (!mkdir) {
                    throw new FileSystemException("directory creation failed, please check the configuration or whether you have permission");
                }
            }
        } catch (FileSystemException e) {
            logger.error("directory creation failed, please check the configuration or whether you have permission");
            throw e;
        } catch (Exception e) {
            logger.error("connect log download error ,msg :{} ", e.getMessage(), e);
            throw e;
        }


        String fileName = connectorName + System.currentTimeMillis();
        String suffix = ".zip";
        String filePath = dir.getPath() + File.separator + fileName;
        File file = new File(filePath + suffix);
        if (!file.exists())
            file.createNewFile();
        Iterator<SearchHit> iterator = tmp.iterator();

        ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(file));

        zipOut.putNextEntry(new ZipEntry(System.currentTimeMillis() + ".txt"));
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("Asia/Shanghai"));

        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            ZonedDateTime parse = ZonedDateTime.parse(sourceAsMap.get("@timestamp").toString());
            stringBuilder.append(timeFormatter.format(parse)).append(" ");
            stringBuilder.append(sourceAsMap.get("level")).append(" ");
            stringBuilder.append(sourceAsMap.get("message")).append(" ");
            if (sourceAsMap.get("exception") != null) {
                Map exception = (Map) sourceAsMap.get("exception");
                stringBuilder.append(" ").append(exception.getOrDefault("stacktrace", "").toString());
            }
            stringBuilder.append("\n");
        }

        ByteArrayInputStream input = new ByteArrayInputStream(stringBuilder.toString().getBytes());
        int temp = 0;
        //读取相关的文件
        while ((temp = input.read()) != -1) {
            //写入输出流中
            zipOut.write(temp);
        }
        //关闭流
        input.close();
        zipOut.close();
        File downLoadFile = new File(filePath + ".zip");
        String encodeFileName = URLEncoder.encode(downLoadFile.getName(), StandardCharsets.UTF_8);
        return Response.ok(downLoadFile)
                .header("content-disposition", "attachment; filename=" + encodeFileName)
                .header("Content-Length", downLoadFile.length())
                .build();
    }

    public static void main(String[] args) {
        ZonedDateTime parse = ZonedDateTime.parse("2022-01-04T06:51:18.683Z");
        System.out.println(parse.toLocalDateTime().toString());
        DateTimeFormatter utc = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneId.of("Asia/Shanghai"));
        String format = utc.format(parse);
        System.out.println(format);
    }

    private ConnectorLog search(String[] connectorLogIndexs, String connectorName, String level, int offset, int limit, Long startTime, Long endTime, String searchWord, int type, SortOrder sortOrder) throws IOException {
        ConnectorLog connectorLog = new ConnectorLog();
        List<ConnectorLog.LogEntity> logEntities = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(connectorLogIndexs);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("connector_name.keyword", connectorName));
        if (level != null && !"all".equalsIgnoreCase(level)) {
            boolQueryBuilder.must(QueryBuilders.termQuery("level.keyword", level));
        }

        if (startTime != null || endTime != null) {
            // 判断时间区间
            if (startTime != null && endTime != null && (endTime - startTime) > 604800000L) {
                throw new RuntimeException("Download logs within seven days at most.");
            }

            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp");
            if (startTime != null) {
                Date startDate = new Date();
                startDate.setTime(startTime);
                rangeQueryBuilder.gt(startDate);
            }
            if (endTime != null) {
                Date endDate = new Date();
                endDate.setTime(endTime);
                rangeQueryBuilder.lt(endDate);
            }

            boolQueryBuilder.must(rangeQueryBuilder);
        }

        // type:1 装载日志
        // type:2 运行日志
        if (type == 1) {
            boolQueryBuilder.must(QueryBuilders.termsQuery("logger_name.keyword", getConnectLogFile()));
        } else if (type == 2) {
            boolQueryBuilder.mustNot(QueryBuilders.termsQuery("logger_name.keyword", getConnectLogFile()));
        }

        if (StringUtils.isNotBlank(searchWord)) {
            // error 级别错误信息在 exception.stacktrace
            // 可能是分词问题导致搜索不到
            boolQueryBuilder.filter(QueryBuilders.boolQuery().should(QueryBuilders.matchPhraseQuery("message", searchWord))
                    //.should(QueryBuilders.wildcardQuery("message", "*" + searchWord + "*"))
                    .should(QueryBuilders.wildcardQuery("message.keyword", "*" + searchWord + "*"))
                    //.should(QueryBuilders.matchPhraseQuery("exception.stacktrace.keyword", searchWord).slop(1).zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL))
                    .should(QueryBuilders.matchPhraseQuery("exception.stacktrace", searchWord))
                    //.should(QueryBuilders.wildcardQuery("exception.stacktrace", "*" + searchWord + "*"))
                    .should(QueryBuilders.wildcardQuery("exception.stacktrace.keyword", "*" + searchWord + "*")).minimumShouldMatch(1))
            ;
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(boolQueryBuilder)
                .sort("@timestamp", sortOrder).trackTotalHits(true);
        if (offset >= 0 && limit >= 0) {//todo 分页查询
            searchSourceBuilder.from(offset).size(limit);
            logger.info("dsl:" + searchSourceBuilder.toString());
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.status() == RestStatus.OK) {
                SearchHit[] hits = searchResponse.getHits().getHits();
                long value = searchResponse.getHits().getTotalHits().value;
                connectorLog.setTotalCount(value);
                //logEntities = parserHitsToConnectorLog(Arrays.asList(hits));
                connectorLog.setTmp(Arrays.asList(hits));
            } else {
                throw new RuntimeException(searchResponse.toString());
            }
        } else { //TODO 查询所有的使用scroll查询.
            //失效时间为5min
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(5));
            //封存快照
            searchRequest.scroll(scroll);
            searchSourceBuilder.size(100);
            logger.info("dsl:" + searchSourceBuilder.toString());
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.status() == RestStatus.OK) {
                String scrollId = searchResponse.getScrollId();
                List<SearchHit> searchHitList = new LinkedList<>();
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                while (ArrayUtils.isNotEmpty(searchHits)) {
                    for (SearchHit hit : searchHits) {
                        searchHitList.add(hit);
                    }
                    SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId)
                            .scroll(scroll);
                    SearchResponse searchScrollResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                    searchHits = searchScrollResponse.getHits().getHits();
                }
                connectorLog.setTmp(searchHitList);
                //logEntities = parserHitsToConnectorLog(searchHitList);
                connectorLog.setTotalCount(Long.valueOf(logEntities.size()));
                //todo 及时清除es快照，释放资源
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            } else {
                throw new RuntimeException(searchResponse.toString());
            }
        }

        connectorLog.setContent(logEntities);
        return connectorLog;
    }

    /**
     * 将查询到的SearchHit转换成ConnectorLog.LogEntity
     */
    private List<ConnectorLog.LogEntity> parserHitsToConnectorLog(Collection<SearchHit> hits) {
        List<ConnectorLog.LogEntity> list = new LinkedList<>();
        Iterator<SearchHit> it1 = hits.iterator();
        while (it1.hasNext()) {
            SearchHit hit = it1.next();
            Map<String, Object> map = hit.getSourceAsMap();
            map.put("id", hit.getId());
            map.put("timestamp", DateTime.parse(map.get("@timestamp").toString()).getMillis());
            ConnectorLog.LogEntity logEntity = JsonBuilder.getInstance().fromObject(map, ConnectorLog.LogEntity.class);
            list.add(logEntity);
        }
        return list;
    }

    /**
     * 通过聚合查询统计不同level下有多少数据量
     */
    private Map<String, Long> groupCountByLevel(String[] indexs, String connectorName) throws IOException {
        Map<String, Long> aggMap = new HashMap<>();
        SearchRequest aggRequest = new SearchRequest(indexs);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("connector_name.keyword", connectorName));
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                .query(queryBuilder)
                .aggregation(AggregationBuilders.terms("group_name")
                        .field("level.keyword")
                        .size(tagMaxCount));
        aggRequest.source(searchSourceBuilder);
        logger.debug("dsl:" + searchSourceBuilder.toString());
        SearchResponse searchResponse = restHighLevelClient.search(aggRequest, RequestOptions.DEFAULT);
        if (searchResponse.status() == RestStatus.OK) {
            Aggregation aggregation = searchResponse.getAggregations().getAsMap().get("group_name");
            ParsedStringTerms parsedStringTerms = (ParsedStringTerms) aggregation;
            long totalCount = 0L;
            for (Terms.Bucket bucket : parsedStringTerms.getBuckets()) {
                long docCount = bucket.getDocCount();
                totalCount += docCount;
                aggMap.put(bucket.getKeyAsString(), docCount);
            }
            aggMap.put("ALL", totalCount);
        }
        return aggMap;
    }

    // TODO 获取log4j2 index:connector_log-yyyy-MM
    private String[] getLogsIndex() {
        List<String> listIndex = new ArrayList<>();
        try {
            if (StringUtils.isNotEmpty(indexDateFormat)) {
                DateTime dateTime = DateTime.now();
                // dateTime = dateTime.plusDays(1);// 提个余量避免logstash和标准日期周第一天起始时间不同的问题
                String index = logIndex + "-" + dateTime.toString(indexDateFormat);
                do {
                    listIndex.add(index);

                    if ("yyyy-MM".equals(indexDateFormat) || "yyyyMM".equals(indexDateFormat)) {
                        dateTime = dateTime.minusMonths(1);
                    } else if ("yyyy-ww".equals(indexDateFormat.toLowerCase())
                            || "yyyyww".equals(indexDateFormat.toLowerCase())) {
                        dateTime = dateTime.minusWeeks(1);
                    }
                    index = logIndex + "-" + dateTime.toString(indexDateFormat);
                } while (restHighLevelClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
            } else {
                listIndex.add(logIndex);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        String[] indexs = new String[listIndex.size()];
        return listIndex.toArray(indexs);
    }


    private List<String> getConnectLogFile() {
        List<String> list = new ArrayList<>();
        list.add("org.apache.kafka.clients.consumer.ConsumerConfig");
        list.add("org.apache.kafka.clients.admin.AdminClientConfig");
        list.add("org.apache.kafka.connect.json.JsonConverterConfig");
        list.add("org.apache.kafka.connect.runtime.ConnectorConfig");
        list.add("org.apache.kafka.clients.producer.ProducerConfig");
        list.add("org.apache.kafka.connect.runtime.SourceConnectorConfig");
        list.add("org.apache.kafka.connect.runtime.TaskConfig");
        list.add("org.apache.kafka.connect.runtime.ConnectorConfig$EnrichedConnectorConfig");

        return list;
    }

}
