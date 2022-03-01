package com.info.infomover.recollect;

import com.info.baymax.common.utils.JsonUtils;
import com.info.infomover.entity.Connector;
import com.info.infomover.entity.Job;
import com.info.infomover.repository.JobRepository;
import com.info.infomover.service.JobService;
import com.info.infomover.util.SinkConnectorKeyword;
import com.io.debezium.configserver.model.ConnectConnectorConfigResponse;
import com.io.debezium.configserver.model.ConnectConnectorStatusResponse;
import com.io.debezium.configserver.model.ConnectorStatus;
import com.io.debezium.configserver.rest.client.KafkaConnectClient;
import com.io.debezium.configserver.rest.client.KafkaConnectClientFactory;
import com.io.debezium.configserver.rest.client.KafkaConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ThreadUtils;
import org.jboss.resteasy.client.exception.ResteasyWebApplicationException;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: haijun
 * @Date: 2022/1/30 0030 13:24
 */
@Slf4j
public class RecollectCheckLauncher implements Runnable {
    private JobRepository jobRepository;
    private JobService jobService;
    private long jobId;

    public RecollectCheckLauncher(JobRepository jobRepository, JobService jobService, long jobId) {
        this.jobService = jobService;
        this.jobRepository = jobRepository;
        this.jobId = jobId;
    }

    @Override
    public void run(){

    }

    public void launchCheckJob(){
        try {
            //更新recollecting状态,防止超时更新慢
            ThreadUtils.sleep(Duration.ofSeconds(30L));
        }catch (Exception e){
        }
        long startTime = System.currentTimeMillis();
        log.info("Start to check recollecting jobId {} thread {}, startTime {}......", jobId, Thread.currentThread().getId(), startTime);

        Job byId = null;
        try{
            //检查新connector是否创建成功
            Job job = jobService.findJobByIdAndLoadConnector(jobId);
            if(job != null && job.getRecollectStatus() != Job.RecollectStatus.RECOLLECTING){
                log.info("before first check job {} recollectStatus {}, not is {} and exit.", job.getName(), job.getRecollectStatus(), Job.RecollectStatus.RECOLLECTING);
//                jobRepository.updateJobRecollectStatusByWhere(jobId, Job.RecollectStatus.RECOLLECTTIMEOUT);
                return;
            }
            boolean checkStatus = checkTaskStatus(job, true);
            log.info("check job {} job Id : {} task status response :{}", job.getName(), jobId, checkStatus);
            byId = jobRepository.findById(jobId).get();
            if (checkStatus) {
                if (byId.getDeployStatus() == Job.DeployStatus.PAUSED) {
                    log.info("update job {} deployStatus {} ->{}", byId.getName(), byId.getDeployStatus(), Job.DeployStatus.DEPLOYED);
                    jobService.updateJobDeployStatus(jobId,Job.DeployStatus.DEPLOYED);
                }
                log.info("update job {} recollectStatus {} -> {}", byId.getName(), byId.getRecollectStatus(), Job.RecollectStatus.RECOLLECTSUCCESS);
                jobService.updateJobRecollectStatus(jobId, Job.RecollectStatus.RECOLLECTSUCCESS);
            } else {
                log.info("update job {} recollectStatus {} -> {}", byId.getName(), byId.getRecollectStatus(), Job.RecollectStatus.RECOLLECTTIMEOUT);
                jobService.updateJobRecollectStatusByWhere(jobId, Job.RecollectStatus.RECOLLECTTIMEOUT);
            }
        }catch (Exception e){
            log.info("update job {} recollectStatus {} -> {}", byId.getName(), byId.getRecollectStatus(), Job.RecollectStatus.RECOLLECTFAILED);
            jobService.updateJobRecollectStatusByWhere(jobId, Job.RecollectStatus.RECOLLECTFAILED);
        }finally {
            long endTime = System.currentTimeMillis();
            log.info("End to check recollecting jobId {} thread {}, endTime {} and cost {}ms", jobId, Thread.currentThread().getId(), endTime, endTime-startTime);
        }
    }

    /**
     * 检查 恢复之后 task 是否正常
     * 重复10次,每次休眠30秒
     * 仍然失败需要修改job.recoveryStatus
     * @param job
     * @return
     */
    public boolean checkTaskStatus(Job job, boolean checkSinkConnectorFlag) throws Exception {
        ThreadUtils.sleep(Duration.ofSeconds(10L));
        List<Connector> sourceConnectors = jobService.findSourceConnectors(job);
        List<Connector> sinkConnectors = jobService.findSinkConnectors(job);
        Connector connector = sourceConnectors.get(0);
        log.info("check job {} connector status :{}",job.getName(), connector.name);
        int maxRetries = 10;
        while (maxRetries > 0) {
            if(checkSinkConnectorFlag){//recollect重采
                Job jobById = jobRepository.findById(job.getId()).get();
                if(jobById != null && jobById.getRecollectStatus() != Job.RecollectStatus.RECOLLECTING){
                    return false;
                }
            }
            if (job.getJobType() == Job.JobType.SYNC || (job.getJobType() == Job.JobType.COLLECT && job.getSinkRealType() == Job.SinkRealType.EXTERNAL)) {
                Long deployClusterId = job.getDeployClusterId();
                if (deployClusterId == null) {
                    return false;
                }

                try {
                    boolean sourceRes = false;
                    boolean sinkRes = false;
                    ConnectorStatus connecorStatus = getConnecorStatus(deployClusterId.intValue(), connector.name);
                    long count = connecorStatus.getTaskStates().keySet().stream()
                            .filter(item -> connecorStatus.getTaskState(item).taskStatus == ConnectorStatus.State.RUNNING)
                            .count();
                    if (connecorStatus.getTaskStates().size() > 0 && count == connecorStatus.getTaskStates().size()) {
                        sourceRes = true;
                    }

                    if(checkSinkConnectorFlag){
                        for(Connector conn : sinkConnectors){
                            ConnectorStatus status = getConnecorStatus(deployClusterId.intValue(), conn.name);
                            long count_sink = status.getTaskStates().keySet().stream()
                                    .filter(item -> status.getTaskState(item).taskStatus == ConnectorStatus.State.RUNNING)
                                    .count();
                            if (status.getTaskStates().size() > 0 && count_sink == status.getTaskStates().size()) {
                                sinkRes = true;
                            }
                        }
                    }else{
                        sinkRes = true;
                    }

                    if(sourceRes && sinkRes) {
                        return true;
                    }
                }catch (Exception e) {
                    if (e instanceof WebApplicationException && (((WebApplicationException) e).getResponse().getStatus() == 404 || ((WebApplicationException) e).getResponse().getStatus() == 409)) {
                        log.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                    } else if (e instanceof ResteasyWebApplicationException && (((ResteasyWebApplicationException) e).getResponse().getStatus() == 404 || ((ResteasyWebApplicationException) e).getResponse().getStatus() == 409)){
                        log.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                    } else {
                        log.error("check source&sink connector {} error:", connector.name, e);
                        throw e;
                    }
                }

            } else if (job.getJobType() == Job.JobType.COLLECT) {
                for (Connector sinkConnector : sinkConnectors) {
                    String id = sinkConnector.getConfig().get("id");
                    if (StringUtils.isBlank(id)) {
                        return false;
                    }
                    try {
                        ConnectorStatus connecorStatus = getConnecorStatus(Integer.valueOf(id), connector.name);
                        long count = connecorStatus.getTaskStates().keySet().stream()
                                .filter(item -> connecorStatus.getTaskState(item).taskStatus == ConnectorStatus.State.RUNNING)
                                .count();
                        if (connecorStatus.getTaskStates().size()>0 && count == connecorStatus.getTaskStates().size()) {
                            return true;
                        }
                    }catch (Exception e) {
                        if (e instanceof WebApplicationException && (((WebApplicationException) e).getResponse().getStatus() == 404 || ((WebApplicationException) e).getResponse().getStatus() == 409)) {
                            log.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                        } else if (e instanceof ResteasyWebApplicationException && (((ResteasyWebApplicationException) e).getResponse().getStatus() == 404 || ((ResteasyWebApplicationException) e).getResponse().getStatus() == 409)){
                            log.warn("connector {} maybe has already being deleted from cluster {} .", connector.name, job.getDeployClusterId());
                        } else {
                            log.error("check collect connector {} error:", connector.name, e);
                            throw e;
                        }
                    }
                }
            }
            maxRetries--;
            ThreadUtils.sleep(Duration.ofSeconds(30L));
        }
        return false;
    }

    private ConnectorStatus getConnecorStatus(int cluster, String connectorName) {
        ConnectorStatus connectorState = new ConnectorStatus(connectorName);
        try {
            KafkaConnectClient kafkaConnectClient = KafkaConnectClientFactory.getClient(cluster);
            ConnectConnectorConfigResponse connectorInfo = kafkaConnectClient.getConnectorInfo(connectorName);
            String connectorTypeClass = connectorInfo.getConfig().get("connector.class");
            log.debug("Kafka Connect connector status details: " + connectorInfo);
            String connectorStatus1 = kafkaConnectClient.getConnectorStatus(connectorName);
            ConnectConnectorStatusResponse connectorStatus = JsonUtils.fromJson(connectorStatus1,ConnectConnectorStatusResponse.class);
            connectorState.setConnectorType(connectorTypeClass);
            if(connectorState.getConnectorType().toLowerCase().equals("jdbcsink")){
                String url = connectorInfo.getConfig().get(SinkConnectorKeyword.SINK_URL);
                if(url.contains("jdbc:oracle")) {
                    connectorState.setDatabaseName("Oracle");
                }else if(url.contains("jdbc:mysql")){
                    connectorState.setDatabaseName("MySQL");
                }else if(url.contains("jdbc:postgresql")){
                    connectorState.setDatabaseName("PostgreSQL");
                }
            } else if (connectorState.getConnectorType().toLowerCase().equals("kafkasink")) {
                connectorState.setDatabaseName("Kafka");
            }
            connectorState.setConnectorStatus(connectorStatus.connectorStatus.status);
            connectorState.setDbServerName(connectorInfo.getConfig().get("database.server.name"));
            connectorStatus.taskStates.forEach(
                    taskStatus -> {connectorState.setTaskState(
                            taskStatus.id,
                            taskStatus.status,
                            parserError(taskStatus.getErrorsAsList()));
                        if (!ConnectorStatus.State.RUNNING.equals(taskStatus.status)) {
                            connectorState.setConnectorStatus(taskStatus.status);
                        }
                    });
            return connectorState;
        } catch (IOException | KafkaConnectException e) {
            log.error("getConnecorStatus {} on cluster {}",connectorName, cluster, e);
        }
        return connectorState;
    }

    private List<String> parserError(List<String> errorList){
        if(errorList == null || errorList.size() == 0){
            return null;
        }

        List<String> result = errorList.stream().filter(s -> s.startsWith("Caused by:")).collect(Collectors.toList());
        if(result == null || result.size() == 0){
            String exception = errorList.get(0);
            result = new ArrayList<>();
            result.add(exception);
            return result;
        }else{
            return result;
        }
    }
}
