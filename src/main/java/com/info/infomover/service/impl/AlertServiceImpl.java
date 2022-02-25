package com.info.infomover.service.impl;

import com.info.infomover.entity.Alert;
import com.info.infomover.entity.QAlert;
import com.info.infomover.repository.AlertRepository;
import com.info.infomover.service.AlertService;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class AlertServiceImpl implements AlertService {


    @Autowired
    private JPAQueryFactory jpaQueryFactory;

    @Autowired
    private AlertRepository alertRepository;

    @Override
    public List<Alert> findByAlert(Alert alert) {
        QAlert alert1 = QAlert.alert;
        JPAQuery<Alert> from = jpaQueryFactory.select(alert1).from(alert1);
        from.where();
        if (alert.name !=null) {
            from.where(alert1.name.eq(alert.name));
        }
        if (StringUtils.isNotBlank(alert.connector)) {
            from.where(alert1.connector.eq(alert.connector));
        }
        if (alert.jobId!=null) {
            from.where(alert1.jobId.eq(alert.jobId));
        }
        if (StringUtils.isNotBlank(alert.job)) {
            from.where(alert1.job.eq(alert.job));
        }
        if (StringUtils.isNotBlank(alert.cluster)) {
            from.where(alert1.cluster.eq(alert.cluster));
        }
        if (StringUtils.isNotBlank(alert.plugin)) {
            from.where(alert1.plugin.eq(alert.plugin));
        }
        if (alert.category!=null) {
            from.where(alert1.category.eq(alert.category));
        }

        if (alert.status!=null) {
            from.where(alert1.status.eq(alert.status));
        }

        if (alert.type!=null) {
            from.where(alert1.type.eq(alert.type));
        }
        return from.fetchResults().getResults();
    }


    @Override
    @Transactional
    public void deleteByJobId(Long jobId) {
        alertRepository.deleteByJobId(jobId);
    }
}
