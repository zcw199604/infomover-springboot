package com.info.infomover.service;

import com.info.infomover.entity.Alert;

import java.util.List;

public interface AlertService {
    List<Alert> findByAlert(Alert alert);

    public void deleteByJobId(Long jobId);

}
