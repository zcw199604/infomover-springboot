package com.info.infomover.datasource;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public abstract class AbstractDataSource implements IDatasource {

    public AbstractDataSource(String type) {
        this.type = type;
    }

    private String type;

    @Override
    public boolean isJDBCSource() {
        return false;
    }

    @Override
    public boolean isKAFKASource() {
        return false;
    }

    @Override
    public List<String> listTopic() {
        throw new RuntimeException("UnSupport ");
    }

    @Override
    public Map<String, Object> topicDescribe(String topic) {
        throw new RuntimeException("UnSupport ");
    }

    @Override
    public List<DataSourceVerification.DataSourceVerificationDetail> validaConnection() {
        return new ArrayList<>();
    }
}
