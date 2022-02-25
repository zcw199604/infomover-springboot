package com.info.infomover.prom.response;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class PromResultConverter {
    
    public static void convertMatrix(PromResult.CommonData data, Map<Long, Map<String, Object>> target, Map<String, String> metricConnectorMap) {
        if ("matrix".equals(data.getResultType())) {
            List<PromResult.Result> resultList = data.getResult();
            for (PromResult.Result result : resultList) {
                PromResult.Metric metric = result.getMetric();
                String key = metricConnectorMap.get(metric.getName());
                
                List<List<Object>> valueList = result.getValues();
                for (List<Object> value : valueList) {
                    Object timeStamp = value.get(0);
                    long time;
                    if (timeStamp instanceof BigDecimal) {
                        time = ((BigDecimal) timeStamp).longValue();
                    } else {
                        time = Long.parseLong((String) timeStamp);
                    }
                    String timeVal = (String) value.get(1);
                    Map<String, Object> keyMap = target.computeIfAbsent(time, s -> new HashMap<>());
                    
                    keyMap.putIfAbsent("time", time * 1000);
                    keyMap.putIfAbsent(key, (int) Double.parseDouble(timeVal));
                }
            }
        }
        
    }
}
