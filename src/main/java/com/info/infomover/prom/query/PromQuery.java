package com.info.infomover.prom.query;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PromQuery implements Serializable {
    
    public String metric;
    
    public List<PromFactor> factors = new ArrayList<>();
    
    public String method;
    
    public Integer step;
    
    public String duration;
    
    public String operator;
    
    public Double threshold;
    
    public String build() {
        if (StringUtils.isBlank(metric)) {
            throw new RuntimeException("metric can not be blank");
        }
        
        StringBuilder queryBuilder = new StringBuilder();
        if (StringUtils.isBlank(method)) {
            buildMetric(queryBuilder);
        } else {
            queryBuilder = new StringBuilder(method);
            queryBuilder.append("(");
            buildMetric(queryBuilder);
            if (step != null && step > 0) {
                queryBuilder.append("[").append(step).append(duration).append("]");
            }
            queryBuilder.append(")");
        }
        
        if (StringUtils.isNotBlank(operator) && threshold!=null) {
            queryBuilder.append(operator).append(threshold);
        }
        
        return queryBuilder.toString();
    }
    
    private void buildMetric(StringBuilder queryBuilder) {
        queryBuilder.append(metric);
        if (factors.size() > 0) {
            queryBuilder.append("{")
                    .append(factors.stream().filter(Objects::nonNull).map(PromFactor::build).collect(Collectors.joining(",")))
                    .append("}");
        }
    }
    
    public void addFactor(PromFactor... promFactors) {
        addFactor(Arrays.asList(promFactors));
    }
    
    public void addFactor(List<PromFactor> factors) {
        this.factors.addAll(factors);
    }
    
    public static PromQuery withoutMethod(String metric, PromFactor... promFactors) {
        return withoutMethod(metric, null, null, promFactors);
    }
    
    public static PromQuery withoutMethod(String metric, String operator, Double threshold, PromFactor ... promFactors){
        return withMethod(metric, null, null, null, operator, threshold, promFactors);
    }
    
    public static PromQuery withMethod(String metric, String method, Integer step, String duration, PromFactor... promFactors) {
        return withMethod(metric, method, step, duration, null, null, promFactors);
    }
    
    public static PromQuery withMethod(String metric, String method, Integer step, String duration, String operator, Double threshold, PromFactor... promFactors) {
        PromQuery promQuery = new PromQuery();
        promQuery.metric = metric;
        promQuery.method = method;
        promQuery.step = step;
        promQuery.duration = duration;
        promQuery.operator = operator;
        promQuery.threshold = threshold;
        promQuery.factors.addAll(Arrays.asList(promFactors));
        return promQuery;
    }
}
