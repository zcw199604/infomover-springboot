package com.info.infomover.prom.constants;

public enum  MetricMethod {
    
    Counter_Increase("increase"), // 给定时间范围内的增量
    Counter_Rate("rate"), // 给定时间范围内增长率
    
    Gauge_Delta("delta"), // 给定时间范围内样本差值
    
    Common_Absent("absent"), // 瞬时值存在返回空，否则存在返回1
    Common_Absent_Over_Time("absent_over_time"), // 给定时间范围内容存在返回空，否则返回1
    
    None("");
    
    public String methodName;
    
    MetricMethod(String methodName) {
        this.methodName = methodName;
    }
}
