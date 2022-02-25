package com.info.infomover.prom.constants;

public enum StepDuration {
    MS("ms", 1L),
    S("s", 1000L),
    M("m", 60000L),
    H("h", 60*60*1000L),
    D("d", 24*60*60*1000L),
    W("w", 7*24*60*60*1000L),
    Y("y", 365*24*60*60*1000L);
    
    public String duration;
    public Long ms;
    
    StepDuration(String duration, Long ms) {
        this.duration = duration;
        this.ms = ms;
    }
}
