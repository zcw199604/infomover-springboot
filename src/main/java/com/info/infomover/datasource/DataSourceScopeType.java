package com.info.infomover.datasource;

public enum DataSourceScopeType {
    All // 既可以做Source 也可以做 Sink
    ,Source // 作为源端
    , Sink, // 作为目标端
}
