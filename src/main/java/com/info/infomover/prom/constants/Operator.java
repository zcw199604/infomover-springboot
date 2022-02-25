package com.info.infomover.prom.constants;

public enum  Operator {
    EQ("=="),
    LT("<"),
    LE("<="),
    NE("!="),
    GT(">"),
    GE(">=");

    public String symbol ;
    
    Operator(String symbol) {
        this.symbol = symbol;
    }
}
