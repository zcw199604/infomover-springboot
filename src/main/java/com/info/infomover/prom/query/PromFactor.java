package com.info.infomover.prom.query;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class PromFactor implements Serializable {

    public String name;
    public MatchOption option;
    public List<String> value;
    
    public String build(){
        if (StringUtils.isBlank(name) || value==null || value.size()==0) {
            return "";
        }
    
        StringBuilder factorBuilder = new StringBuilder();
        factorBuilder.append(name);
        if (value.size()>1){
            if (MatchOption.EQUAL.equals(option)) {
                option = MatchOption.REGEX_EQUAL;
            } else if (MatchOption.NOT_EQUAL.equals(option)) {
                option = MatchOption.NOT_REGEX_EQUAL;
            }
    
            factorBuilder.append(option.option)
                    .append("'(")
                    .append(String.join("|", value))
                    .append(")'");
        } else {
            if (MatchOption.REGEX_EQUAL.equals(option)) {
                option = MatchOption.EQUAL;
            } else if (MatchOption.NOT_REGEX_EQUAL.equals(option)) {
                option = MatchOption.NOT_EQUAL;
            }
            
            factorBuilder.append(option.option)
                    .append("'")
                    .append(value.get(0))
                    .append("'");
        }
    
        return factorBuilder.toString();
        
    }
    
    public  static PromFactor factor(String name, List<String> value) {
        return factor(name, MatchOption.REGEX_EQUAL, value);
    }
    
    public static PromFactor factor(String name, String value) {
        return factor(name, MatchOption.EQUAL, Collections.singletonList(value));
    }
    
    public static PromFactor factor(String name, MatchOption option, List<String> value){
        PromFactor factor = new PromFactor();
        factor.name = name;
        factor.option = option;
        factor.value = value;
        return factor;
    }
    
    public enum MatchOption {
        EQUAL("="), NOT_EQUAL("!="), REGEX_EQUAL("=~"), NOT_REGEX_EQUAL("!~");
        
        private String option;
        
        MatchOption(String option) {
            this.option = option;
        }
        
        public String getOption() {
            return option;
        }
    }
    
}
