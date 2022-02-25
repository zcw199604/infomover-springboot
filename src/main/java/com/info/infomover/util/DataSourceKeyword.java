package com.info.infomover.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataSourceKeyword {

    public static final String DRIVER = "driver";
    public static final String URL = "url";
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String DATABASE = "database";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DATABASE_PASSWORD = "database.password";

    public static void main(String[] args) {
        Pattern r = Pattern.compile("AES\\(.*\\)");
        Matcher matcher = r.matcher("org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='AES(ASDASDASD)';");
        System.out.println(matcher.find());
        System.out.println(matcher.group());

    }
}
