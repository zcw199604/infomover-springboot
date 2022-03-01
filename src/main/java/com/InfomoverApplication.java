package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableScheduling
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = "com.info.infomover.repository")
public class InfomoverApplication {
    public static void main(String[] args) {
        SpringApplication.run(InfomoverApplication.class, args);
    }
}
