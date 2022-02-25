package com.info.infomover.config;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@Configuration
public class JapConfig {

    @PersistenceContext
    private EntityManager entityManager;


    @Bean
    public JPAQueryFactory initJpaQueryFactory() {
        return new JPAQueryFactory(entityManager);
    }

}