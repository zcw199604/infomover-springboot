package com.info.infomover.config;

import com.info.infomover.prom.PromClient;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.URISyntaxException;

@Configuration
public class PromClientConfig {

    @Value("${prometheus.url}")
    private String url;

    private String scope;

    @Bean
    public PromClient createPromClient() {
        try {
            return RestClientBuilder.newBuilder()
                    .baseUri(new URI(url))
                    .build(PromClient.class);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

}
