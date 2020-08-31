package com.justcp.es7.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
@Slf4j
public class EsClientConfig {

    @Bean
    public RestHighLevelClient getRestHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                //如果是集群再配置多个
                RestClient.builder(new HttpHost("192.168.43.152",9200,"http"))
        );

        return client;
    }


}
