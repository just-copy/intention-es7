package com.justcp.es7.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
@Slf4j
public class EsClientConfig {

    @Value("${research.es7.es.servers}")
    private String esHostname;

    @Value("${research.es7.es.port}")
    private int port;

    @Bean
    public RestHighLevelClient getRestHighLevelClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                //如果是集群再配置多个
                RestClient.builder(new HttpHost(esHostname, port, "http"))
                        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                            // 该方法接收一个RequestConfig.Builder对象，对该对象进行修改后然后返回。
                            @Override
                            public RequestConfig.Builder customizeRequestConfig(
                                    RequestConfig.Builder requestConfigBuilder) {
                                return requestConfigBuilder
//                                        .setConnectTimeout(5000 * 1000) // 连接超时（默认为1秒）
                                        .setSocketTimeout(6000 * 1000);// 套接字超时（默认为30秒）//更改客户端的超时限制默认30秒现在改为100*1000分钟
                            }
                        })// 调整最大重试超时时间（默认为30秒）.setMaxRetryTimeoutMillis(60000);

        );

        return client;
    }


}
