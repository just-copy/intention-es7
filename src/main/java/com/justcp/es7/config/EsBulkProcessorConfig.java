package com.justcp.es7.config;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.function.BiConsumer;

@Component
public class EsBulkProcessorConfig {

    @Resource
    private RestHighLevelClient client;

    @Bean(name = "bulkProcessor")
    public BulkProcessor bulkProcessor(){

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        return BulkProcessor.builder(bulkConsumer, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int i = request.numberOfActions();
//                log.error("ES 同步数量{}",i);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                // todo do something
                Iterator<BulkItemResponse> iterator = response.iterator();
                while (iterator.hasNext()){
//                    System.out.println(JSON.toJSONString(iterator.next()));
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
//                log.error("写入ES 重新消费");
            }
        }).setBulkActions(1000) //  达到刷新的条数
                .setBulkSize(new ByteSizeValue(2, ByteSizeUnit.MB)) // 达到 刷新的大小
                .setFlushInterval(TimeValue.timeValueSeconds(5)) // 固定刷新的时间频率
                .setConcurrentRequests(2) //并发线程数
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // 重试补偿策略
                .build();

    }

}
