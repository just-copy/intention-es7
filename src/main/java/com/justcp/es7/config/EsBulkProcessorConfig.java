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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EsBulkProcessorConfig {

    public static BulkProcessor bulkProcessor(RestHighLevelClient client){

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        return BulkProcessor.builder(bulkConsumer, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("executionId: " + executionId + ", num to execute: " + request.numberOfActions());

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                Iterator<BulkItemResponse> iterator = response.iterator();
                long cntSuccess = 0,cntFail = 0,cntConflict = 0;
                while (iterator.hasNext()) {
                    BulkItemResponse BulkResponse = iterator.next();
                    switch (BulkResponse.status()) {
                        case OK:
                            cntSuccess ++;
                            break;
                        case CREATED:
                            cntSuccess ++;
                            break;
                        case ACCEPTED:
                            cntSuccess++;
                            break;
                        case CONFLICT:
                            cntConflict++;
                            break;
                        default:
                            cntFail++;
                    }
                }
                log.info("executionId: " + executionId + ", num success: " + cntSuccess +
                        ", num fail: " + cntFail +", num conflict: " + cntConflict);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.error("executionId: " + executionId + ", FAILURE MESSAGE: " + failure.getMessage());
            }
        }).setBulkActions(2000) //  达到刷新的条数
                .setBulkSize(new ByteSizeValue(3, ByteSizeUnit.MB)) // 达到 刷新的大小
                .setFlushInterval(TimeValue.timeValueSeconds(5)) // 固定刷新的时间频率
                .setConcurrentRequests(1) //并发线程数
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // 重试补偿策略
                .build();

    }

}
