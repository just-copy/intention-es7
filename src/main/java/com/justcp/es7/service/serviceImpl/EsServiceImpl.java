package com.justcp.es7.service.serviceImpl;

import com.alibaba.fastjson.JSONObject;
import com.justcp.es7.config.EsBulkProcessorConfig;
import com.justcp.es7.service.EsService;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("esService")
public class EsServiceImpl implements EsService {

    @Resource
    private RestHighLevelClient client;

    @Override
    public void insertBatch() {
        try {
            BulkProcessor bulkProcessor = EsBulkProcessorConfig.bulkProcessor(client);
            int count = 0;
            //把导出的结果以JSON的格式写到文件里
            for (int i = 0; i < 20; i++) {
                BufferedReader br = new BufferedReader(new FileReader("/Users/lilong/logs/log.json"));
                String json = null;
                while ((json = br.readLine()) != null) {
                    JSONObject jsonObject =JSONObject.parseObject(json);
                    jsonObject.put("instanceId", count / 1000 + 1);
                    jsonObject.put("type", "query");
                    bulkProcessor.add(new IndexRequest("index_dev").source(jsonObject.toJSONString(), XContentType.JSON));
                    //每一千条提交一次
                    count++;
                }
                br.close();
            }

            bulkProcessor.flush();
            try {
                bulkProcessor.awaitClose(20, TimeUnit.SECONDS);
    //            bulkProcessor.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void deleteByQuery() {
        DeleteByQueryRequest request =
                new DeleteByQueryRequest("index_dev");
        request.setConflicts("proceed");
        // 并行
        request.setSlices(2);
        // 使用滚动参数来控制“搜索上下文”存活的时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        // 超时
        request.setTimeout(TimeValue.timeValueMinutes(2));
        // 刷新索引
        request.setRefresh(true);

        QueryBuilder qb = QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.existsQuery("instanceIds"));
        request.setQuery(qb);
        try {
            BulkByScrollResponse bulkResponse =
                    client.deleteByQuery(request, RequestOptions.DEFAULT);
            TimeValue timeTaken = bulkResponse.getTook();
            boolean timedOut = bulkResponse.isTimedOut();
            long totalDocs = bulkResponse.getTotal();
            long deletedDocs = bulkResponse.getDeleted();
            long batches = bulkResponse.getBatches();
            long noops = bulkResponse.getNoops();
            log.info("timeTaken:" + timeTaken);
            log.info("timedOut:" + timedOut);
            log.info("totalDocs:" + totalDocs);
            log.info("deletedDocs:" + deletedDocs);
            log.info("batches:" + batches);
            log.info("noops:" + noops);
            log.info("timeTaken:" + timeTaken);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
