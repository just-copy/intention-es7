package com.justcp.es7.service.serviceImpl;

import com.justcp.es7.service.EsService;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Service("esService")
public class EsServiceImpl implements EsService {

    @Resource
    private BulkProcessor bulkProcessor;

    @Override
    public void insertBatch() {
        bulkProcessor.add(new IndexRequest("posts").id("1")
                .source(XContentType.JSON,"field", "foo"));
        bulkProcessor.add(new IndexRequest("posts").id("2")
                .source(XContentType.JSON,"field", "bar"));
        bulkProcessor.add(new IndexRequest("posts").id("3")
                .source(XContentType.JSON,"field", "baz"));
//        bulkProcessor.close();
        bulkProcessor.flush();
        try {
            System.out.println("try");
//            bulkProcessor.awaitClose(5, TimeUnit.MINUTES);
            System.out.println("over");
        } catch (Exception e) {
            System.out.println("关闭异常");
        }
    }
}
