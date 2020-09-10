package com.justcp.es7.service.serviceImpl;

import com.justcp.es7.service.EsQueryService;

import java.io.IOException;

import javax.annotation.Resource;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

/**
 * @description EsQueryServiceImpl
 */
@Slf4j
@Service("esQueryService")
public class EsQueryServiceImpl implements EsQueryService {

    @Resource
    private RestHighLevelClient client;

    @Value("${research.es7.es.index.devInex}")
    private String devIndex;

    @Override
    public void searchQuery() {

        SearchRequest searchRequest = new SearchRequest(devIndex);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.trackTotalHits(true);
        searchSourceBuilder.size(10000);
        searchRequest.source(searchSourceBuilder);

//        searchRequest.routing("routing");
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            RestStatus status = searchResponse.status();
            long took = searchResponse.getTook().getMillis();
            SearchHits hits = searchResponse.getHits();
            TotalHits totalHits = hits.getTotalHits();
            int length = hits.getHits().length;
            long value = totalHits.value;
            log.info("took:" + took + " value :" + value + " length: " + length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
