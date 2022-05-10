package com.practice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.entity.Phone;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RestHighLevelClientForAggsTests extends ElasticsearchDemoApplicationTests {

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelClientForAggsTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 基于terms的聚合查询 基于字段进行分组聚合
     */
    @Test
    public void textAggs() throws IOException {
        // 搜索索引
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 指定条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 高亮的时候不能查询所有
        searchSourceBuilder
                .query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.terms("price_group").field("price"));// 用来设置聚合处理
        // 指定查询条件
        searchRequest.source(searchSourceBuilder);
        // 参数1:搜索的请求对象 参数2：请求配置对象  返回值：查询的响应对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        ParsedDoubleTerms parsedDoubleTerms = aggregations.get("price_group");
        List<? extends Terms.Bucket> buckets = parsedDoubleTerms.getBuckets();
        for (Terms.Bucket b:buckets
             ) {
            System.out.println(b.getKey()+" "+b.getDocCount());
        }
    }

    /**
     * max(ParsedMax)，min(ParsedMin)，sum(ParsedSum)，avg(ParsedAvg) 聚合函数 ,返回桶中只有一个value
     */
    @Test
    public void testOtherFunc() throws IOException {
        // 搜索索引
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 指定条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 高亮的时候不能查询所有
        searchSourceBuilder
                .query(QueryBuilders.matchAllQuery())
                //.aggregation(AggregationBuilders.sum("sum_price").field("price"));// 用来设置聚合处理 sum
                .aggregation(AggregationBuilders.avg("avg_price").field("price"));// 用来设置聚合处理 avg
        // 指定查询条件
        searchRequest.source(searchSourceBuilder);
        // 参数1:搜索的请求对象 参数2：请求配置对象  返回值：查询的响应对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
//        ParsedSum parsedSum = aggregations.get("sum_price");
//        System.out.println(parsedSum.getValue());
        ParsedAvg parsedAvg = aggregations.get("avg_price");
        System.out.println(parsedAvg.getValue());

    }
}
