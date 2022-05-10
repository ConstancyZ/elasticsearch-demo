package com.practice;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.awt.print.PrinterJob;
import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RestHighLevelClientForObjectTests extends ElasticsearchDemoApplicationTests {

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelClientForObjectTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 将对象放入es中
     */
    @Test
    public void testIndex() throws IOException {
        Phone phone = new Phone(1, "iphone", 5000.0, "苹果手机真滴好用!!!");
        // 录入 es 中
        IndexRequest indexRequest = new IndexRequest("phone");
        indexRequest.id(phone.getId().toString()).source(new ObjectMapper().writeValueAsString(phone).toString(), XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
    }

    /**
     * 查询到封装成对象
     */
    @Test
    public void testSearch() throws IOException {
        // 搜索索引
        SearchRequest searchRequest = new SearchRequest("phone");
        // 指定条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 创建高亮器
        HighlightBuilder highlighterBuilder = new HighlightBuilder();
        highlighterBuilder.requireFieldMatch(false).field("description").preTags("<span style = 'color:red'>").postTags("</span>");
        // 高亮的时候不能查询所有
        searchSourceBuilder.query(QueryBuilders.termQuery("description", "苹果"))
                .from(0)
                .size(30)
                .highlighter(highlighterBuilder);
        // 指定查询条件
        searchRequest.source(searchSourceBuilder);
        // 参数1:搜索的请求对象 参数2：请求配置对象  返回值：查询的响应对象
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 因为返回的结构和kibana一样，hits里便是对象
        System.out.println("总条数" + searchResponse.getHits().getTotalHits().value);
        // 最大得分
        System.out.println("得分" + searchResponse.getHits().getMaxScore());
        // 获取结果
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Phone> phoneList = new ArrayList<Phone>();
        for (SearchHit hit : hits) {
            Phone phone = new ObjectMapper().readValue(hit.getSourceAsString(), Phone.class);
            // 高亮结果需要重新赋值给对象
            Map<String, HighlightField> highlightFieldHashMap = hit.getHighlightFields();
            if (highlightFieldHashMap.containsKey("description")) {
                phone.setDescription(highlightFieldHashMap.get("description").fragments()[0].toString());
            }
            phoneList.add(phone);
        }
        for (Phone p:phoneList
             ) {
            System.out.println(p);
        }
    }
}
