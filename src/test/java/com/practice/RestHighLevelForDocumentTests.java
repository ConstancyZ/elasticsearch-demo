package com.practice;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.swing.text.Highlighter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// 操作文档的一系列方法
public class RestHighLevelForDocumentTests extends ElasticsearchDemoApplicationTests {
    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelForDocumentTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 创建索引一条文档
     */
    @Test
    public void testCreate() throws IOException {
        IndexRequest indexRequest = new IndexRequest("products");
        indexRequest.id("3")// 手动指定id
                .source(" {\n" +
                        "  \"title\":\"床3\",\n" +
                        "  \"price\":503.0,\n" +
                        "  \"create_at\":\"2022-05-06\",\n" +
                        "  \"description\":\"慕思床3\"\n" +
                        "}", XContentType.JSON);
        // 参数1：索引的请求对象 参数2：请求配置对象
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
    }

    /**
     * 更新文档
     */
    @Test
    public void testUpdate() throws IOException {
        // 参数1：对应索引 参数2：更新的文档id
        UpdateRequest updateRequest = new UpdateRequest("products", "2");
        updateRequest.doc(" {\n" +
                "  \"title\":\"大床\",\n" +
                "  \"price\":5000.0,\n" +
                "  \"create_at\":\"2022-05-06\",\n" +
                "  \"description\":\"高级慕思床\"\n" +
                "}", XContentType.JSON);
        // 参数1:更新请求对象 参数2：请求配置对象
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    /**
     * 删除文档
     */
    @Test
    public void testDetele() throws IOException {

        // 参数1:删除请求对象 参数2：请求配置对象
        DeleteResponse deleteResponse = restHighLevelClient.delete(new DeleteRequest("products", "2"), RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    /**
     * 基于id去查询文档
     */
    @Test
    public void testQueryByID() throws IOException {

        // 参数1:查询请求对象 参数2：请求配置对象  返回值：查询的响应对象
        GetResponse getResponse = restHighLevelClient.get(new GetRequest("products", "2"), RequestOptions.DEFAULT);
        System.out.println("查询结果" + getResponse.getSourceAsMap());
    }

    /**
     * 查询所有
     */
    @Test
    public void testQueryAll() throws IOException {
        // 搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());// 查询所有
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
        for (SearchHit hit : hits) {
            System.out.println("结果集:" + hit.getSourceAsMap());
        }
    }

    /**
     * 不同条件查询
     */

    @Test
    public void testQuery() throws IOException {
        // 1.term查询
//        query(QueryBuilders.termQuery("description", "高级"));// 关键词查询
//        // 2.范围查询 range
//        query(QueryBuilders.rangeQuery("price").gt(0).lte(1000));
//        // 3.前缀查询 prefix
//        query(QueryBuilders.prefixQuery("title", "大"));
//        // 4.wildcard 通配符查询 ?匹配一个字符 *匹配多个字符
//        query(QueryBuilders.wildcardQuery("title", "大?"));
//        // 5.ids 多个指定id查询
        //query(QueryBuilders.idsQuery().addIds("1").addIds("2"));
        // 6.multi_match  多字段查询
        query(QueryBuilders.multiMatchQuery("慕思", "title", "description"));

    }

    /**
     * 分页查询 from 其实位置 size 每页记录
     * 排序 sort
     * 返回指定的字段 _source 用来指定查询文档返回哪些字段
     * 高亮结果 highlight
     */
    @Test
    public void testSearch() throws IOException {
        // 搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 创建高亮器
        HighlightBuilder highlighterBuilder = new HighlightBuilder();
        highlighterBuilder.requireFieldMatch(false).field("description").preTags("<span style = 'color:red'>").postTags("</span>");
        // 高亮的时候不能查询所有
        searchSourceBuilder.query(QueryBuilders.termQuery("description", "床"))
                // searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0) // 起始位置
                .size(2)// 每页条数，默认1
                .sort("price", SortOrder.ASC)// 排序
                .fetchSource(new String[]{}, new String[]{"create_at"}) // 参数1：包含字段数组 参数2：排除字段参数 2选1使用;
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
        for (SearchHit hit : hits) {
            System.out.println("结果集:" + hit.getSourceAsMap());
            // 获取高亮结果
            Map<String, HighlightField> highlightFieldHashMap = hit.getHighlightFields();
            if (highlightFieldHashMap.containsKey("description")) {
                System.out.println("description高亮结果:" + highlightFieldHashMap.get("description").fragments()[0]);
            }
        }

    }

    /**
     * query        :查找精确查询 查询计算文档得分 并根据文档得分进行返回
     * filter query :过滤查询 用来在大量数据中筛选出本地查询相关数据  不会计算文档得分 经常使用fileter query 结果进行缓存
     * 注意：一旦使用 query 和 filter query  es先执行 filter query 然后再执行 query
     */
    @Test
    public void testFilterQuery() throws IOException {
        // 搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 先找到id = 1，2，3,再在这个结果集中查询description中有慕思的，对于大数据量过滤了查询效率会很高
        searchSourceBuilder
                .query(QueryBuilders.termQuery("description","慕g"))
                .postFilter(QueryBuilders.idsQuery().addIds("1").addIds("2").addIds("3")); //用来指定过滤对象
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
        for (SearchHit hit : hits) {
            System.out.println("结果集:" + hit.getSourceAsMap());
        }
    }


    /**
     * @param queryBuilder
     * @throws IOException
     */

    // 公共方法query
    public void query(QueryBuilder queryBuilder) throws IOException {
        // 搜索索引
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定条件对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);// 指定查询条件
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
        for (SearchHit hit : hits) {
            System.out.println("结果集:" + hit.getSourceAsMap());
        }
    }
}

