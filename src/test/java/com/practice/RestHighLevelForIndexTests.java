package com.practice;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class RestHighLevelForIndexTests extends ElasticsearchDemoApplicationTests {


    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelForIndexTests(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /*
    创建索引，创建映射
     */
    @Test
    public void testIndexAndMapping() throws IOException {
        // 参数1：创建索引的请求对象 参数2：请求配置对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("products");
        // 指定映射 参数1：指定映射的json结构(一般直接从kibana中复制过来) 参数2：指定数据类型
        createIndexRequest.mapping("  {\n" +
                "    \"properties\": {\n" +
                "      \"id\":{\n" +
                "        \"type\":\"integer\"\n" +
                "      },\n" +
                "      \"title\":{\n" +
                "        \"type\":\"keyword\"\n" +
                "      },\n" +
                "      \"price\":{\n" +
                "        \"type\":\"double\"\n" +
                "      },\n" +
                "      \"create_at\":{\n" +
                "        \"type\":\"date\"\n" +
                "      },\n" +
                "      \"description\":{\n" +
                "        \"type\":\"text\",\n" +
                "        \"analyzer\": \"ik_max_word\"\n" +
                "      }\n" +
                "    }\n" +
                "  }", XContentType.JSON);
        CreateIndexResponse createIndexResponse = null;
        try {
            createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("创建状态" + createIndexResponse.toString());
        // 关闭资源
        restHighLevelClient.close();
    }

    /*
删除索引
 */
    @Test
    public void testDeleteIndex() throws IOException {
        // 参数1：删除索引对象 参数2：请求配置对象
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(new DeleteIndexRequest("products"), RequestOptions.DEFAULT);
        System.out.println("删除状态" + acknowledgedResponse);
    }

}


