package com.shade.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.AvgAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import lombok.val;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author: shade
 * @date: 2022/6/25 21:23
 * @description:
 */
public class EsReader {
    public static void main(String[] args) throws IOException {
        //新建工厂类
        JestClientFactory jestClientFactory = new JestClientFactory();

        //新建conf
        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //设置工厂config
        jestClientFactory.setHttpClientConfig(config);

        //创建客户端
        JestClient jestClient = jestClientFactory.getObject();

        //TODO 查询
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"男\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\"match\": {\n" +
                "          \"favo\": \"球\"\n" +
                "        }}\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupbyClass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\",\n" +
                "        \"size\": 10\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"maxAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 2\n" +
                "}").addIndex("student")
                .addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        Long total = searchResult.getTotal();
        System.out.println(total);

        //获取hits
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        //遍历hits
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("index: "+hit.index);
            System.out.println("type: "+hit.type);
            System.out.println("id: "+hit.id);
            System.out.println("score: "+hit.score);
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o+": "+source.get(o));
            }
            System.out.println("-----------------------------------------------------");
        }

        System.out.println("=====================================================");
        //获取aggs
        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation groupbyClass = aggregations.getTermsAggregation("groupbyClass");

        List<TermsAggregation.Entry> buckets = groupbyClass.getBuckets();

        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key: "+bucket.getKey());
            System.out.println("doc_count: "+bucket.getCount());
            val maxAge = bucket.getMaxAggregation("maxAge");
            System.out.println("value: "+maxAge.getMax());
        }


        //关闭客户端
        jestClient.shutdownClient();

    }
}
