package com.shade.read;

import com.shade.bean.Student;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import lombok.val;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author: shade
 * @date: 2022/6/25 22:05
 * @description:
 */
public class JavaAPIReader {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        jestClientFactory.setHttpClientConfig(config);

        JestClient jestClient = jestClientFactory.getObject();

//--------------------------------------new查询构建者对象-----------------------------------------------
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//-----------------------------------------添加query-----------------------------------------------------------
        //bool
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //term
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "男");
        //match
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "球");
        //合并query
        BoolQueryBuilder queryBuilder = boolQueryBuilder.filter(termQueryBuilder).must(matchQueryBuilder);

        sourceBuilder.query(queryBuilder);
//-------------------------------------------添加aggs----------------------------------------------------------------
        //通过工具类创建termsaggs对象
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupbyClass").field("class_id").size(10);
        //通过工具类创建maxsaggs对象
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        //使用aggs的sub方法合并
        TermsAggregationBuilder aggs = termsAggregationBuilder.subAggregation(maxAggregationBuilder);

        sourceBuilder.aggregation(aggs);
//---------------------------------------------添加页面------------------------------------------------------------
        sourceBuilder.from(0);
        sourceBuilder.size(2);

        Search search = new Search.Builder(sourceBuilder.toString()).addIndex("student").build();

        SearchResult searchResult = jestClient.execute(search);

        Long total = searchResult.getTotal();
        System.out.println(total);

        //获取hits
        List<SearchResult.Hit<Student, Void>> hits = searchResult.getHits(Student.class);
        //遍历hits
        for (SearchResult.Hit<Student, Void> hit : hits) {
            System.out.println("index: "+hit.index);
            System.out.println("type: "+hit.type);
            System.out.println("id: "+hit.id);
            System.out.println("score: "+hit.score);
            Student student = hit.source;
            System.out.println(student.print());

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


        jestClient.shutdownClient();
    }
}
