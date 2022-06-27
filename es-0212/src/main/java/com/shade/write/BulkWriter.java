package com.shade.write;

import com.shade.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
import java.util.ArrayList;


/**
 * @author: shade
 * @date: 2022/6/25 21:09
 * @description:
 */
public class BulkWriter {
    public static void main(String[] args) throws IOException {
        //创建工厂类
        JestClientFactory jestClientFactory = new JestClientFactory();

        //创建工厂config
        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //设置工厂conf
        jestClientFactory.setHttpClientConfig(config);

        //创建jest客户端
        JestClient jestClient = jestClientFactory.getObject();

        Movie s1 = new Movie("106", "雪国列车");
        Movie s2 = new Movie("107", "三国");
        Movie s3 = new Movie("108", "西游记");


        Index index1 = new Index.Builder(s1).index("movie_chn_2020").type("movie").id("1006").build();
        Index index2 = new Index.Builder(s2).index("movie_chn_2020").type("movie").id("1007").build();
        Index index3 = new Index.Builder(s3).index("movie_chn_2020").type("movie").id("1008").build();

        ArrayList<Index> list = new ArrayList<>();
        list.add(index1);
        list.add(index2);
        list.add(index3);

        Bulk bulk = new Bulk.Builder().addAction(list).build();

        //执行行动
        jestClient.execute(bulk);

        //关闭jest客户端
        jestClient.shutdownClient();

    }
}
